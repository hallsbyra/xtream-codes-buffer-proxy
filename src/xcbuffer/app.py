"""HTTP server: Xtream Codes API relay, buffered streams and the status page."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import hashlib
import json
import logging
import re
import shutil
import socket
import time
from collections.abc import Awaitable, Callable
from importlib import resources
from urllib.parse import parse_qs, urljoin, urlsplit

import aiohttp
from aiohttp import web

from . import xtream
from .config import Settings
from .connection import ProviderConnection
from .live import LiveManager
from .tunables import Tunables
from .upstream import API_TIMEOUT, CHUNK, Upstream, UpstreamError, forward_headers
from .vod import NotCacheable, VodCache
from .vpn import VpnMonitor

log = logging.getLogger(__name__)

SETTINGS = web.AppKey("settings", Settings)
UPSTREAM = web.AppKey("upstream", Upstream)
CONNECTION = web.AppKey("connection", ProviderConnection)
VOD = web.AppKey("vod", VodCache)
LIVE = web.AppKey("live", LiveManager)
CATALOG = web.AppKey("catalog", xtream.Catalog)
RELAY_HOSTS = web.AppKey("relay_hosts", set)
# peer -> (live stream id, time of last playlist or segment request)
HLS = web.AppKey("hls", dict)
HLS_ACTIVE_SECONDS = 30
VPN = web.AppKey("vpn", VpnMonitor)
TUNABLES = web.AppKey("tunables", Tunables)

IMAGE_SUFFIXES = (".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg", ".ico")
RELAY_RESPONSE_HEADERS = ("Content-Type", "Content-Range", "Accept-Ranges", "Content-Disposition")
DISCONNECTS = (ConnectionResetError, aiohttp.ClientConnectionResetError, BrokenPipeError)


class Passthrough:
    """Connection holder for streams relayed without buffering (HLS, catch-up).

    Relays share one holder, so they open one provider connection at a time:
    a new one waits briefly for the previous to finish (an HLS segment) and
    otherwise closes it (a stream). HLS fetches many short requests, so the
    connection is kept for a while after the last one ends instead of being
    handed to a background download."""

    label = "relay"
    HANDOVER_SECONDS = 2

    def __init__(self, connection: ProviderConnection, linger: float) -> None:
        self.connection = connection
        self.linger = linger
        # The one open provider response; a relay that lost it ends quietly.
        self.active: aiohttp.ClientResponse | None = None
        self._release: asyncio.TimerHandle | None = None
        self._preempted = False
        self._opening = asyncio.Lock()
        self._idle = asyncio.Event()
        self._idle.set()
        # A connection still waiting for the provider's response headers
        self._pending: asyncio.Future | None = None

    def is_idle(self) -> bool:
        return False

    def preempt(self) -> None:
        self._preempted = True
        if self._pending:
            self._pending.cancel()
        if self.active:
            self.active.close()
        self._schedule_release(0)

    async def open(
        self, content: str, connect: Callable[[], Awaitable[aiohttp.ClientResponse]]
    ) -> aiohttp.ClientResponse:
        async with self._opening:
            # A release scheduled while we waited must still happen if the
            # content was switched away from meanwhile.
            await self.connection.acquire(self, content, foreground=True)
            if self._release:
                self._release.cancel()
                self._release = None
            self._preempted = False
            if self.active:
                try:
                    await asyncio.wait_for(self._idle.wait(), self.HANDOVER_SECONDS)
                except TimeoutError:
                    self.active.close()
                    self.active = None
            if self._preempted or self.connection.holder is not self:
                raise UpstreamError(503, "another request took the provider connection")
            self._pending = asyncio.ensure_future(connect())
            try:
                resp = await self._pending
            except asyncio.CancelledError:
                if asyncio.current_task().cancelling():
                    raise
                raise UpstreamError(503, "another request took the provider connection") from None
            finally:
                self._pending = None
            if self._preempted:
                resp.close()
                raise UpstreamError(503, "another request took the provider connection")
            self.active = resp
            self._idle.clear()
            return resp

    def end(self, resp: aiohttp.ClientResponse | None) -> None:
        if self.active is resp:
            self.active = None
        if self.active is None:
            self._idle.set()
            self._schedule_release(0 if self._preempted else self.linger)

    def _schedule_release(self, delay: float) -> None:
        if self._release:
            self._release.cancel()
        loop = asyncio.get_running_loop()
        self._release = loop.call_later(delay, lambda: asyncio.ensure_future(self._release_now()))

    async def _release_now(self) -> None:
        # A new relay may have started between scheduling and running.
        if self.active and not self._preempted:
            return
        if self._pending:
            # Let the cancelled connection close before someone else opens one.
            await asyncio.wait([self._pending])
        self.connection.release(self)


RELAY = web.AppKey("relay", Passthrough)


# --- helpers ---------------------------------------------------------------------------


def _keepalive(request: web.Request) -> None:
    """Detect a TV that disappeared without closing the connection, while
    still letting a paused one sit on an open connection for hours."""
    sock = request.transport.get_extra_info("socket") if request.transport else None
    if sock is None:
        return
    with contextlib.suppress(OSError, AttributeError):
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPIDLE, 60)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPINTVL, 15)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_KEEPCNT, 8)


def _proxy_origin(request: web.Request) -> tuple[str, str, int]:
    host = request.url.host or "localhost"
    port = request.url.port or 80
    return f"http://{host}:{port}", host, port


def _split_name(name: str) -> tuple[str, str]:
    stem, dot, ext = name.rpartition(".")
    return (stem, ext.lower()) if dot else (name, "")


def parse_range(header: str | None, size: int) -> tuple[int, int] | None:
    """Inclusive byte range from a Range header; None for the whole file.
    Raises ValueError when the range cannot be satisfied."""
    if not header:
        return None
    match = re.fullmatch(r"\s*bytes=(\d*)-(\d*)\s*", header.split(",")[0])
    if not match or not (match.group(1) or match.group(2)):
        return None
    first, last = match.groups()
    if first:
        start = int(first)
        end = min(int(last), size - 1) if last else size - 1
    else:
        start = max(size - int(last), 0)
        end = size - 1
    if start >= size or start > end:
        raise ValueError(header)
    return start, end


def peer_of(request: web.Request) -> str:
    return request.remote or "?"


def _busy(request: web.Request, content: str, *, may_switch: bool = True) -> web.Response | None:
    """Refuse a device that just lost the connection to another one."""
    connection = request.app[CONNECTION]
    if connection.admit(peer_of(request), content, may_switch=may_switch):
        return None
    return web.Response(status=503, text=f"Busy: {connection.owner} is watching {connection.content}.")


def _unavailable(request: web.Request) -> web.Response | None:
    """Answer for when the provider must not be contacted at all."""
    if not request.app[SETTINGS].configured:
        return web.Response(status=503, text="UPSTREAM_URL is not set; see the status page.")
    if not request.app[UPSTREAM].allowed:
        return web.Response(status=503, text="VPN is down; provider traffic is blocked.")
    return None


# --- Xtream API ---------------------------------------------------------------------------


async def api(request: web.Request) -> web.Response:
    if blocked := _unavailable(request):
        return blocked
    settings = request.app[SETTINGS]
    upstream = request.app[UPSTREAM]
    try:
        async with await upstream.get(
            upstream.url(request.path_qs), forward_headers(request.headers), timeout=API_TIMEOUT
        ) as resp:
            body = await resp.read()
            status = resp.status
            content_type = resp.headers.get("Content-Type", "application/octet-stream")
    except UpstreamError as exc:
        return web.Response(status=exc.status, text=str(exc))
    except (aiohttp.ClientError, TimeoutError) as exc:
        return web.Response(status=502, text=f"provider unreachable: {exc}")

    proxy_origin, host, port = _proxy_origin(request)
    up_origin = xtream.origin(settings.upstream_url)
    if request.path == "/player_api.php" and status == 200:
        data = xtream.parse_json(body)
        action = request.query.get("action")
        if data is not None:
            if action:
                request.app[CATALOG].harvest(action, data)
            else:
                body = json.dumps(xtream.rewrite_auth(data, host, port)).encode()
    for old, new in (
        (up_origin, proxy_origin),
        (up_origin.replace("/", "\\/"), proxy_origin.replace("/", "\\/")),
    ):
        body = body.replace(old.encode(), new.encode())
    headers = {"Content-Type": content_type}
    return web.Response(status=status, body=body, headers=headers)


# --- VOD ----------------------------------------------------------------------------------


async def vod(request: web.Request) -> web.StreamResponse:
    if blocked := _unavailable(request):
        return blocked
    kind = request.match_info["kind"]
    stream_id, ext = _split_name(request.match_info["name"])
    if not ext or not stream_id:
        return await relay(request)
    if busy := _busy(request, f"{kind} {stream_id}"):
        return busy
    cache = request.app[VOD]
    vf = cache.get(kind, stream_id, ext)
    _learn_duration(request, vf)
    vf.request(request.path_qs, forward_headers(request.headers))
    try:
        await vf.wait_ready()
    except NotCacheable:
        return await relay(request, content=f"{kind} {stream_id}")
    except UpstreamError as exc:
        return web.Response(status=exc.status, text=str(exc))
    assert vf.size is not None
    try:
        byte_range = parse_range(request.headers.get("Range"), vf.size)
    except ValueError:
        return web.Response(status=416, headers={"Content-Range": f"bytes */{vf.size}"})
    start, end = byte_range or (0, vf.size - 1)
    headers = {
        "Content-Type": vf.content_type,
        "Accept-Ranges": "bytes",
        "Content-Length": str(end - start + 1),
    }
    if byte_range:
        headers["Content-Range"] = f"bytes {start}-{end}/{vf.size}"
    resp = web.StreamResponse(status=206 if byte_range else 200, headers=headers)
    _keepalive(request)
    await resp.prepare(request)
    if request.method == "HEAD":
        return resp
    reader = vf.open_reader(peer_of(request), start)
    try:
        while reader.pos <= end:
            available = await vf.wait_data(reader)
            if not available:
                break
            await resp.write(vf.read(reader, min(available, CHUNK, end + 1 - reader.pos)))
    except (UpstreamError, OSError, *DISCONNECTS):
        pass
    finally:
        vf.close_reader(reader)
    return resp


def _learn_duration(request: web.Request, vf) -> None:
    """The buffer window is in minutes, so the proxy needs the title's
    duration. Players usually fetch it through us before playing; for a
    movie we can also ask the provider's API ourselves."""
    entry = request.app[CATALOG].get(vf.kind, vf.stream_id)
    if entry.duration:
        vf.duration = entry.duration
    if vf.duration or vf.kind != "movie":
        return
    user, password = request.match_info["user"], request.match_info["password"]
    path = f"/player_api.php?username={user}&password={password}&action=get_vod_info&vod_id={vf.stream_id}"

    async def fetch() -> None:
        upstream = request.app[UPSTREAM]
        try:
            async with await upstream.get(
                upstream.url(path), forward_headers(request.headers), timeout=API_TIMEOUT
            ) as resp:
                data = xtream.parse_json(await resp.read()) if resp.status == 200 else None
        except (aiohttp.ClientError, TimeoutError, UpstreamError):
            return
        request.app[CATALOG].harvest("get_vod_info", data)
        vf.duration = vf.duration or request.app[CATALOG].get(vf.kind, vf.stream_id).duration

    asyncio.create_task(fetch())


# --- live ---------------------------------------------------------------------------------


def _hls_content(stream_id: str) -> str:
    """HLS and MPEG-TS of one channel each need their own provider
    connection, so they are different content: the last asked for wins."""
    return f"live {stream_id} hls"


async def live(request: web.Request) -> web.StreamResponse:
    if blocked := _unavailable(request):
        return blocked
    stream_id, ext = _split_name(request.match_info["name"])
    if stream_id.isdigit() and ext == "m3u8":
        # HLS is relayed unbuffered; remember the channel for the status page.
        request.app[HLS][peer_of(request)] = (stream_id, time.time())
        return await relay(request, content=_hls_content(stream_id))
    if not stream_id.isdigit() or ext not in ("", "ts"):
        return await relay(request)
    if busy := _busy(request, f"live {stream_id}"):
        return busy
    manager = request.app[LIVE]
    channel = manager.get(stream_id)
    channel.request(request.path_qs, forward_headers(request.headers))
    reader = manager.open_reader(channel, peer_of(request))
    try:
        try:
            await asyncio.wait_for(channel.wait_data(reader), 30)
        except TimeoutError:
            return web.Response(status=504, text="provider did not start the stream")
        if channel.head == 0:
            error = channel.error or UpstreamError(502, channel.last_error or "stream stopped")
            return web.Response(status=error.status, text=str(error))
        resp = web.StreamResponse(headers={"Content-Type": "video/mp2t"})
        _keepalive(request)
        await resp.prepare(request)
        if request.method == "HEAD":
            return resp
        with contextlib.suppress(OSError, *DISCONNECTS):
            while True:
                available = await channel.wait_data(reader)
                if not available:
                    break
                data = channel.read(reader.pos, min(available, CHUNK))
                await resp.write(data)
                reader.pos += len(data)
        return resp
    finally:
        manager.close_reader(channel, reader)


async def short_live(request: web.Request) -> web.StreamResponse:
    """/{user}/{password}/{id} is the short Xtream form of a live URL."""
    stream_id, _ = _split_name(request.match_info["name"])
    if not stream_id.isdigit():
        return await relay(request)
    return await live(request)


# --- plain relay ------------------------------------------------------------------------------


def _relay_content(url: str) -> str:
    """Content label of a stream relayed as is. Catch-up (timeshift) is
    named after its channel and start; for anything else the URL holds the
    credentials, so only a digest of it is shown."""
    parts = urlsplit(url)
    segments = parts.path.strip("/").split("/")
    if len(segments) == 6 and segments[0] == "timeshift":
        # /timeshift/{user}/{password}/{duration}/{start}/{stream_id}.ts
        return f"timeshift {segments[5].split('.')[0]} {segments[4]}"
    if parts.path.endswith("/timeshift.php"):
        query = parse_qs(parts.query)
        if "stream" in query and "start" in query:
            return f"timeshift {query['stream'][0]} {query['start'][0]}"
    return "relay " + hashlib.sha256(url.encode()).hexdigest()[:10]


def _relay_token(url: str, content: str) -> str:
    """A playlist entry remembers the content of its playlist."""
    return base64.urlsafe_b64encode(f"{content}\n{url}".encode()).decode().rstrip("=")


def _parse_relay_token(token: str) -> tuple[str, str]:
    """(content, url); raises ValueError for a malformed token."""
    content, newline, url = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4)).decode().partition("\n")
    if not newline:
        raise ValueError(token)
    return content, url


def _rewrite_playlist(app: web.Application, text: str, base_url: str, content: str) -> str:
    """Send every URL in an HLS playlist back through /_relay/."""
    allowed = app[RELAY_HOSTS]

    def through_proxy(url: str) -> str:
        absolute = urljoin(base_url, url.strip())
        allowed.add(urlsplit(absolute).netloc)
        return f"/_relay/{_relay_token(absolute, content)}"

    lines = []
    for line in text.splitlines():
        if line and not line.startswith("#"):
            line = through_proxy(line)
        elif 'URI="' in line:
            line = re.sub(r'URI="([^"]+)"', lambda m: f'URI="{through_proxy(m.group(1))}"', line)
        lines.append(line)
    return "\n".join(lines) + "\n"


async def relay(
    request: web.Request, url: str | None = None, content: str | None = None, *, may_switch: bool = True
) -> web.StreamResponse:
    if blocked := _unavailable(request):
        return blocked
    upstream = request.app[UPSTREAM]
    url = url or upstream.url(request.path_qs)
    content = content or _relay_content(url)
    path = urlsplit(url).path.lower()
    holds_slot = not path.endswith(IMAGE_SUFFIXES)
    if holds_slot and (busy := _busy(request, content, may_switch=may_switch)):
        return busy
    passthrough = request.app[RELAY]
    extra = {"Range": request.headers["Range"]} if "Range" in request.headers else {}
    up: aiohttp.ClientResponse | None = None

    def connect() -> Awaitable[aiohttp.ClientResponse]:
        return upstream.get(url, forward_headers(request.headers, extra))

    try:
        try:
            up = await (passthrough.open(content, connect) if holds_slot else connect())
        except UpstreamError as exc:
            return web.Response(status=exc.status, text=str(exc))
        except (aiohttp.ClientError, TimeoutError) as exc:
            return web.Response(status=502, text=f"provider unreachable: {exc}")
        content_type = up.headers.get("Content-Type", "")
        if "mpegurl" in content_type.lower() or str(up.url.path).lower().endswith(".m3u8"):
            text = (await up.read()).decode("utf-8", "replace")
            body = _rewrite_playlist(request.app, text, str(up.url), content)
            return web.Response(status=up.status, text=body, content_type="application/vnd.apple.mpegurl")
        headers = {k: up.headers[k] for k in RELAY_RESPONSE_HEADERS if k in up.headers}
        if up.content_length is not None and "Content-Encoding" not in up.headers:
            headers["Content-Length"] = str(up.content_length)
        resp = web.StreamResponse(status=up.status, headers=headers)
        await resp.prepare(request)
        if request.method != "HEAD":
            with contextlib.suppress(aiohttp.ClientError, OSError, *DISCONNECTS):
                async for chunk in up.content.iter_chunked(CHUNK):
                    await resp.write(chunk)
        return resp
    finally:
        if up is not None:
            up.close()
        if holds_slot:
            passthrough.end(up)


async def relay_token(request: web.Request) -> web.StreamResponse:
    token = request.match_info["token"]
    try:
        content, url = _parse_relay_token(token)
    except ValueError:
        raise web.HTTPNotFound() from None
    if urlsplit(url).netloc not in request.app[RELAY_HOSTS]:
        raise web.HTTPForbidden()
    hls = request.app[HLS].get(peer_of(request))
    if hls and content == _hls_content(hls[0]):
        request.app[HLS][peer_of(request)] = (hls[0], time.time())
    # Segments continue their playlist; a late one must not switch back to it.
    return await relay(request, url, content, may_switch=False)


async def fallback(request: web.Request) -> web.StreamResponse:
    if request.path in xtream.API_PATHS:
        return await api(request)
    return await relay(request)


# --- status page -------------------------------------------------------------------------------


async def status(request: web.Request) -> web.Response:
    app = request.app
    settings = app[SETTINGS]
    catalog = app[CATALOG]
    cache = app[VOD]
    holder = app[CONNECTION].holder
    now = time.time()

    live_items = []
    for ch in app[LIVE].channels.values():
        live_items.append({
            "id": ch.stream_id,
            "name": catalog.get("live", ch.stream_id).name,
            "state": ch.state,
            "error": ch.last_error if ch.state in ("reconnecting", "error") else None,
            "rate": ch.rate.rate(),
            "speed": ch.speed(),
            "buffer_bytes": ch.head - ch.start,
            "buffer_seconds": ch.behind_seconds(ch.start),
            "uptime": now - ch.started_at,
            "idle_for": now - ch.idle_since if ch.idle_since else None,
            "has_slot": holder is ch,
            "viewers": [
                {
                    "peer": r.peer,
                    "behind_seconds": ch.behind_seconds(r.pos),
                    "behind_bytes": ch.head - r.pos,
                    "waiting": r.waiting,
                    "connected_for": now - r.started,
                }
                for r in ch.readers
            ],
        })

    vod_items = []
    for vf in sorted(cache.files.values(), key=lambda v: v.last_read, reverse=True):
        entry = catalog.get(vf.kind, vf.stream_id)
        byte_rate = vf.byte_rate
        rate = vf.rate.rate()
        vod_items.append({
            "key": vf.key,
            "kind": vf.kind,
            "id": vf.stream_id,
            "name": entry.name,
            "duration": vf.duration,
            "duration_known": bool(vf.duration),
            "state": vf.state,
            "current": vf.current,
            "error": str(vf.error) if vf.error else (vf.last_error if vf.state == "retrying" else None),
            "size": vf.size,
            "downloaded": vf.downloaded,
            "rate": rate,
            "realtime_ratio": rate / byte_rate if vf.duration and vf.downloading else None,
            "capped": vf.capped,
            "has_slot": holder is vf,
            "last_read_ago": now - vf.last_read,
            "ranges": vf.ranges.to_list(),
            "play_pos": vf.play_pos,
            "play_seconds": vf.play_pos / byte_rate,
            "ahead_seconds": vf.ahead_bytes / byte_rate,
            "ahead_bytes": vf.ahead_bytes,
            "rest_downloaded": vf.size is not None and vf.play_pos + vf.ahead_bytes >= vf.size,
            "window_seconds": vf.window_bytes() / byte_rate if vf.size else None,
            "viewers": [
                {
                    "peer": r.peer,
                    "pos": r.pos,
                    "ahead_seconds": vf.ranges.covered_from(r.pos) / byte_rate,
                    "position_seconds": r.pos / byte_rate,
                    "waiting": r.waiting,
                }
                for r in vf.readers
            ],
        })

    viewer = app[CONNECTION].snapshot()
    kind, _, rest = (viewer["content"] or "").partition(" ")
    if kind == "timeshift":
        viewer["name"] = catalog.get("live", rest.split(" ")[0]).name

    try:
        free = shutil.disk_usage(cache.dir).free
    except OSError:
        free = None
    return web.json_response({
        "configured": settings.configured,
        "upstream": xtream.origin(settings.upstream_url) if settings.configured else None,
        "via_proxy": bool(settings.upstream_proxy),
        "vpn": app[VPN].snapshot() if VPN in app else None,
        "slot": holder.label if holder else None,
        "viewer": viewer,
        "hls": [
            {"peer": peer, "id": sid, "name": catalog.get("live", sid).name, "last_ago": now - last}
            for peer, (sid, last) in app[HLS].items()
            if now - last < HLS_ACTIVE_SECONDS
        ],
        "buffer_minutes": app[TUNABLES].buffer_minutes,
        "live": live_items,
        "vod": vod_items,
        "cache": {
            "used": cache.used_bytes(),
            "budget": settings.vod_cache_bytes,
            "free": free,
            "min_free": settings.min_free_bytes,
        },
    })


async def live_jump(request: web.Request) -> web.Response:
    channel = request.app[LIVE].channels.get(request.match_info["id"])
    if channel:
        for reader in channel.readers:
            reader.jump_to_live = True
    return web.json_response({"ok": bool(channel)})


async def live_stop(request: web.Request) -> web.Response:
    channel = request.app[LIVE].channels.get(request.match_info["id"])
    if channel:
        channel.stop()
    return web.json_response({"ok": bool(channel)})


async def update_settings(request: web.Request) -> web.Response:
    try:
        body = await request.json()
        request.app[TUNABLES].set_buffer_minutes(body["buffer_minutes"])
    except (ValueError, KeyError, TypeError) as exc:
        return web.json_response({"error": str(exc)}, status=400)
    request.app[VOD].wake_current()
    return web.json_response({"buffer_minutes": request.app[TUNABLES].buffer_minutes})


async def vod_delete(request: web.Request) -> web.Response:
    cache = request.app[VOD]
    vf = cache.files.get(request.match_info["key"])
    if vf:
        cache.remove(vf)
    return web.json_response({"ok": bool(vf)})


async def index(request: web.Request) -> web.Response:
    html = resources.files("xcbuffer").joinpath("static/index.html").read_text()
    return web.Response(text=html, content_type="text/html")


# --- app -----------------------------------------------------------------------------------------


def create_app(settings: Settings) -> web.Application:
    app = web.Application()
    app[SETTINGS] = settings
    app[CATALOG] = xtream.Catalog()
    app[RELAY_HOSTS] = set()
    app[CONNECTION] = ProviderConnection()
    app[HLS] = {}

    async def lifecycle(app: web.Application):
        tasks = []
        if settings.gluetun_control_url:
            app[VPN] = VpnMonitor(settings.gluetun_control_url)
            await app[VPN].refresh()
            tasks.append(asyncio.create_task(app[VPN].run()))
        app[UPSTREAM] = Upstream(settings, app.get(VPN))
        settings.data_dir.mkdir(parents=True, exist_ok=True)
        app[CATALOG].path = settings.data_dir / "catalog.json"
        app[CATALOG].load()
        app[TUNABLES] = Tunables(settings.data_dir / "settings.json", settings.buffer_minutes)
        app[VOD] = VodCache(settings, app[UPSTREAM], app[CONNECTION], app[TUNABLES])
        app[LIVE] = LiveManager(settings, app[UPSTREAM], app[CONNECTION], app[TUNABLES])
        app[RELAY] = Passthrough(app[CONNECTION], settings.passthrough_linger_seconds)
        if VPN in app:
            app[VPN].on_down = lambda: app[CONNECTION].holder and app[CONNECTION].holder.preempt()
        tasks += [
            asyncio.create_task(app[VOD].run_housekeeping()),
            asyncio.create_task(app[LIVE].run_housekeeping()),
        ]
        yield
        for task in tasks:
            task.cancel()
        app[LIVE].shutdown()
        app[VOD].shutdown()
        await app[UPSTREAM].close()
        if VPN in app:
            await app[VPN].close()

    app.cleanup_ctx.append(lifecycle)
    app.router.add_get("/", index)
    app.router.add_get("/_proxy/status", status)
    app.router.add_post("/_proxy/live/{id}/jump", live_jump)
    app.router.add_post("/_proxy/live/{id}/stop", live_stop)
    app.router.add_post("/_proxy/vod/{key}/delete", vod_delete)
    app.router.add_post("/_proxy/settings", update_settings)
    app.router.add_get("/_relay/{token}", relay_token)
    app.router.add_get(r"/{kind:movie|series}/{user}/{password}/{name}", vod)
    app.router.add_get("/live/{user}/{password}/{name}", live)
    app.router.add_get("/{user}/{password}/{name}", short_live)
    app.router.add_route("*", "/{tail:.*}", fallback)
    return app


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = Settings.from_env()
    if not settings.configured:
        log.warning("UPSTREAM_URL is not set; only the status page will work")
    web.run_app(create_app(settings), port=8080, access_log=None)


if __name__ == "__main__":
    main()
