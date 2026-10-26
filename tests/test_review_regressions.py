"""Regressions from code review: each test reproduced a defect before its fix."""

import asyncio
import errno
import json
import os
from pathlib import Path
from unittest.mock import patch

import pytest
from aiohttp import web

from conftest import MOVIE, status, ts_packets, wait_for
from xcbuffer import app as app_module
from xcbuffer import vod as vod_module
from xcbuffer.app import CATALOG, CONNECTION, LIVE, RELAY, VOD
from xcbuffer.upstream import UpstreamError

MOVIE_URL = "/movie/user/pass/1.mp4"
LIVE_URL = "/live/user/pass/5.ts"


async def test_concurrent_passthrough_keeps_one_connection(provider, settings, make_proxy):
    client = await make_proxy(settings)
    # An unbuffered live format follows relay() rather than LiveChannel.
    a = await client.get("/live/user/pass/5.raw")
    await a.content.readexactly(188)
    b = await client.get("/live/user/pass/5.raw")
    await b.content.readexactly(188)
    a.close()
    b.close()
    assert provider.max_open_streams == 1


async def test_cached_movie_stops_previous_live(provider, settings, make_proxy):
    client = await make_proxy(settings)
    assert await (await client.get(MOVIE_URL)).read() == MOVIE
    live = await client.get(LIVE_URL)
    await live.content.readexactly(188)
    live.close()
    movie = await client.get(MOVIE_URL, headers={"Range": "bytes=0-9"})
    assert await movie.read() == MOVIE[:10]
    await asyncio.sleep(.2)
    s = await status(client)
    assert s["viewer"]["content"] == "movie 1"
    assert s["slot"] != "live 5"


async def test_delete_active_movie_finishes_waiters(provider, settings, make_proxy):
    provider.movie_delay = .05
    client = await make_proxy(settings)
    movie = await client.get(MOVIE_URL)
    await movie.content.readexactly(1024)
    vf = client.app[VOD].files["movie_1"]
    reader = vf.open_reader("test", len(MOVIE) - 10)
    waiter = asyncio.create_task(vf.wait_data(reader))
    await asyncio.sleep(.01)
    assert (await client.post("/_proxy/vod/movie_1/delete")).status == 200
    await asyncio.sleep(.1)
    finished = waiter.done()
    waiter.cancel()
    await asyncio.gather(waiter, return_exceptions=True)
    vf.close_reader(reader)
    movie.close()
    assert finished, "reader remains waiting forever after its file was deleted"


async def test_catalog_disk_failure_does_not_break_api(provider, settings, make_proxy, tmp_path):
    client = await make_proxy(settings)
    catalog = client.app[CATALOG]
    # A write failure in the optional name cache must not replace provider success.
    with patch.object(type(catalog.path), "write_text", side_effect=OSError(28, "No space left on device")):
        resp = await client.get("/player_api.php?username=u&password=p&action=get_live_streams")
    assert resp.status == 200


async def test_slot_released_when_vod_metadata_write_fails(provider, settings, make_proxy):
    provider.movie_delay = .05
    client = await make_proxy(settings)
    movie = await client.get(MOVIE_URL)
    await movie.content.readexactly(1024)
    vf = client.app[VOD].files["movie_1"]
    with patch.object(vf, "_save_meta", side_effect=OSError(28, "No space left on device")):
        vf.preempt()
        await asyncio.sleep(.1)
        # Before the fix the failed task kept the slot and this hung forever.
        live = await asyncio.wait_for(client.get(LIVE_URL), 5)
        await asyncio.wait_for(live.content.readexactly(188), 5)
        assert client.app[CONNECTION].holder.label == "live 5"
    live.close()
    movie.close()

# --- second review ---


async def test_switch_while_relay_is_waiting_for_headers_keeps_one_connection(
    provider, settings, make_proxy, aiohttp_server,
):
    opened = asyncio.Event()
    headers_allowed = asyncio.Event()

    async def delayed_relay(request):
        provider._enter(request)
        opened.set()
        try:
            await headers_allowed.wait()
            resp = web.StreamResponse()
            await resp.prepare(request)
            while True:
                await resp.write(ts_packets(0, 250))
                await asyncio.sleep(.01)
        finally:
            provider.open_streams -= 1

    app = web.Application()
    app.router.add_get("/slow-relay", delayed_relay)
    app.router.add_get("/movie/{user}/{password}/{name}", provider.movie)
    app.router.add_get("/player_api.php", provider.player_api)
    server = await aiohttp_server(app)
    client = await make_proxy(settings, upstream_url=str(server.make_url("")).rstrip("/"))
    provider.movie_delay = .05
    relay = asyncio.create_task(client.get("/slow-relay"))
    await asyncio.wait_for(opened.wait(), 2)
    movie = await asyncio.wait_for(client.get("/movie/user/pass/1.mp4"), 2)
    await movie.content.readexactly(100)
    concurrent = provider.max_open_streams
    headers_allowed.set()
    relay_resp = await asyncio.wait_for(relay, 2)
    relay_status = relay_resp.status
    relay_resp.close()
    movie.close()
    assert concurrent == 1, "pending relay survives preemption and overlaps new movie"
    # The movie asked last, so the relay that was still connecting lost.
    assert relay_status == 503


async def test_second_request_to_chunked_vod_is_relayed(settings, make_proxy, aiohttp_server, monkeypatch):
    async def chunked(request):
        resp = web.StreamResponse(headers={"Content-Type": "video/mp4"})
        await resp.prepare(request)
        await resp.write(b"video data")
        return resp

    app = web.Application()
    app.router.add_get("/movie/{user}/{password}/{name}", chunked)
    server = await aiohttp_server(app)
    client = await make_proxy(settings, upstream_url=str(server.make_url("")).rstrip("/"))
    url = "/movie/user/pass/1.mp4"
    first = await client.get(url)
    assert await first.read() == b"video data"
    # Exercise the real timeout path without waiting for its 90-second default.
    vf = next(iter(client.app[app_module.VOD].files.values()))
    wait_ready = vf.wait_ready

    async def shorter_wait():
        return await wait_ready(timeout=.05)

    monkeypatch.setattr(vf, "wait_ready", shorter_wait)
    second = await asyncio.wait_for(client.get(url), 2)
    assert second.status == 200
    assert await second.read() == b"video data"


async def test_sharing_tv_is_also_displaced_on_switch(provider, settings, make_proxy, monkeypatch):
    monkeypatch.setattr(app_module, "peer_of", lambda req: req.headers.get("X-Peer", "tv-a"))
    provider.movie_delay = .05
    client = await make_proxy(settings)
    a = await client.get("/live/user/pass/5.ts", headers={"X-Peer": "tv-a"})
    await a.content.readexactly(188)
    b = await client.get("/live/user/pass/5.ts", headers={"X-Peer": "tv-b"})
    await b.content.readexactly(188)
    movie = await client.get("/movie/user/pass/1.mp4", headers={"X-Peer": "tv-a"})
    await movie.content.readexactly(100)
    b_reconnect = await client.get("/live/user/pass/5.ts", headers={"X-Peer": "tv-b"})
    response_status = b_reconnect.status
    viewer = (await status(client))["viewer"]
    for response in (a, b, movie, b_reconnect):
        response.close()
    assert response_status == 503, f"auto-reconnect stole slot: {viewer}"

# --- third review ---


async def test_preempt_relay_while_another_relay_waits_for_handover(provider, settings, make_proxy):
    provider.movie_delay = .02
    client = await make_proxy(settings)
    a = await client.get("/live/user/pass/5.raw")
    await a.content.readexactly(188)
    b = asyncio.create_task(client.get("/live/user/pass/6.raw"))
    await asyncio.sleep(.05)
    relay = client.app[RELAY]
    assert relay._opening.locked() and relay._pending is None and relay.active
    movie = await asyncio.wait_for(client.get("/movie/user/pass/1.mp4"), 3)
    await movie.content.readexactly(188)
    b_response = await asyncio.wait_for(b, 3)
    for response in (a, b_response, movie):
        response.close()
    assert provider.max_open_streams == 1


async def test_old_movie_reader_does_not_resume_download_after_switch(provider, settings, make_proxy):
    provider.movie_delay = .01
    client = await make_proxy(settings)
    first = await client.get("/movie/user/pass/1.mp4")
    await first.content.readexactly(1024)
    consume_first = asyncio.create_task(first.content.read())
    second = await client.get("/movie/user/pass/2.mp4")
    assert await second.read() == MOVIE
    await asyncio.sleep(.3)
    old_requests = [p for p in provider.stream_requests if "/movie/user/pass/1.mp4" in p]
    consume_first.cancel()
    await asyncio.gather(consume_first, return_exceptions=True)
    first.close()
    client.app[VOD].remove(client.app[VOD].files["movie_1"])
    assert len(old_requests) == 1, f"displaced title continued downloading: {old_requests}"


async def test_416_does_not_mark_unwritten_bytes_as_cached(provider, settings, make_proxy, aiohttp_server):
    reject = False

    async def movie(request):
        if reject:
            # The backing object has been replaced with a shorter file.
            return web.Response(status=416, headers={"Content-Range": "bytes */1024"})
        return await provider.movie(request)

    app = web.Application()
    app.router.add_get("/movie/{user}/{password}/{name}", movie)
    app.router.add_get("/player_api.php", provider.player_api)
    server = await aiohttp_server(app)
    provider.movie_delay = .005
    client = await make_proxy(settings, buffer_minutes=1, upstream_url=str(server.make_url("")).rstrip("/"))
    first = await client.get("/movie/user/pass/1.mp4", headers={"Range": "bytes=0-9"})
    assert await first.read() == MOVIE[:10]
    vf = client.app[VOD].files["movie_1"]

    async def full():
        return vf.state == "full"

    await wait_for(full)
    reject = True
    # Isolate the fetch. Through the full HTTP handler the bogus coverage
    # also leaves a waiting reader in a busy loop that starves the event loop.
    start = len(MOVIE) - 100
    with pytest.raises(UpstreamError):
        await vf._fetch(start)
    available = vf.ranges.covered_from(start)
    assert available == 0, f"416 fabricated {available} bytes of cache coverage"
    # The copy is dropped; the next request starts a fresh one.
    assert vf.stale
    assert client.app[VOD].get("movie", "1", "mp4") is not vf
    client.app[VOD].remove(client.app[VOD].files["movie_1"])


async def test_failed_hole_punch_preserves_accounting(provider, settings, make_proxy, monkeypatch):
    monkeypatch.setattr(vod_module, "KEEP_EDGE_BYTES", 1024 * 1024)
    monkeypatch.setattr(vod_module, "KEEP_BEHIND_SECONDS", 10)
    client = await make_proxy(settings)
    assert await (await client.get("/movie/user/pass/1.mp4")).read() == MOVIE
    vf = client.app[VOD].files["movie_1"]
    before = vf.downloaded

    def unsupported(*args):
        raise OSError(errno.EOPNOTSUPP, "Operation not supported")

    monkeypatch.setattr(vod_module, "punch_hole", unsupported)
    vf.trim_played()
    actual_bytes = vf.path.stat().st_blocks * 512
    assert vf.downloaded == before, f"accounted {vf.downloaded} bytes but {actual_bytes} still allocated"


# --- connection ownership ---


async def test_relay_queued_before_switch_does_not_take_the_connection(provider, settings, make_proxy):
    provider.movie_delay = .02
    client = await make_proxy(settings)
    a = await client.get("/live/user/pass/5.raw")
    await a.content.readexactly(188)
    # b waits for a to finish; c waits behind b. Both were admitted before the switch.
    b = asyncio.create_task(client.get("/live/user/pass/6.raw"))
    c = asyncio.create_task(client.get("/live/user/pass/7.raw"))
    await asyncio.sleep(.1)
    movie = await asyncio.wait_for(client.get(MOVIE_URL), 15)
    body = await asyncio.wait_for(movie.read(), 15)
    responses = [a, movie] + [await asyncio.wait_for(t, 3) for t in (b, c)]
    for response in responses:
        response.close()
    stale = [p for p in provider.stream_requests if "/7.raw" in p]
    assert not stale, "a relay queued before the switch connected after it"
    assert body == MOVIE


async def test_ts_viewer_takes_channel_from_hls_viewer(provider, settings, make_proxy, monkeypatch):
    monkeypatch.setattr(app_module, "peer_of", lambda req: req.headers.get("X-Peer", "tv-a"))
    client = await make_proxy(settings)
    hls = {"X-Peer": "tv-a"}
    playlist = await (await client.get("/live/user/pass/5.m3u8", headers=hls)).text()
    segment = next(line for line in playlist.splitlines() if line.startswith("/_relay/"))
    assert (await client.get(segment, headers=hls)).status == 200
    ts = await client.get(LIVE_URL, headers={"X-Peer": "tv-b"})
    await ts.content.readexactly(188)
    # The HLS player keeps fetching segments; the newer TS viewer must keep the channel.
    next_segment = await client.get(segment, headers=hls)
    still_streaming = not client.app[LIVE].channels["5"].stopped
    ts.close()
    assert next_segment.status == 503
    assert still_streaming


# --- cache accounting ---


async def test_short_disk_write_is_not_marked_as_cached(provider, settings, make_proxy, monkeypatch):
    real_pwrite = os.pwrite

    def short_pwrite(fd, data, offset):
        # Regular files may take less than asked, e.g. when the disk fills up.
        return real_pwrite(fd, bytes(data[: max(1, len(data) // 2)]), offset)

    monkeypatch.setattr(os, "pwrite", short_pwrite)
    client = await make_proxy(settings)
    body = await (await client.get(MOVIE_URL)).read()
    assert body == MOVIE


async def test_hole_punch_without_saved_metadata_cannot_resurrect_ranges(
    provider, settings, make_proxy, monkeypatch
):
    monkeypatch.setattr(vod_module, "KEEP_EDGE_BYTES", 1024 * 1024)
    monkeypatch.setattr(vod_module, "KEEP_BEHIND_SECONDS", 10)
    client = await make_proxy(settings)
    assert await (await client.get(MOVIE_URL)).read() == MOVIE
    cache = client.app[VOD]
    vf = cache.files["movie_1"]

    def disk_full(*args, **kwargs):
        raise OSError(errno.ENOSPC, "No space left on device")

    with monkeypatch.context() as m:
        m.setattr(Path, "write_text", disk_full)
        vf.trim_played()
    # After a crash the cache is rebuilt from the metadata on disk.
    restarted = vod_module.VodFile.load(cache, vf.meta_path)
    on_disk = vf.path.read_bytes()
    for start, end in restarted.ranges.to_list():
        assert on_disk[start:end] == MOVIE[start:end], f"bytes {start}-{end} claimed but freed"


async def test_unrecorded_bytes_are_freed_at_startup(provider, settings, make_proxy):
    vod_dir = settings.data_dir / "vod"
    vod_dir.mkdir(parents=True)
    # Written in full, but a crash left metadata that knows only the first MiB.
    (vod_dir / "movie_1.mp4").write_bytes(MOVIE)
    (vod_dir / "movie_1.json").write_text(json.dumps({
        "kind": "movie", "stream_id": "1", "ext": "mp4", "size": len(MOVIE),
        "content_type": "video/mp4", "ranges": [[0, 1024 * 1024]],
    }))
    client = await make_proxy(settings)
    vf = client.app[VOD].files["movie_1"]
    allocated = vf.path.stat().st_blocks * 512
    assert allocated < 2 * 1024 * 1024, f"{allocated} bytes allocated but 1 MiB accounted"
    head = await client.get(MOVIE_URL, headers={"Range": "bytes=0-999"})
    assert await head.read() == MOVIE[:1000]
