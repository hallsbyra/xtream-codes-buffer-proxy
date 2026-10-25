from __future__ import annotations

import asyncio
import json
from dataclasses import replace

import pytest
from aiohttp import web

from xcbuffer.app import create_app
from xcbuffer.config import Settings

MOVIE = bytes((i * 7 + i // 251) % 256 for i in range(24 * 1024 * 1024))
TS_PACKET = 188


def ts_packets(start_pcr_seconds: float, count: int) -> bytes:
    """MPEG-TS packets; every 10th carries a PCR, 40 ms apart."""
    out = bytearray()
    pcr = start_pcr_seconds
    for i in range(count):
        if i % 10 == 0:
            base = int(pcr * 90000)
            adaptation = bytes([
                7, 0x10,
                (base >> 25) & 0xFF, (base >> 17) & 0xFF, (base >> 9) & 0xFF, (base >> 1) & 0xFF,
                ((base & 1) << 7) | 0x7E, 0,
            ])
            packet = bytes([0x47, 0x01, 0x00, 0x30]) + adaptation
            pcr += 0.04
        else:
            packet = bytes([0x47, 0x01, 0x00, 0x10])
        out += packet + bytes([i % 256]) * (TS_PACKET - len(packet))
    return bytes(out)


class FakeProvider:
    """Xtream Codes provider that allows one stream at a time and records how
    many streams were ever open at once."""

    def __init__(self) -> None:
        self.open_streams = 0
        self.max_open_streams = 0
        self.stream_requests: list[str] = []
        self.movie_delay = 0.0
        self.live_interval = 0.01
        self.app = web.Application()
        self.app.router.add_get("/player_api.php", self.player_api)
        self.app.router.add_get("/get.php", self.get_php)
        self.app.router.add_get("/movie/{user}/{password}/{name}", self.movie)
        self.app.router.add_get("/live/{user}/{password}/{name}", self.live)
        self.app.router.add_get("/hls/{token}/{name}", self.hls_segment)
        self.origin = ""

    async def player_api(self, request: web.Request) -> web.Response:
        action = request.query.get("action")
        if action == "get_vod_info":
            return web.json_response({
                "info": {"name": "Testfilm", "duration_secs": 240},
                "movie_data": {"stream_id": 1, "name": "Testfilm"},
            })
        if action == "get_live_streams":
            return web.json_response([{"stream_id": 5, "name": "Kanal 5"}])
        return web.json_response({
            "user_info": {"username": request.query.get("username"), "auth": 1, "max_connections": "1"},
            "server_info": {"url": "provider.example", "port": "8080", "server_protocol": "http",
                            "direct": f"{self.origin}/x"},
        })

    async def get_php(self, request: web.Request) -> web.Response:
        return web.Response(text=f"#EXTM3U\n#EXTINF:-1,Kanal 5\n{self.origin}/live/u/p/5.ts\n")

    def _enter(self, request: web.Request) -> None:
        self.stream_requests.append(request.path_qs + " " + request.headers.get("Range", ""))
        self.open_streams += 1
        self.max_open_streams = max(self.max_open_streams, self.open_streams)

    async def hls_segment(self, request: web.Request) -> web.Response:
        return web.Response(body=ts_packets(0, 10), content_type="video/mp2t")

    async def movie(self, request: web.Request) -> web.StreamResponse:
        if request.match_info["name"].startswith("404"):
            raise web.HTTPNotFound()
        await asyncio.sleep(0.05)
        self._enter(request)
        try:
            size = len(MOVIE)
            start = 0
            rng = request.headers.get("Range")
            if rng:
                start = int(rng.split("=")[1].split("-")[0])
                resp = web.StreamResponse(status=206, headers={
                    "Content-Range": f"bytes {start}-{size - 1}/{size}",
                    "Content-Length": str(size - start), "Content-Type": "video/mp4"})
            else:
                resp = web.StreamResponse(headers={"Content-Length": str(size), "Content-Type": "video/mp4"})
            await resp.prepare(request)
            pos = start
            while pos < size:
                await resp.write(MOVIE[pos:pos + 256 * 1024])
                pos += 256 * 1024
                if self.movie_delay:
                    await asyncio.sleep(self.movie_delay)
            return resp
        finally:
            self.open_streams -= 1

    async def live(self, request: web.Request) -> web.StreamResponse:
        if request.match_info["name"].startswith("404"):
            raise web.HTTPNotFound()
        if request.match_info["name"].endswith(".m3u8"):
            # Real providers redirect to a tokenised playlist on another path.
            return web.Response(
                text="#EXTM3U\n#EXT-X-TARGETDURATION:4\n#EXTINF:4,\n/hls/abc/1.ts\n#EXTINF:4,\n2.ts\n",
                content_type="application/vnd.apple.mpegurl",
            )
        await asyncio.sleep(0.05)
        self._enter(request)
        try:
            resp = web.StreamResponse(headers={"Content-Type": "video/mp2t"})
            await resp.prepare(request)
            pcr = 0.0
            while True:
                await resp.write(ts_packets(pcr, 250))  # 1 s of playback
                pcr += 1.0
                await asyncio.sleep(self.live_interval)
        finally:
            self.open_streams -= 1


class FakeGluetun:
    def __init__(self) -> None:
        self.status = "running"
        self.app = web.Application()
        self.app.router.add_get("/v1/vpn/status", self.vpn_status)
        self.app.router.add_get("/v1/publicip/ip", self.public_ip)
        self.url = ""

    async def vpn_status(self, request: web.Request) -> web.Response:
        return web.json_response({"status": self.status})

    async def public_ip(self, request: web.Request) -> web.Response:
        return web.json_response({"public_ip": "198.51.100.7", "city": "Amsterdam", "country": "Netherlands"})


@pytest.fixture
async def gluetun(aiohttp_server):
    fake = FakeGluetun()
    server = await aiohttp_server(fake.app)
    fake.url = str(server.make_url("")).rstrip("/")
    return fake


@pytest.fixture
async def provider(aiohttp_server):
    fake = FakeProvider()
    server = await aiohttp_server(fake.app)
    fake.origin = str(server.make_url("")).rstrip("/")
    return fake


@pytest.fixture
def settings(tmp_path, provider):
    return Settings(
        upstream_url=provider.origin,
        upstream_proxy=None,
        gluetun_control_url=None,
        data_dir=tmp_path,
        vod_cache_bytes=200 * 1024 * 1024,
        min_free_bytes=0,
        vod_keep_seconds=3600,
        buffer_minutes=30,
        live_buffer_bytes=64 * 1024 * 1024,
        live_idle_seconds=600,
        live_start_delay_seconds=0,
        passthrough_linger_seconds=0.2,
    )


@pytest.fixture
def make_proxy(aiohttp_client):
    async def make(settings: Settings, **overrides):
        return await aiohttp_client(create_app(replace(settings, **overrides)))

    return make


async def status(client) -> dict:
    resp = await client.get("/_proxy/status")
    return json.loads(await resp.text())


async def wait_for(predicate, timeout: float = 10, interval: float = 0.05):
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        result = await predicate()
        if result:
            return result
        if loop.time() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(interval)
