"""Regressions from code review: each test reproduced a defect before its fix."""

import asyncio
from unittest.mock import patch

from aiohttp import web

from conftest import MOVIE, status, ts_packets
from xcbuffer import app as app_module
from xcbuffer.app import CATALOG, SLOT, VOD

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
        assert client.app[SLOT].holder.label == "live 5"
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
