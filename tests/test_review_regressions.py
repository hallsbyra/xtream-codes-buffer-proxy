"""Regressions from code review: each test reproduced a defect before its fix."""

import asyncio
from unittest.mock import patch

from conftest import MOVIE, status
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
