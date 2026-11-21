import asyncio
import json
import os
import re
from urllib.parse import urlsplit

from aiohttp import web

from conftest import MOVIE, status, wait_for
from xcbuffer import app as app_module
from xcbuffer import vod as vod_module
from xcbuffer.app import CONNECTION, VOD, VPN, _relay_token

MOVIE_URL = "/movie/user/pass/1.mp4"
LIVE_URL = "/live/user/pass/5.ts"


async def test_login_points_tv_at_proxy(provider, settings, make_proxy):
    client = await make_proxy(settings)
    resp = await client.get("/player_api.php?username=user&password=pass")
    data = json.loads(await resp.text())
    assert data["server_info"]["url"] == "127.0.0.1"
    assert data["server_info"]["port"] == str(client.port)
    assert provider.origin not in await resp.text()


async def test_playlist_urls_are_rewritten(provider, settings, make_proxy):
    client = await make_proxy(settings)
    text = await (await client.get("/get.php?username=user&password=pass&type=m3u_plus")).text()
    assert provider.origin not in text
    assert f"http://127.0.0.1:{client.port}/live/u/p/5.ts" in text


async def test_paused_movie_keeps_downloading_to_the_end(provider, settings, make_proxy):
    provider.movie_delay = 0.005
    client = await make_proxy(settings)
    await client.get("/player_api.php?username=u&password=p&action=get_vod_info&vod_id=1")
    resp = await client.get(MOVIE_URL)
    assert resp.status == 200
    assert int(resp.headers["Content-Length"]) == len(MOVIE)
    first = await resp.content.readexactly(64 * 1024)
    # The TV is paused now; the proxy should still fetch the whole file.

    async def complete():
        vod = (await status(client))["vod"]
        return vod and vod[0]["state"] == "complete" and vod[0]

    item = await wait_for(complete, timeout=20)
    assert item["name"] == "Testfilm"
    assert item["viewers"][0]["ahead_seconds"] > 100
    rest = await resp.content.read()
    assert first + rest == MOVIE
    assert provider.max_open_streams == 1


async def test_seek_to_end_then_play_from_start(provider, settings, make_proxy):
    provider.movie_delay = 0.01
    client = await make_proxy(settings)
    tail = await client.get(MOVIE_URL, headers={"Range": f"bytes={len(MOVIE) - 1000}-"})
    assert tail.status == 206
    assert tail.headers["Content-Range"] == f"bytes {len(MOVIE) - 1000}-{len(MOVIE) - 1}/{len(MOVIE)}"
    assert await tail.read() == MOVIE[-1000:]
    middle = await client.get(MOVIE_URL, headers={"Range": "bytes=1000000-1999999"})
    assert await middle.read() == MOVIE[1000000:2000000]
    full = await client.get(MOVIE_URL)
    assert await full.read() == MOVIE
    assert provider.max_open_streams == 1


async def test_cached_movie_survives_restart(provider, settings, make_proxy):
    client = await make_proxy(settings)
    assert await (await client.get(MOVIE_URL)).read() == MOVIE
    await client.close()
    requests_before = len(provider.stream_requests)
    client = await make_proxy(settings)
    assert (await status(client))["vod"][0]["state"] == "complete"
    assert await (await client.get(MOVIE_URL)).read() == MOVIE
    assert len(provider.stream_requests) == requests_before


async def test_live_buffers_while_paused_and_resumes_on_reconnect(provider, settings, make_proxy):
    provider.live_interval = 0.02
    client = await make_proxy(settings)
    resp = await client.get(LIVE_URL)
    assert resp.status == 200
    data = await resp.content.readexactly(188 * 1000)
    assert data[0] == 0x47

    async def behind():
        ch = (await status(client))["live"][0]
        return ch["viewers"] and ch["viewers"][0]["behind_seconds"] > 20 and ch["speed"] and ch

    ch = await wait_for(behind)
    assert ch["speed"] > 1
    resp.close()

    async def viewer_gone():
        ch = (await status(client))["live"][0]
        return not ch["viewers"]

    await wait_for(viewer_gone)
    # Let live run well past what socket buffers can hold before reconnecting.
    left_at = (await status(client))["live"][0]["buffer_bytes"]

    async def grown():
        return (await status(client))["live"][0]["buffer_bytes"] > left_at + 16 * 1024 * 1024

    await wait_for(grown)
    again = await client.get(LIVE_URL)
    await again.content.readexactly(188)
    ch = (await status(client))["live"][0]
    # Resumed where the first connection stopped, not at live.
    assert ch["viewers"][0]["behind_seconds"] > 20
    await client.post("/_proxy/live/5/jump")

    async def keep_watching():
        while True:
            await again.content.read(65536)

    watching = asyncio.create_task(keep_watching())

    async def near_live():
        ch = (await status(client))["live"][0]
        return ch["viewers"][0]["behind_seconds"] < 10

    await wait_for(near_live)
    watching.cancel()
    again.close()
    assert provider.max_open_streams == 1


async def test_switching_to_live_stops_the_movie_download(provider, settings, make_proxy):
    provider.movie_delay = 0.02
    client = await make_proxy(settings)
    movie = await client.get(MOVIE_URL)
    await movie.content.readexactly(1024)
    movie.close()

    async def movie_downloading():
        vod = (await status(client))["vod"]
        return vod and vod[0]["state"] == "downloading"

    await wait_for(movie_downloading)

    # Switching to live stops the movie download for good; no queue.
    tv = await client.get(LIVE_URL)
    await tv.content.readexactly(188 * 100)
    s = await status(client)
    assert s["slot"] == "live 5"
    assert s["vod"][0]["state"] == "stopped"
    tv.close()
    await asyncio.sleep(1)
    assert (await status(client))["vod"][0]["state"] == "stopped"

    # Going back to the movie picks up where the download stopped.
    assert await (await client.get(MOVIE_URL)).read() == MOVIE
    assert provider.max_open_streams == 1


async def test_unknown_paths_are_relayed(provider, settings, make_proxy):
    client = await make_proxy(settings)
    resp = await client.get("/player_api.php?username=u&password=p&action=get_live_streams")
    assert json.loads(await resp.text())[0]["name"] == "Kanal 5"
    resp = await client.get("/does/not/exist.txt")
    assert resp.status == 404


async def test_status_page_without_upstream(settings, make_proxy):
    client = await make_proxy(settings, upstream_url="")
    assert (await status(client))["configured"] is False
    assert "IPTV Buffer" in await (await client.get("/")).text()
    assert (await client.get(MOVIE_URL)).status == 503


async def test_provider_error_reaches_the_tv(provider, settings, make_proxy):
    client = await make_proxy(settings)
    resp = await client.get("/movie/user/pass/404.mp4")
    assert resp.status == 404
    resp = await client.get("/live/user/pass/404.ts")
    assert resp.status == 404


async def test_slow_reader_does_not_block_other_requests(provider, settings, make_proxy):
    client = await make_proxy(settings)
    movie = await client.get(MOVIE_URL)
    await movie.content.readexactly(1024)
    api = await asyncio.wait_for(client.get("/player_api.php?username=u&password=p"), 5)
    assert api.status == 200
    movie.close()


async def test_client_disconnect_mid_movie(provider, settings, make_proxy):
    client = await make_proxy(settings)
    resp = await client.get(MOVIE_URL)
    await resp.content.readexactly(1024)
    resp.close()
    resp = await client.get(MOVIE_URL, headers={"Range": "bytes=5-9"})
    assert await resp.read() == MOVIE[5:10]


async def test_hls_playlist_is_relayed_through_proxy(provider, settings, make_proxy):
    client = await make_proxy(settings)
    playlist = await (await client.get("/live/user/pass/5.m3u8")).text()
    segments = [line for line in playlist.splitlines() if line and not line.startswith("#")]
    assert len(segments) == 2
    assert all(line.startswith("/_relay/") for line in segments)
    first = await client.get(segments[0])
    assert first.status == 200
    assert (await first.read())[0] == 0x47
    evil = _relay_token("http://evil.example/x", "live 5 hls")
    assert (await client.get(f"/_relay/{evil}")).status == 403


async def test_vpn_down_blocks_all_provider_traffic(provider, gluetun, settings, make_proxy):
    client = await make_proxy(settings, gluetun_control_url=gluetun.url)
    vpn = (await status(client))["vpn"]
    assert vpn["up"] is True
    assert vpn["public_ip"] == "198.51.100.7"
    assert vpn["location"] == "Amsterdam, Netherlands"

    gluetun.status = "stopped"
    await client.app[VPN].refresh()
    assert (await status(client))["vpn"]["up"] is False
    before = len(provider.stream_requests)
    for path in ("/player_api.php?username=u&password=p", MOVIE_URL, LIVE_URL, "/x/y/z.txt"):
        assert (await client.get(path)).status == 503, path
    assert len(provider.stream_requests) == before

    gluetun.status = "running"
    await client.app[VPN].refresh()
    assert (await client.get("/player_api.php?username=u&password=p")).status == 200


async def test_download_waits_for_vpn_instead_of_failing(provider, gluetun, settings, make_proxy):
    provider.movie_delay = 0.02
    client = await make_proxy(settings, gluetun_control_url=gluetun.url)
    movie = await client.get(MOVIE_URL)
    await movie.content.readexactly(1024)
    gluetun.status = "stopped"
    await client.app[VPN].refresh()

    async def waiting_for_vpn():
        return (await status(client))["vod"][0]["state"] == "vpn_down"

    await wait_for(waiting_for_vpn, timeout=20)
    gluetun.status = "running"
    await client.app[VPN].refresh()

    async def complete():
        return (await status(client))["vod"][0]["state"] == "complete"

    await wait_for(complete, timeout=30)
    movie.close()


async def test_unreachable_gluetun_counts_as_down(provider, settings, make_proxy):
    client = await make_proxy(settings, gluetun_control_url="http://127.0.0.1:9")
    vpn = (await status(client))["vpn"]
    assert vpn["status"] == "unreachable"
    assert (await client.get(MOVIE_URL)).status == 503


def _peer_header(monkeypatch):
    monkeypatch.setattr(app_module, "peer_of", lambda request: request.headers.get("X-Peer", "tv"))


async def test_buffer_window_stops_and_refills(provider, settings, make_proxy):
    provider.movie_delay = 0.005
    client = await make_proxy(settings, buffer_minutes=1)
    # 240 s and 24 MiB: one minute is about 6 MiB.
    await client.get("/player_api.php?username=u&password=p&action=get_vod_info&vod_id=1")
    resp = await client.get(MOVIE_URL)
    await resp.content.readexactly(64 * 1024)

    async def full():
        item = (await status(client))["vod"][0]
        return item["state"] == "full" and item

    item = await wait_for(full)
    assert item["downloaded"] < len(MOVIE)
    assert 55 < item["ahead_seconds"] < 70
    first_fill = item["downloaded"]

    await resp.content.readexactly(8 * 1024 * 1024)

    async def refilled():
        item = (await status(client))["vod"][0]
        return item["downloaded"] > first_fill + 4 * 1024 * 1024

    await wait_for(refilled)
    resp.close()


async def test_played_part_is_removed_from_disk(provider, settings, make_proxy, monkeypatch):
    monkeypatch.setattr(vod_module, "KEEP_EDGE_BYTES", 1024 * 1024)
    monkeypatch.setattr(vod_module, "KEEP_BEHIND_SECONDS", 10)
    client = await make_proxy(settings)
    await client.get("/player_api.php?username=u&password=p&action=get_vod_info&vod_id=1")
    assert await (await client.get(MOVIE_URL)).read() == MOVIE
    client.app[VOD].housekeeping()
    item = (await status(client))["vod"][0]
    assert item["downloaded"] <= 2 * 1024 * 1024 + 1
    vf = client.app[VOD].files["movie_1"]
    assert os.stat(vf.path).st_blocks * 512 < 4 * 1024 * 1024
    # Start and end stay, so the player can open the file again.
    head = await client.get(MOVIE_URL, headers={"Range": "bytes=0-999"})
    assert await head.read() == MOVIE[:1000]
    tail = await client.get(MOVIE_URL, headers={"Range": f"bytes={len(MOVIE) - 1000}-"})
    assert await tail.read() == MOVIE[-1000:]


async def test_housekeeping_trims_while_the_tv_watches(provider, settings, make_proxy, monkeypatch):
    monkeypatch.setattr(vod_module, "KEEP_EDGE_BYTES", 1024 * 1024)
    monkeypatch.setattr(vod_module, "KEEP_BEHIND_SECONDS", 10)
    client = await make_proxy(settings)
    await client.get("/player_api.php?username=u&password=p&action=get_vod_info&vod_id=1")
    resp = await client.get(MOVIE_URL, headers={"Range": f"bytes=0-{4 * 1024 * 1024 - 1}"})
    await resp.read()
    vf = client.app[VOD].files["movie_1"]
    await wait_for(lambda: asyncio.sleep(0, vf.ranges.covered_from(4 * 1024 * 1024) and not vf.downloading))
    # The TV plays on from the buffer; nothing downloads to trim behind it.
    reader = vf.open_reader("tv", 20 * 1024 * 1024)
    client.app[VOD].housekeeping()
    assert vf.downloaded < 8 * 1024 * 1024
    vf.close_reader(reader)


async def test_high_bitrate_title_keeps_refilling_within_the_cache(provider, settings, make_proxy, monkeypatch):
    # A buffer setting larger than the cache used to cap the title for good:
    # once the buffer drained, every chunk was fetched as the TV waited for it.
    monkeypatch.setattr(vod_module, "KEEP_EDGE_BYTES", 1024 * 1024)
    monkeypatch.setattr(vod_module, "KEEP_BEHIND_SECONDS", 10)
    monkeypatch.setattr(vod_module, "META_SAVE_BYTES", 4 * 1024 * 1024)
    provider.movie_delay = 0.01
    budget = 10 * 1024 * 1024
    client = await make_proxy(settings, vod_cache_bytes=budget, buffer_minutes=30)
    await client.get("/player_api.php?username=u&password=p&action=get_vod_info&vod_id=1")
    resp = await client.get(MOVIE_URL)
    await resp.content.readexactly(64 * 1024)
    vf = client.app[VOD].files["movie_1"]
    await wait_for(lambda: asyncio.sleep(0, vf.size is not None and not vf.downloading))
    item = (await status(client))["vod"][0]
    assert item["window_limited"]
    assert item["window_seconds"] < 30 * 60
    # Play 14 MiB, past what the first fill held.
    for _ in range(14 * 16):
        await resp.content.readexactly(64 * 1024)
        await asyncio.sleep(0.02)

    def refilled():
        full = vf.ahead_bytes >= vod_module.REFILL_AT * vf.window_bytes()
        rest = vf.play_pos + vf.ahead_bytes >= vf.size
        return asyncio.sleep(0, (full or rest) and not vf.downloading)

    await wait_for(refilled)
    assert not vf.capped
    assert client.app[VOD].used_bytes() <= budget
    resp.close()


async def test_only_the_latest_title_downloads(provider, settings, make_proxy):
    provider.movie_delay = 0.02
    client = await make_proxy(settings, vod_keep_seconds=0)
    first = await client.get(MOVIE_URL)
    await first.content.readexactly(1024)
    first.close()
    second = await client.get("/movie/user/pass/2.mp4")
    await second.content.readexactly(1024)
    second.close()
    items = {i["key"]: i for i in (await status(client))["vod"]}
    assert items["movie_1"]["state"] == "stopped"
    assert items["movie_2"]["current"]
    client.app[VOD].housekeeping()
    assert "movie_1" not in client.app[VOD].files
    assert provider.max_open_streams == 1


async def test_latest_device_wins_without_tug_of_war(provider, settings, make_proxy, monkeypatch):
    _peer_header(monkeypatch)
    client = await make_proxy(settings)
    livingroom = await client.get(LIVE_URL, headers={"X-Peer": "livingroom"})
    await livingroom.content.readexactly(188 * 10)

    # Another TV starts a movie and takes the connection.
    bedroom = await client.get(MOVIE_URL, headers={"X-Peer": "bedroom"})
    assert bedroom.status == 200
    await bedroom.content.readexactly(1024)
    viewer = (await status(client))["viewer"]
    assert viewer["owner"] == "bedroom"
    assert "livingroom" in viewer["displaced"]

    # The first TV reconnecting on its own does not take it back...
    assert (await client.get(LIVE_URL, headers={"X-Peer": "livingroom"})).status == 503
    # ...but it may join what is being watched.
    shared = await client.get(MOVIE_URL, headers={"X-Peer": "livingroom", "Range": "bytes=0-9"})
    assert await shared.read() == MOVIE[:10]

    # After a short while the first TV can take over again.
    client.app[CONNECTION]._displaced["livingroom"] = 0
    again = await client.get(LIVE_URL, headers={"X-Peer": "livingroom"})
    assert again.status == 200
    assert (await status(client))["viewer"]["owner"] == "livingroom"
    again.close()
    bedroom.close()
    livingroom.close()
    assert provider.max_open_streams == 1


async def test_buffer_setting_is_saved(provider, settings, make_proxy):
    client = await make_proxy(settings)
    assert (await status(client))["buffer_minutes"] == 30
    resp = await client.post("/_proxy/settings", json={"buffer_minutes": 45})
    assert resp.status == 200
    assert (await client.post("/_proxy/settings", json={"buffer_minutes": 0})).status == 400
    await client.close()
    client = await make_proxy(settings)
    assert (await status(client))["buffer_minutes"] == 45


async def test_hls_live_shows_on_status_page(provider, settings, make_proxy):
    client = await make_proxy(settings)
    await client.get("/player_api.php?username=u&password=p&action=get_live_streams")
    await (await client.get("/live/user/pass/5.m3u8")).text()
    s = await status(client)
    assert s["viewer"]["content"] == "live 5 hls"
    assert s["hls"][0]["name"] == "Kanal 5"


async def test_channel_names_survive_restart(provider, settings, make_proxy):
    client = await make_proxy(settings)
    await client.get("/player_api.php?username=u&password=p&action=get_live_streams")
    await client.close()
    client = await make_proxy(settings)
    tv = await client.get(LIVE_URL)
    await tv.content.readexactly(188)
    assert (await status(client))["live"][0]["name"] == "Kanal 5"
    tv.close()


async def test_catch_up_is_named_after_its_channel(provider, settings, make_proxy):
    client = await make_proxy(settings)
    await client.get("/player_api.php?username=u&password=p&action=get_live_streams")
    await client.get("/timeshift/u/p/60/2026-10-26:15-00/5.ts")
    viewer = (await status(client))["viewer"]
    assert viewer["content"] == "timeshift 5 2026-10-26:15-00"
    assert viewer["name"] == "Kanal 5"
    await client.get("/streaming/timeshift.php?username=u&password=p&stream=5&start=2026-10-26:16-00&duration=60")
    assert (await status(client))["viewer"]["content"] == "timeshift 5 2026-10-26:16-00"


async def test_pictures_on_other_hosts_go_through_the_proxy(provider, settings, make_proxy, aiohttp_server):
    fetched = []

    async def picture(request):
        fetched.append(request.path_qs)
        return web.Response(body=b"PNG", content_type="image/png")

    pictures = web.Application()
    pictures.router.add_get("/{name}", picture)
    picture_host = str((await aiohttp_server(pictures)).make_url("")).rstrip("/")

    async def live_streams(request):
        if request.query.get("action") == "get_live_streams":
            return web.json_response([{"stream_id": 5, "name": "Kanal 5", "stream_icon": f"{picture_host}/logo?id=5"}])
        return await provider.player_api(request)

    async def get_php(request):
        return web.Response(text=f'#EXTM3U\n#EXTINF:-1 tvg-logo="{picture_host}/m3u.png",Kanal 5\n'
                                 f"{request.url.origin()}/live/u/p/5.ts\n")

    async def xmltv(request):
        return web.Response(text=f'<tv><channel id="5"><icon src="{picture_host}/epg?a=1&amp;b=2"/></channel></tv>',
                            content_type="application/xml")

    app = web.Application()
    app.router.add_get("/player_api.php", live_streams)
    app.router.add_get("/get.php", get_php)
    app.router.add_get("/xmltv.php", xmltv)
    app.router.add_get("/live/{user}/{password}/{name}", provider.live)
    origin = str((await aiohttp_server(app)).make_url("")).rstrip("/")
    client = await make_proxy(settings, upstream_url=origin)
    proxy = str(client.make_url("")).rstrip("/")

    streams = await (await client.get("/player_api.php?username=u&password=p&action=get_live_streams")).json()
    playlist = await (await client.get("/get.php?username=u&password=p")).text()
    guide = await (await client.get("/xmltv.php?username=u&password=p")).text()
    logo = streams[0]["stream_icon"]
    m3u_logo = re.search(r'tvg-logo="([^"]+)"', playlist)[1]
    epg_icon = re.search(r'src="([^"]+)"', guide)[1]
    assert picture_host not in playlist + guide + json.dumps(streams)
    assert f"{proxy}/live/u/p/5.ts" in playlist
    assert all(url.startswith(f"{proxy}/_relay/") for url in (logo, m3u_logo, epg_icon))

    # A picture loaded while a channel plays does not take the connection.
    live = await client.get("/live/user/pass/5.ts")
    await live.content.readexactly(188)
    for url in (logo, m3u_logo, epg_icon):
        resp = await client.get(urlsplit(url).path)
        assert (resp.status, await resp.read()) == (200, b"PNG")
    assert fetched == ["/logo?id=5", "/m3u.png", "/epg?a=1&b=2"]
    assert client.app[CONNECTION].content == "live 5"
    live.close()

    # The TV keeps its catalog; the picture URLs in it outlive a restart.
    restarted = await make_proxy(settings, upstream_url=origin)
    assert (await restarted.get(urlsplit(logo).path)).status == 200


async def test_stream_on_another_host_starts(provider, settings, make_proxy, aiohttp_server):
    other = web.Application()
    other.router.add_get("/live/{user}/{password}/{name}", provider.live)
    other_host = str((await aiohttp_server(other)).make_url("")).rstrip("/")

    async def live_streams(request):
        if request.query.get("action") == "get_live_streams":
            return web.json_response(
                [{"stream_id": 7, "name": "Kanal 7", "direct_source": f"{other_host}/live/u/p/7.ts"}]
            )
        return await provider.player_api(request)

    async def get_php(request):
        return web.Response(text=f"#EXTM3U\n#EXTINF:-1,Kanal 8\n{other_host}/live/u/p/8.ts\n")

    app = web.Application()
    app.router.add_get("/player_api.php", live_streams)
    app.router.add_get("/get.php", get_php)
    origin = str((await aiohttp_server(app)).make_url("")).rstrip("/")
    client = await make_proxy(settings, upstream_url=origin)

    streams = await (await client.get("/player_api.php?username=u&password=p&action=get_live_streams")).json()
    playlist = await (await client.get("/get.php?username=u&password=p")).text()
    m3u_stream = next(line for line in playlist.splitlines() if "/_relay/" in line)
    for url in (streams[0]["direct_source"], m3u_stream):
        resp = await client.get(urlsplit(url).path)
        assert resp.status == 200, await resp.text()
        await resp.content.readexactly(188)
        resp.close()


async def test_switching_title_does_not_wait_out_a_retry(provider, settings, make_proxy, aiohttp_server):
    failures = 0

    async def failing_movie(request):
        nonlocal failures
        failures += 1
        raise web.HTTPInternalServerError()

    app = web.Application()
    app.router.add_get("/player_api.php", provider.player_api)
    app.router.add_get("/movie/{user}/{password}/{name}", failing_movie)
    app.router.add_get("/live/{user}/{password}/{name}", provider.live)
    origin = str((await aiohttp_server(app)).make_url("")).rstrip("/")
    client = await make_proxy(settings, upstream_url=origin)
    movie = asyncio.ensure_future(client.get(MOVIE_URL))
    # After the second failure the download waits 4 s before trying again.
    await wait_for(lambda: asyncio.sleep(0, failures >= 2))
    await asyncio.sleep(0.1)
    loop = asyncio.get_running_loop()
    started = loop.time()
    live = await client.get(LIVE_URL)
    await live.content.readexactly(188)
    assert loop.time() - started < 2
    live.close()
    movie.cancel()
