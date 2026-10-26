import asyncio
import json
import os

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
