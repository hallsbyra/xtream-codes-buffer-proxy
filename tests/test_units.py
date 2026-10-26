import asyncio

import pytest

from conftest import ts_packets
from xcbuffer.app import parse_range
from xcbuffer.connection import ProviderConnection, Revoked
from xcbuffer.live import TsClock
from xcbuffer.rangeset import RangeSet
from xcbuffer.vod import plausible_seconds
from xcbuffer.xtream import Catalog, rewrite_auth


def test_rangeset_merges_and_finds_gaps():
    rs = RangeSet()
    rs.add(10, 20)
    rs.add(30, 40)
    rs.add(20, 25)
    rs.add(35, 50)
    assert rs.to_list() == [[10, 25], [30, 50]]
    assert rs.covered_from(12) == 13
    assert rs.covered_from(25) == 0
    assert rs.first_missing(0, 60) == 0
    assert rs.first_missing(10, 60) == 25
    assert rs.first_missing(30, 60) == 50
    assert rs.first_missing(30, 50) is None
    rs.add(0, 100)
    assert rs.to_list() == [[0, 100]]
    assert rs.total() == 100


def test_parse_range():
    assert parse_range(None, 100) is None
    assert parse_range("bytes=0-", 100) == (0, 99)
    assert parse_range("bytes=10-19", 100) == (10, 19)
    assert parse_range("bytes=90-200", 100) == (90, 99)
    assert parse_range("bytes=-10", 100) == (90, 99)
    with pytest.raises(ValueError):
        parse_range("bytes=100-", 100)


def test_ts_clock_counts_playback_time_across_chunks():
    data = ts_packets(0, 2500)  # 10 s
    clock = TsClock()
    for i in range(0, len(data), 1000):
        clock.feed(data[i:i + 1000])
    assert 9.8 < clock.seconds < 10.0


def test_ts_clock_ignores_discontinuities():
    clock = TsClock()
    clock.feed(ts_packets(0, 250))
    clock.feed(ts_packets(5000, 250))
    assert clock.seconds < 2


def test_rewrite_auth_points_server_info_at_proxy():
    data = {"user_info": {}, "server_info": {"url": "provider.example", "port": "8080"}}
    rewrite_auth(data, "proxy.lan", 8099)
    assert data["server_info"]["url"] == "proxy.lan"
    assert data["server_info"]["port"] == "8099"
    assert data["server_info"]["server_protocol"] == "http"


def test_catalog_harvests_names_and_durations():
    catalog = Catalog()
    catalog.harvest("get_vod_info", {"info": {"duration": "01:30:00"}, "movie_data": {"stream_id": 7, "name": "X"}})
    catalog.harvest("get_series_info", {
        "info": {"name": "Serie"},
        "episodes": {"1": [{"id": "99", "title": "Avsnitt 1", "info": {"duration_secs": 1800}}]},
    })
    assert catalog.get("movie", "7").name == "X"
    assert catalog.get("movie", "7").duration == 5400
    assert catalog.get("series", "99").name == "Serie – Avsnitt 1"
    assert catalog.get("series", "99").duration == 1800


def test_duration_in_minutes_is_recognised():
    # The Mask: 1.6 GB, provider says "101" meaning minutes.
    assert plausible_seconds(101, 1_626_645_845) == 6060
    assert plausible_seconds(6060, 1_626_645_845) == 6060
    # A 24 MB, four-minute clip stays in seconds.
    assert plausible_seconds(240, 25_165_824) == 240
    assert plausible_seconds(None, 1_000) is None


def test_catalog_reads_minute_fields():
    catalog = Catalog()
    catalog.harvest("get_vod_info", {"info": {"episode_run_time": "101"}, "movie_data": {"stream_id": 8}})
    assert catalog.get("movie", "8").duration == 6060


class Holder:
    def __init__(self, label: str) -> None:
        self.label = label
        self.preempted = False

    def preempt(self) -> None:
        self.preempted = True

    def is_idle(self) -> bool:
        return False


async def test_switch_revokes_work_waiting_for_the_connection():
    connection = ProviderConnection()
    connection.admit("tv", "movie 1")
    slow = Holder("movie 1")
    await connection.acquire(slow, "movie 1", foreground=True)
    connection.admit("tv", "live 5")
    live = Holder("live 5")
    waiting = asyncio.create_task(connection.acquire(live, "live 5", foreground=True))
    await asyncio.sleep(0)
    connection.admit("tv", "movie 2")
    with pytest.raises(Revoked):
        await asyncio.wait_for(waiting, 1)
    assert slow.preempted and not live.preempted
    await connection.release(slow)
    assert connection.holder is None


async def test_switch_preempts_holder_of_other_content_only():
    connection = ProviderConnection()
    connection.admit("tv-a", "live 5")
    relay = Holder("relay")
    await connection.acquire(relay, "live 5", foreground=True)
    connection.admit("tv-b", "live 5")
    assert not relay.preempted
    connection.admit("tv-b", "movie 1")
    assert relay.preempted
