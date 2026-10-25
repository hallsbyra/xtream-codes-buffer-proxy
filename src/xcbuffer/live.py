"""Live channel buffer.

One provider connection per channel is written to disk in segments. Every
viewer has its own read position, so a paused TV simply falls behind live
while the buffer keeps filling. When the TV reconnects (some players do on
pause) it continues where it left off.
"""

from __future__ import annotations

import asyncio
import bisect
import itertools
import logging
import os
import shutil
import time
from collections import deque
from dataclasses import dataclass, field

import aiohttp

from .config import Settings
from .meter import Notifier, RateMeter
from .slot import UpstreamSlot
from .tunables import Tunables
from .upstream import CHUNK, Upstream, UpstreamError, VpnDown

log = logging.getLogger(__name__)

SEGMENT = 32 * 1024 * 1024
TS_PACKET = 188
MAX_FAILURES = 8
FATAL_STATUSES = {400, 401, 404, 410}
# A channel without viewers gives way to background downloads after this long.
IDLE_YIELD_SECONDS = 120
# Ignore PCR jumps bigger than this; they are discontinuities, not playback time.
MAX_PCR_STEP = 5.0


class TsClock:
    """Playback time of an MPEG-TS stream, from its PCR timestamps."""

    def __init__(self) -> None:
        self.seconds = 0.0
        self._pid: int | None = None
        self._last: float | None = None
        self._pending = b""

    def feed(self, data: bytes) -> None:
        buf = self._pending + data
        i = 0
        n = len(buf)
        while i + TS_PACKET <= n:
            if buf[i] != 0x47:
                i += 1
                continue
            self._packet(buf, i)
            i += TS_PACKET
        self._pending = buf[i:][-TS_PACKET:]

    def _packet(self, p: bytes, i: int) -> None:
        if not (p[i + 3] & 0x20) or p[i + 4] < 7 or not (p[i + 5] & 0x10):
            return
        pid = ((p[i + 1] & 0x1F) << 8) | p[i + 2]
        if self._pid is None:
            self._pid = pid
        elif pid != self._pid:
            return
        base = (p[i + 6] << 25) | (p[i + 7] << 17) | (p[i + 8] << 9) | (p[i + 9] << 1) | (p[i + 10] >> 7)
        now = base / 90000
        if self._last is not None:
            step = now - self._last
            if 0 < step < MAX_PCR_STEP:
                self.seconds += step
        self._last = now

    def reset(self) -> None:
        """Forget the PCR position after a reconnect."""
        self._last = None
        self._pid = None
        self._pending = b""


@dataclass(eq=False)
class LiveReader:
    peer: str
    pos: int
    started: float = field(default_factory=time.time)
    waiting: bool = False
    jump_to_live: bool = False


class LiveChannel:
    _ids = itertools.count(1)

    def __init__(self, manager: LiveManager, stream_id: str) -> None:
        self.manager = manager
        self.stream_id = stream_id
        self.dir = manager.dir / f"{stream_id}-{next(self._ids)}"
        self.dir.mkdir(parents=True)
        self.start = 0
        self.head = 0
        self.readers: set[LiveReader] = set()
        self.state = "connecting"
        self.error: UpstreamError | None = None
        self.last_error: str | None = None
        self.stopped = False
        self.started_at = time.time()
        self.idle_since: float | None = None
        self.rate = RateMeter()
        self.clock = TsClock()
        # (wall time, byte offset, playback seconds), about one per second
        self.samples: deque[tuple[float, int, float]] = deque()
        self._segments: dict[int, int] = {}
        self._changed = Notifier()
        self._upstream_path: str | None = None
        self._headers: dict[str, str] = {}
        self._task: asyncio.Task | None = None

    # --- slot holder protocol -------------------------------------------------

    @property
    def label(self) -> str:
        return f"live {self.stream_id}"

    def preempt(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()

    def is_idle(self) -> bool:
        return self.idle_since is not None and time.time() - self.idle_since > IDLE_YIELD_SECONDS

    # --- lifecycle ----------------------------------------------------------------

    def request(self, upstream_path: str, headers: dict[str, str]) -> None:
        self._upstream_path = upstream_path
        self._headers = headers
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name=self.label)

    def stop(self) -> None:
        self.preempt()

    def check_idle(self) -> None:
        if self.readers or self.idle_since is None:
            return
        if time.time() - self.idle_since > self.manager.settings.live_idle_seconds:
            log.info("%s: no viewers for a while, closing", self.label)
            self.stop()

    async def _run(self) -> None:
        slot = self.manager.slot
        try:
            await slot.acquire(self, foreground=True)
            log.info("%s: connecting to provider", self.label)
            try:
                failures = 0
                while True:
                    try:
                        await self._fetch()
                        failures = 0
                        self.last_error = "provider closed the stream"
                    except VpnDown:
                        self.state = "vpn_down"
                        await self.manager.upstream.wait_allowed()
                        continue
                    except (aiohttp.ClientError, TimeoutError, OSError, UpstreamError) as exc:
                        failures += 1
                        self.last_error = str(exc) or type(exc).__name__
                        log.warning("%s: %s, attempt %d", self.label, self.last_error, failures)
                        status = exc.status if isinstance(exc, UpstreamError) else 502
                        if status in FATAL_STATUSES or failures >= MAX_FAILURES:
                            self.error = exc if isinstance(exc, UpstreamError) else UpstreamError(502, self.last_error)
                            self.state = "error"
                            return
                    self.state = "reconnecting"
                    self.clock.reset()
                    await asyncio.sleep(min(2**failures, 10) if failures else 1)
            finally:
                await slot.release(self)
        finally:
            log.info("%s: stopped after %.0f s", self.label, time.time() - self.started_at)
            self.stopped = True
            if self.state != "error":
                self.state = "stopped"
            self._changed.notify()

    async def _fetch(self) -> None:
        assert self._upstream_path is not None
        upstream = self.manager.upstream
        async with await upstream.get(upstream.url(self._upstream_path), self._headers) as resp:
            if resp.status != 200:
                raise UpstreamError(resp.status)
            self.state = "streaming"
            async for chunk in resp.content.iter_chunked(CHUNK):
                self._append(chunk)

    # --- storage -------------------------------------------------------------------

    def _segment_fd(self, index: int) -> int:
        fd = self._segments.get(index)
        if fd is None:
            fd = os.open(self.dir / f"{index}.seg", os.O_RDWR | os.O_CREAT, 0o644)
            self._segments[index] = fd
        return fd

    def _append(self, data: bytes) -> None:
        self.clock.feed(data)
        view = memoryview(data)
        while view:
            index, offset = divmod(self.head, SEGMENT)
            n = min(len(view), SEGMENT - offset)
            os.pwrite(self._segment_fd(index), view[:n], offset)
            self.head += n
            view = view[n:]
        self.rate.add(len(data))
        now = time.time()
        if not self.samples or now - self.samples[-1][0] >= 1:
            self.samples.append((now, self.head, self.clock.seconds))
        self._trim()
        self._changed.notify()

    def _trim(self) -> None:
        """Drop the oldest segments beyond the buffer window or disk limit;
        never the segment being written."""
        limit = self.manager.settings.live_buffer_bytes
        oldest_kept = time.time() - self.manager.tunables.buffer_seconds
        while self.start // SEGMENT < self.head // SEGMENT:
            index = self.start // SEGMENT
            segment_end = self._sample_at((index + 1) * SEGMENT)
            too_old = segment_end is not None and segment_end[0] < oldest_kept
            if self.head - self.start <= limit and not too_old:
                break
            fd = self._segments.pop(index, None)
            if fd is not None:
                os.close(fd)
            (self.dir / f"{index}.seg").unlink(missing_ok=True)
            self.start = (index + 1) * SEGMENT
        while len(self.samples) > 1 and self.samples[1][1] <= self.start:
            self.samples.popleft()

    def read(self, pos: int, n: int) -> bytes:
        index, offset = divmod(pos, SEGMENT)
        n = min(n, SEGMENT - offset, self.head - pos)
        return os.pread(self._segment_fd(index), n, offset)

    def close_files(self) -> None:
        for fd in self._segments.values():
            os.close(fd)
        self._segments.clear()
        shutil.rmtree(self.dir, ignore_errors=True)

    # --- positions -------------------------------------------------------------------

    def _sample_at(self, pos: int) -> tuple[float, int, float] | None:
        if not self.samples:
            return None
        i = bisect.bisect_right([s[1] for s in self.samples], pos)
        return self.samples[max(i - 1, 0)]

    def live_position(self, delay: float = 0) -> int:
        pos = self.head
        if delay > 0 and self.samples:
            target = time.time() - delay
            for wall, offset, _ in reversed(self.samples):
                pos = offset
                if wall <= target:
                    break
        pos = max(pos, self.start)
        return pos - pos % TS_PACKET

    def behind_seconds(self, pos: int) -> float:
        """Playback seconds between pos and live, PCR based when available."""
        sample = self._sample_at(pos)
        if not sample or not self.samples:
            return 0.0
        last = self.samples[-1]
        if self.clock.seconds > 0:
            return max(last[2] - sample[2], 0.0)
        return max(last[0] - sample[0], 0.0)

    def speed(self, window: float = 30) -> float | None:
        """Playback seconds received per wall-clock second; below 1 means the
        provider is not keeping up."""
        if len(self.samples) < 5 or self.clock.seconds <= 0:
            return None
        last = self.samples[-1]
        first = next((s for s in self.samples if last[0] - s[0] <= window), self.samples[0])
        if last[0] - first[0] < 3:
            return None
        return (last[2] - first[2]) / (last[0] - first[0])

    # --- readers -------------------------------------------------------------------------

    def add_reader(self, reader: LiveReader) -> None:
        self.readers.add(reader)
        self.idle_since = None

    def remove_reader(self, reader: LiveReader) -> None:
        self.readers.discard(reader)
        if not self.readers:
            self.idle_since = time.time()

    async def wait_data(self, reader: LiveReader) -> int:
        """Bytes available at reader.pos; 0 once the channel has stopped."""
        while True:
            if reader.jump_to_live:
                reader.jump_to_live = False
                reader.pos = self.live_position()
            if reader.pos < self.start:
                reader.pos = self.start
            if reader.pos < self.head:
                reader.waiting = False
                return self.head - reader.pos
            if self.stopped:
                return 0
            reader.waiting = True
            await self._changed.wait(5)


class LiveManager:
    def __init__(self, settings: Settings, upstream: Upstream, slot: UpstreamSlot, tunables: Tunables) -> None:
        self.settings = settings
        self.upstream = upstream
        self.slot = slot
        self.tunables = tunables
        self.dir = settings.data_dir / "live"
        shutil.rmtree(self.dir, ignore_errors=True)
        self.dir.mkdir(parents=True)
        self.channels: dict[str, LiveChannel] = {}
        # (peer, stream id) -> (position, time), for players that reconnect on pause
        self._resume: dict[tuple[str, str], tuple[int, float]] = {}

    def get(self, stream_id: str) -> LiveChannel:
        channel = self.channels.get(stream_id)
        if channel is None or channel.stopped:
            if channel is not None:
                self._discard(channel)
            channel = LiveChannel(self, stream_id)
            self.channels[stream_id] = channel
        return channel

    def open_reader(self, channel: LiveChannel, peer: str) -> LiveReader:
        resume = self._resume.pop((peer, channel.stream_id), None)
        if resume and resume[0] >= channel.start:
            pos = resume[0]
        else:
            pos = channel.live_position(self.settings.live_start_delay_seconds)
        reader = LiveReader(peer, pos)
        channel.add_reader(reader)
        return reader

    def close_reader(self, channel: LiveChannel, reader: LiveReader) -> None:
        channel.remove_reader(reader)
        if not channel.stopped:
            self._resume[(reader.peer, channel.stream_id)] = (reader.pos, time.time())

    def _discard(self, channel: LiveChannel) -> None:
        channel.stop()
        channel.close_files()
        if self.channels.get(channel.stream_id) is channel:
            del self.channels[channel.stream_id]
        for key in [k for k in self._resume if k[1] == channel.stream_id]:
            del self._resume[key]

    def housekeeping(self) -> None:
        for channel in list(self.channels.values()):
            channel.check_idle()
            if channel.stopped and not channel.readers:
                self._discard(channel)
        cutoff = time.time() - self.settings.live_idle_seconds
        for key, (_, when) in list(self._resume.items()):
            if when < cutoff:
                del self._resume[key]

    async def run_housekeeping(self) -> None:
        while True:
            await asyncio.sleep(5)
            self.housekeeping()
            await self.slot.poke()

    def shutdown(self) -> None:
        for channel in list(self.channels.values()):
            channel.stop()
