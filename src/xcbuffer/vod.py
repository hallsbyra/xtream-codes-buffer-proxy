"""Movie and episode cache.

Each title is downloaded to one sparse file on disk. The downloader keeps a
window of buffer minutes ahead of where the TV plays, also while the TV is
paused. What has been played is punched out of the file again, so a long 4K
film fits in a few gigabytes. Only the title watched last downloads; the
others keep what they have for a while in case the viewer goes back.

Readers are served from the file and wait for bytes that have not arrived;
when a reader seeks outside what is being fetched, the downloader moves there.
"""

from __future__ import annotations

import asyncio
import ctypes
import ctypes.util
import json
import logging
import os
import re
import shutil
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import aiohttp

from .config import Settings
from .meter import Notifier, RateMeter
from .rangeset import RangeSet
from .slot import UpstreamSlot
from .tunables import Tunables
from .upstream import CHUNK, Upstream, UpstreamError, VpnDown

log = logging.getLogger(__name__)

# A reader just ahead of the download position waits instead of moving it.
JUMP_SLACK = 8 * 1024 * 1024
META_SAVE_BYTES = 32 * 1024 * 1024
MAX_FAILURES = 6
# Below this much free disk nothing is downloaded, not even what is playing.
CRITICAL_FREE_BYTES = 512 * 1024 * 1024
# Statuses where retrying the same request will not help.
FATAL_STATUSES = {400, 401, 404, 410}
# Container headers and indexes live at the start or end of the file; keep them.
KEEP_EDGE_BYTES = 16 * 1024 * 1024
# Keep this much playback behind the viewer for short rewinds.
KEEP_BEHIND_SECONDS = 5 * 60
# A full buffer starts downloading again when it has drained to this share.
REFILL_AT = 0.8
# Byte rate assumed when the title's duration is unknown (about 20 Mbit/s).
FALLBACK_BYTE_RATE = 2.5 * 1024 * 1024
# Above UHD Blu-ray rates; a duration giving more than this is not seconds.
MAX_BYTE_RATE = 100_000_000 / 8


def plausible_seconds(duration: float | None, size: int | None) -> float | None:
    """Some providers give the duration in minutes where seconds belong."""
    if duration and size and size / duration > MAX_BYTE_RATE and size / (duration * 60) <= MAX_BYTE_RATE:
        return duration * 60
    return duration


class NotCacheable(Exception):
    """The provider did not give a file size; relay the stream instead."""


@dataclass(eq=False)
class Reader:
    peer: str
    pos: int
    started: float = field(default_factory=time.time)
    waiting: bool = False


_libc = ctypes.CDLL(ctypes.util.find_library("c"), use_errno=True)
_libc.fallocate.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_longlong, ctypes.c_longlong]
_FALLOC_FL_KEEP_SIZE = 0x01
_FALLOC_FL_PUNCH_HOLE = 0x02


def punch_hole(fd: int, offset: int, length: int) -> None:
    """Give the disk blocks of a byte range back without changing file size."""
    if _libc.fallocate(fd, _FALLOC_FL_KEEP_SIZE | _FALLOC_FL_PUNCH_HOLE, offset, length) != 0:
        err = ctypes.get_errno()
        raise OSError(err, os.strerror(err))


class VodFile:
    def __init__(self, cache: VodCache, kind: str, stream_id: str, ext: str) -> None:
        self.cache = cache
        self.kind = kind
        self.stream_id = stream_id
        self.ext = ext
        self.key = f"{kind}_{stream_id}"
        self.path = cache.dir / f"{self.key}.{ext}"
        self.meta_path = cache.dir / f"{self.key}.json"
        self.size: int | None = None
        # As the provider gave it; see the duration property
        self._duration: float | None = None
        self.content_type = "application/octet-stream"
        self.ranges = RangeSet()
        self.supports_range = True
        self.last_read = time.time()
        # Where the TV last read; the buffer window is measured from here.
        self.play_pos = 0
        self.readers: set[Reader] = set()
        self.state = "idle"
        self.error: UpstreamError | None = None
        self.last_error: str | None = None
        self.capped = False
        self.uncacheable = False
        self.write_pos: int | None = None
        self.rate = RateMeter()
        # Provider path including credentials; kept in memory only.
        self._upstream_path: str | None = None
        self._headers: dict[str, str] = {}
        self._jump_to: int | None = None
        self._want_foreground = False
        self._preempted = False
        self._full = False
        self._task: asyncio.Task | None = None
        self._fetch_task: asyncio.Task | None = None
        self._wake = asyncio.Event()
        self._ready = asyncio.Event()
        self._changed = Notifier()
        self._fd: int | None = None
        self._unsaved = 0
        self._ready_waiters = 0
        self._deleted = False

    # --- slot holder protocol -------------------------------------------

    @property
    def label(self) -> str:
        return f"{self.kind} {self.stream_id}"

    def preempt(self) -> None:
        self._preempted = True
        self._want_foreground = False
        if self._fetch_task:
            self._fetch_task.cancel()

    def is_idle(self) -> bool:
        return False

    # --- state ------------------------------------------------------------

    @property
    def complete(self) -> bool:
        return self.size is not None and self.ranges.first_missing(0, self.size) is None

    @property
    def downloaded(self) -> int:
        return self.ranges.total()

    @property
    def downloading(self) -> bool:
        return self.write_pos is not None

    @property
    def duration(self) -> float | None:
        return plausible_seconds(self._duration, self.size)

    @duration.setter
    def duration(self, value: float | None) -> None:
        self._duration = value

    @property
    def byte_rate(self) -> float:
        """Bytes per second of playback."""
        if self.size and self.duration:
            return self.size / self.duration
        return FALLBACK_BYTE_RATE

    @property
    def ahead_bytes(self) -> int:
        return self.ranges.covered_from(self.play_pos)

    def window_bytes(self) -> int:
        return int(self.cache.tunables.buffer_seconds * self.byte_rate)

    @property
    def current(self) -> bool:
        return self.cache.is_current(self.label)

    def _blocked(self) -> bool:
        return any(r.waiting for r in self.readers)

    def _window_open(self) -> bool:
        """Below the buffer window; refills only after draining a bit, so a
        full buffer does not reconnect for every chunk the TV reads."""
        if self.size is not None and self.play_pos + self.ahead_bytes >= self.size:
            return False
        limit = self.window_bytes() * (REFILL_AT if self._full else 1)
        self._full = self.ahead_bytes >= limit
        return not self._full

    def _wants_data(self) -> bool:
        if self.error or self.uncacheable or self._deleted:
            return False
        if self.size is None:
            return bool(self.readers) or self._want_foreground or self._ready_waiters > 0
        if self.complete or self.cache.disk_free() < CRITICAL_FREE_BYTES:
            return False
        if self._blocked() or self._want_foreground:
            return True
        if not self.current or self.capped or self.cache.disk_low():
            return False
        return self._window_open()

    def _idle_state(self) -> str:
        if self.complete:
            return "complete"
        if self.error:
            return "error"
        if not self.current:
            return "stopped"
        if self._full or (self.size is not None and self.play_pos + self.ahead_bytes >= self.size):
            return "full"
        return "paused"

    # --- entry points used by request handlers ------------------------------

    def request(self, upstream_path: str, headers: dict[str, str]) -> None:
        """A new request from the TV: take the provider connection if needed."""
        self._upstream_path = upstream_path
        self._headers = headers
        self.error = None
        self.last_read = time.time()
        if not self.complete and self.cache.slot.holder is not self:
            self._want_foreground = True
        self._kick()

    async def wait_ready(self, timeout: float = 90) -> None:
        if self.size is not None:
            return
        self._ready.clear()
        self._wake.set()
        self._ready_waiters += 1
        try:
            await asyncio.wait_for(self._ready.wait(), timeout)
        except TimeoutError:
            raise UpstreamError(504, "provider did not answer in time") from None
        finally:
            self._ready_waiters -= 1
        if self.uncacheable:
            raise NotCacheable()
        if self.size is None:
            raise self.error or UpstreamError(502)

    def open_reader(self, peer: str, pos: int) -> Reader:
        reader = Reader(peer, pos)
        self.readers.add(reader)
        self.play_pos = pos
        return reader

    def close_reader(self, reader: Reader) -> None:
        self.readers.discard(reader)
        self.last_read = time.time()
        self._kick()

    async def wait_data(self, reader: Reader) -> int:
        """Bytes available at reader.pos, waiting for the download if needed.
        Returns 0 at end of file."""
        assert self.size is not None
        requested = False
        while True:
            available = self.ranges.covered_from(reader.pos)
            if available or reader.pos >= self.size:
                reader.waiting = False
                return available
            if self.error:
                reader.waiting = False
                raise self.error
            if self._deleted:
                reader.waiting = False
                raise UpstreamError(410, "title was removed from the cache")
            if not requested:
                requested = True
                reader.waiting = True
                self._seek_download(reader.pos)
            await self._changed.wait(5)

    def read(self, reader: Reader, n: int) -> bytes:
        self.last_read = time.time()
        data = os.pread(self._open(), n, reader.pos)
        reader.pos += len(data)
        self.play_pos = reader.pos
        if not self.downloading and self._wants_data():
            self._kick()
        return data

    # --- downloader -----------------------------------------------------------

    def _kick(self) -> None:
        if self._deleted:
            return
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._run(), name=f"vod {self.key}")
        self._wake.set()

    def _seek_download(self, pos: int) -> None:
        if self.write_pos is not None and self.write_pos <= pos <= self.write_pos + JUMP_SLACK:
            return
        self._jump_to = pos
        if self._fetch_task and self.write_pos is not None:
            self._fetch_task.cancel()
        self._kick()

    def _next_target(self) -> int | None:
        if self.size is None or not self.supports_range:
            return 0
        if self._jump_to is not None:
            jump, self._jump_to = self._jump_to, None
            target = self.ranges.first_missing(jump, self.size)
            if target is not None:
                return target
        return self.ranges.first_missing(self.play_pos, self.size)

    async def _run(self) -> None:
        slot = self.cache.slot
        while True:
            if not self._wants_data():
                self.state = self._idle_state()
                self._wake.clear()
                try:
                    await asyncio.wait_for(self._wake.wait(), 60)
                except TimeoutError:
                    if not self._wants_data():
                        self.state = self._idle_state()
                        return
                continue
            self.state = "starting"
            await slot.acquire(self, foreground=self._want_foreground)
            self._want_foreground = False
            self._preempted = False
            try:
                await self._download_while_wanted()
            finally:
                self.write_pos = None
                try:
                    self._save_meta()
                except OSError as exc:
                    log.warning("%s: could not save metadata (%s)", self.label, exc)
                finally:
                    await slot.release(self)

    async def _download_while_wanted(self) -> None:
        failures = 0
        while self._wants_data() and not self._preempted:
            target = self._next_target()
            if target is None:
                return
            self._fetch_task = asyncio.create_task(self._fetch(target))
            try:
                await self._fetch_task
                failures = 0
            except asyncio.CancelledError:
                if asyncio.current_task().cancelling():
                    raise
            except VpnDown:
                self.state = "vpn_down"
                await self.cache.upstream.wait_allowed()
            except (aiohttp.ClientError, TimeoutError, OSError, UpstreamError) as exc:
                failures += 1
                self.last_error = str(exc) or type(exc).__name__
                log.warning("%s: download failed (%s), attempt %d", self.label, self.last_error, failures)
                status = exc.status if isinstance(exc, UpstreamError) else 502
                if status in FATAL_STATUSES or failures >= MAX_FAILURES:
                    self.error = exc if isinstance(exc, UpstreamError) else UpstreamError(502, self.last_error)
                    self._ready.set()
                    self._changed.notify()
                    return
                self.state = "retrying"
                await asyncio.sleep(min(2**failures, 20))
            finally:
                self._fetch_task = None

    async def _fetch(self, start: int) -> None:
        assert self._upstream_path is not None
        upstream = self.cache.upstream
        headers = dict(self._headers, Range=f"bytes={start}-")
        async with await upstream.get(upstream.url(self._upstream_path), headers) as resp:
            if resp.status == 416 and self.size is not None:
                self.ranges.add(start, self.size)
                return
            if resp.status not in (200, 206):
                raise UpstreamError(resp.status)
            offset, total = _parse_content_range(resp)
            if resp.status == 200 and start > 0:
                self.supports_range = False
            if self.size is None:
                if not total:
                    self.uncacheable = True
                    self._ready.set()
                    return
                self._init_file(total, resp.headers.get("Content-Type"))
            elif total and total != self.size:
                raise UpstreamError(502, f"size changed from {self.size} to {total}")
            pos = offset
            self.write_pos = pos
            self.state = "downloading"
            log.info("%s: downloading from %d", self.label, pos)
            async for chunk in resp.content.iter_chunked(CHUNK):
                os.pwrite(self._open(), chunk, pos)
                self.ranges.add(pos, pos + len(chunk))
                pos += len(chunk)
                self.write_pos = pos
                self.rate.add(len(chunk))
                self._changed.notify()
                self._unsaved += len(chunk)
                if self._unsaved >= META_SAVE_BYTES:
                    self.trim_played()
                    self._save_meta()
                    self.cache.enforce_budget(self)
                if not self._wants_data() or self._jump_to is not None:
                    return
                # Ran into bytes we already have; continue at the next gap.
                if self.supports_range and pos < self.size and self.ranges.covered_from(pos):
                    return
            if pos < (self.size or 0) and resp.status == 200 and not self.supports_range:
                raise UpstreamError(502, "connection ended early")

    # --- disk -------------------------------------------------------------------

    def trim_played(self) -> None:
        """Punch out what lies well behind every viewer, keeping the file's
        first and last bytes where containers keep headers and indexes."""
        if self.size is None or not self.supports_range or self._deleted:
            return
        positions = [r.pos for r in self.readers] or [self.play_pos]
        keep_from = min(positions) - int(KEEP_BEHIND_SECONDS * self.byte_rate)
        end = min(keep_from, self.size - KEEP_EDGE_BYTES)
        if end <= KEEP_EDGE_BYTES:
            return
        removed = self.ranges.remove(KEEP_EDGE_BYTES, end)
        for start, stop in removed:
            try:
                punch_hole(self._open(), start, stop - start)
            except OSError as exc:
                log.warning("%s: could not free disk space (%s)", self.label, exc)
                return
        if removed:
            self._save_meta()

    def _init_file(self, size: int, content_type: str | None) -> None:
        self.size = size
        if content_type and "html" not in content_type and "json" not in content_type:
            self.content_type = content_type.split(";")[0]
        else:
            self.content_type = _guess_type(self.ext)
        with open(self.path, "wb") as fh:
            fh.truncate(size)
        self._save_meta()
        self._ready.set()
        self.cache.enforce_budget(self)

    def _open(self) -> int:
        if self._fd is None:
            self._fd = os.open(self.path, os.O_RDWR)
        return self._fd

    def _save_meta(self) -> None:
        self._unsaved = 0
        if self.size is None or self._deleted:
            return
        meta = {
            "kind": self.kind,
            "stream_id": self.stream_id,
            "ext": self.ext,
            "size": self.size,
            "duration": self._duration,
            "content_type": self.content_type,
            "supports_range": self.supports_range,
            "ranges": self.ranges.to_list(),
            "play_pos": self.play_pos,
            "last_read": self.last_read,
        }
        tmp = self.meta_path.with_suffix(".tmp")
        try:
            tmp.write_text(json.dumps(meta))
            tmp.replace(self.meta_path)
        except OSError as exc:
            log.warning("%s: could not save metadata (%s)", self.label, exc)

    @classmethod
    def load(cls, cache: VodCache, meta_path: Path) -> VodFile | None:
        try:
            meta = json.loads(meta_path.read_text())
            vf = cls(cache, meta["kind"], meta["stream_id"], meta["ext"])
            if not vf.path.exists() or vf.path.stat().st_size != meta["size"]:
                return None
            vf.size = int(meta["size"])
            vf.duration = meta.get("duration")
            vf.content_type = meta["content_type"]
            vf.supports_range = bool(meta.get("supports_range", True))
            vf.ranges = RangeSet(meta["ranges"])
            vf.play_pos = int(meta.get("play_pos", 0))
            vf.last_read = float(meta.get("last_read", time.time()))
            vf.state = "complete" if vf.complete else "stopped"
            return vf
        except (OSError, ValueError, KeyError, TypeError):
            return None

    def delete(self) -> None:
        self._deleted = True
        self._changed.notify()
        if self._task:
            self._task.cancel()
        if self._fd is not None:
            os.close(self._fd)
            self._fd = None
        for path in (self.path, self.meta_path):
            path.unlink(missing_ok=True)


class VodCache:
    def __init__(
        self,
        settings: Settings,
        upstream: Upstream,
        slot: UpstreamSlot,
        tunables: Tunables,
        is_current: Callable[[str], bool],
    ) -> None:
        self.settings = settings
        self.upstream = upstream
        self.slot = slot
        self.tunables = tunables
        self.is_current = is_current
        self.dir = settings.data_dir / "vod"
        self.dir.mkdir(parents=True, exist_ok=True)
        self.files: dict[str, VodFile] = {}
        for meta_path in self.dir.glob("*.json"):
            vf = VodFile.load(self, meta_path)
            if vf:
                self.files[vf.key] = vf
            else:
                meta_path.unlink(missing_ok=True)
        known = {vf.path.name for vf in self.files.values()} | {
            vf.meta_path.name for vf in self.files.values()
        }
        for path in self.dir.iterdir():
            if path.name not in known:
                path.unlink(missing_ok=True)

    def get(self, kind: str, stream_id: str, ext: str) -> VodFile:
        key = f"{kind}_{stream_id}"
        vf = self.files.get(key)
        if vf is None or vf.ext != ext:
            if vf is not None:
                self.remove(vf)
            vf = VodFile(self, kind, stream_id, ext)
            self.files[key] = vf
        return vf

    def remove(self, vf: VodFile) -> None:
        vf.delete()
        self.files.pop(vf.key, None)

    def used_bytes(self) -> int:
        return sum(vf.downloaded for vf in self.files.values())

    def disk_free(self) -> int:
        try:
            return shutil.disk_usage(self.dir).free
        except OSError:
            return 0

    def disk_low(self) -> bool:
        return self.disk_free() < self.settings.min_free_bytes

    def enforce_budget(self, active: VodFile) -> None:
        """Evict least recently read titles until the cache fits its budget."""
        budget = self.settings.vod_cache_bytes
        candidates = sorted(
            (vf for vf in self.files.values() if vf is not active and not vf.readers and not vf.downloading),
            key=lambda vf: vf.last_read,
        )
        while self.used_bytes() > budget and candidates:
            victim = candidates.pop(0)
            log.info("evicting %s to stay within the cache budget", victim.label)
            self.remove(victim)
        active.capped = self.used_bytes() > budget

    def wake_current(self) -> None:
        """Let the title being watched refill, e.g. after the buffer setting grew."""
        for vf in self.files.values():
            if vf.current:
                vf._kick()

    def housekeeping(self) -> None:
        cutoff = time.time() - self.settings.vod_keep_seconds
        for vf in list(self.files.values()):
            if vf.readers or vf.downloading:
                continue
            if not vf.current and vf.last_read < cutoff:
                log.info("removing %s, not watched for a while", vf.label)
                self.remove(vf)
            else:
                vf.trim_played()

    async def run_housekeeping(self) -> None:
        while True:
            await asyncio.sleep(60)
            self.housekeeping()

    def shutdown(self) -> None:
        for vf in self.files.values():
            if vf._task:
                vf._task.cancel()
            vf._save_meta()


def _parse_content_range(resp: aiohttp.ClientResponse) -> tuple[int, int | None]:
    """(offset of the first byte in the body, total size or None)."""
    if resp.status == 206:
        match = re.match(r"bytes (\d+)-\d+/(\d+|\*)", resp.headers.get("Content-Range", ""))
        if not match:
            raise UpstreamError(502, "bad Content-Range from provider")
        total = None if match.group(2) == "*" else int(match.group(2))
        return int(match.group(1)), total
    return 0, resp.content_length


def _guess_type(ext: str) -> str:
    return {
        "mp4": "video/mp4",
        "m4v": "video/mp4",
        "mkv": "video/x-matroska",
        "avi": "video/x-msvideo",
        "ts": "video/mp2t",
        "mov": "video/quicktime",
        "webm": "video/webm",
    }.get(ext.lower(), "application/octet-stream")
