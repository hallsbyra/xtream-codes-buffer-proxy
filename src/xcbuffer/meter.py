"""Small helpers shared by the VOD and live buffers."""

from __future__ import annotations

import asyncio
import time
from collections import deque


class RateMeter:
    """Bytes per second over a sliding window."""

    def __init__(self, window: float = 10.0) -> None:
        self.window = window
        self._samples: deque[tuple[float, int]] = deque()

    def add(self, n: int) -> None:
        now = time.monotonic()
        self._samples.append((now, n))
        self._trim(now)

    def rate(self) -> float:
        now = time.monotonic()
        self._trim(now)
        return sum(n for _, n in self._samples) / self.window

    def _trim(self, now: float) -> None:
        while self._samples and now - self._samples[0][0] > self.window:
            self._samples.popleft()


class Notifier:
    """Wake every waiter on each change, without losing wakeups."""

    def __init__(self) -> None:
        self._event = asyncio.Event()

    def notify(self) -> None:
        self._event.set()
        self._event = asyncio.Event()

    async def wait(self, timeout: float | None = None) -> None:
        event = self._event
        try:
            await asyncio.wait_for(event.wait(), timeout)
        except TimeoutError:
            pass
