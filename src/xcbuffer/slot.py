"""The single provider connection the subscription allows.

Every component that streams from the provider holds the slot while its
connection is open. A new request from the TV (foreground) takes the slot
from whoever has it; background work such as finishing a movie download only
gets the slot when it is free or held by something idle.
"""

from __future__ import annotations

import asyncio
from typing import Protocol


class Holder(Protocol):
    label: str

    def preempt(self) -> None:
        """Close the provider connection soon and release the slot."""

    def is_idle(self) -> bool:
        """True when background work may take the slot from this holder."""


class UpstreamSlot:
    def __init__(self) -> None:
        self._holder: Holder | None = None
        self._cond = asyncio.Condition()
        self._foreground_waiting = 0

    @property
    def holder(self) -> Holder | None:
        return self._holder

    async def acquire(self, holder: Holder, *, foreground: bool) -> None:
        async with self._cond:
            if self._holder is holder:
                return
            if foreground:
                self._foreground_waiting += 1
                try:
                    while self._holder is not None:
                        self._holder.preempt()
                        await self._cond.wait()
                finally:
                    self._foreground_waiting -= 1
            else:
                while self._holder is not None or self._foreground_waiting:
                    if self._holder is not None and self._holder.is_idle():
                        self._holder.preempt()
                    await self._cond.wait()
            self._holder = holder

    async def release(self, holder: Holder) -> None:
        async with self._cond:
            if self._holder is holder:
                self._holder = None
            self._cond.notify_all()

    async def poke(self) -> None:
        """Wake background waiters, e.g. after a holder became idle."""
        async with self._cond:
            self._cond.notify_all()
