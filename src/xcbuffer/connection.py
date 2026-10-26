"""The single provider connection the subscription allows, and who may use it.

The device that asked last wins: starting something on another TV takes the
connection from the one that was watching. A device asking for what is
already being watched shares it instead. A device that just lost the
connection cannot take it back for a short while, so a player that
reconnects on its own does not pull it away again and start a tug of war.

Every component that streams from the provider holds the connection while
its stream is open, on behalf of the content it was asked for. Switching to
other content revokes that: the holder is told to close, and work still
waiting for the connection on behalf of the old content gives up instead of
connecting later. A new request from the TV (foreground) takes the
connection from whoever has it; background work such as finishing a movie
download only gets it when it is free or held by something idle.
"""

from __future__ import annotations

import time
from typing import Protocol

from .meter import Notifier
from .upstream import UpstreamError

DISPLACED_SECONDS = 30


class Holder(Protocol):
    label: str

    def preempt(self) -> None:
        """Close the provider connection soon and release it."""

    def is_idle(self) -> bool:
        """True when background work may take the connection from this holder."""


class Revoked(UpstreamError):
    def __init__(self, content: str) -> None:
        super().__init__(503, f"{content} is no longer being watched")


class ProviderConnection:
    def __init__(self, displaced_seconds: float = DISPLACED_SECONDS) -> None:
        self.displaced_seconds = displaced_seconds
        self.owner: str | None = None
        # Label of what was asked for last, e.g. "movie 123" or "live 5"
        self.content: str | None = None
        self.since = 0.0
        self._displaced: dict[str, float] = {}
        # Other devices watching the current content with the owner
        self._sharing: set[str] = set()
        self._holder: Holder | None = None
        # The content the holder streams for
        self._holding_for: str | None = None
        self._foreground_waiting = 0
        self._changed = Notifier()

    # --- devices ----------------------------------------------------------------

    def admit(self, peer: str, content: str, *, may_switch: bool = True) -> bool:
        """May peer use the connection for content? Requests that only
        continue earlier content, such as HLS segments, pass may_switch=False
        so a late one cannot switch back to what was left."""
        now = time.time()
        if content == self.content:
            if peer != self.owner:
                self._sharing.add(peer)
            return True
        if not may_switch or self._displaced.get(peer, 0) > now:
            return False
        for other in self._sharing | {self.owner}:
            if other is not None and other != peer:
                self._displaced[other] = now + self.displaced_seconds
        self._sharing.clear()
        self._displaced.pop(peer, None)
        self.owner = peer
        self.content = content
        self.since = now
        if self._holder is not None and self._holding_for != content:
            self._holder.preempt()
        self._changed.notify()
        return True

    def is_current(self, content: str) -> bool:
        return self.content == content

    def snapshot(self) -> dict:
        now = time.time()
        return {
            "owner": self.owner,
            "content": self.content,
            "since": now - self.since if self.owner else None,
            "displaced": {p: until - now for p, until in self._displaced.items() if until > now},
        }

    # --- the connection -------------------------------------------------------------

    @property
    def holder(self) -> Holder | None:
        return self._holder

    async def acquire(self, holder: Holder, content: str, *, foreground: bool) -> None:
        """Wait for the connection on behalf of content; raises Revoked as
        soon as that content is no longer the one being watched."""
        if foreground:
            self._foreground_waiting += 1
        try:
            while True:
                if content != self.content:
                    raise Revoked(content)
                free = self._holder is None and (foreground or not self._foreground_waiting)
                if free or self._holder is holder:
                    self._holder = holder
                    self._holding_for = content
                    return
                if self._holder is not None and (foreground or self._holder.is_idle()):
                    self._holder.preempt()
                await self._changed.wait()
        finally:
            if foreground:
                self._foreground_waiting -= 1
                # Background work may have been waiting for us to go.
                self._changed.notify()

    async def release(self, holder: Holder) -> None:
        if self._holder is holder:
            self._holder = None
            self._holding_for = None
        self._changed.notify()

    async def poke(self) -> None:
        """Wake background waiters, e.g. after a holder became idle."""
        self._changed.notify()
