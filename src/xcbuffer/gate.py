"""Who may use the single provider connection.

The device that asked last wins: starting something on another TV takes the
connection from the one that was watching. A device asking for what is
already being watched shares it instead. A device that just lost the
connection cannot take it back for a short while, so a player that
reconnects on its own does not pull it away again and start a tug of war.
"""

from __future__ import annotations

import time
from collections.abc import Callable

DISPLACED_SECONDS = 30


class ViewerGate:
    def __init__(self, displaced_seconds: float = DISPLACED_SECONDS) -> None:
        self.displaced_seconds = displaced_seconds
        self.owner: str | None = None
        # Label of what was asked for last, e.g. "movie 123" or "live 5"
        self.content: str | None = None
        self.since = 0.0
        self._displaced: dict[str, float] = {}
        # Called with the new content when it changes
        self.on_switch: Callable[[str], None] | None = None

    def admit(self, peer: str, content: str) -> bool:
        now = time.time()
        if content == self.content:
            return True
        if self._displaced.get(peer, 0) > now:
            return False
        if self.owner is not None and self.owner != peer:
            self._displaced[self.owner] = now + self.displaced_seconds
        self._displaced.pop(peer, None)
        self.owner = peer
        self.content = content
        self.since = now
        if self.on_switch:
            self.on_switch(content)
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
