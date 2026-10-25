"""VPN state from Gluetun's control server.

Gluetun's firewall is the real kill switch. This monitor adds a second gate:
the proxy does not contact the provider unless Gluetun reports the tunnel as
running, and the status page shows the tunnel state and exit IP.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Callable

import aiohttp

log = logging.getLogger(__name__)

POLL_SECONDS = 5
TIMEOUT = aiohttp.ClientTimeout(total=4)


class VpnMonitor:
    def __init__(self, control_url: str) -> None:
        self.control_url = control_url.rstrip("/")
        self.status: str | None = None
        self.public_ip: str | None = None
        self.location: str | None = None
        self.error: str | None = None
        self.checked_at: float | None = None
        self.changed_at = time.time()
        self._session = aiohttp.ClientSession(timeout=TIMEOUT)
        self._up = asyncio.Event()
        # Called when the tunnel goes down, to drop provider connections at once
        self.on_down: Callable[[], None] | None = None

    @property
    def up(self) -> bool:
        return self.status == "running"

    async def wait_up(self) -> None:
        await self._up.wait()

    async def refresh(self) -> None:
        was_up = self.up
        try:
            vpn = await self._get("/v1/vpn/status")
            status = str(vpn.get("status") or "unknown")
            ip = await self._get("/v1/publicip/ip") if status == "running" else {}
            self.error = None
        except (aiohttp.ClientError, TimeoutError, ValueError) as exc:
            status, ip = "unreachable", {}
            self.error = f"Gluetun control server: {exc or type(exc).__name__}"
        self.status = status
        self.public_ip = ip.get("public_ip") or None
        self.location = ", ".join(p for p in (ip.get("city"), ip.get("country")) if p) or None
        self.checked_at = time.time()
        if self.up != was_up:
            self.changed_at = self.checked_at
            log.warning("VPN is %s", "up" if self.up else f"down ({status})")
            if not self.up and self.on_down:
                self.on_down()
        if self.up:
            self._up.set()
        else:
            self._up.clear()

    async def _get(self, path: str) -> dict:
        async with self._session.get(self.control_url + path) as resp:
            resp.raise_for_status()
            data = await resp.json(content_type=None)
            if not isinstance(data, dict):
                raise ValueError(f"unexpected answer from {path}")
            return data

    async def run(self) -> None:
        while True:
            await self.refresh()
            await asyncio.sleep(POLL_SECONDS)

    def snapshot(self) -> dict:
        return {
            "status": self.status,
            "up": self.up,
            "public_ip": self.public_ip,
            "location": self.location,
            "error": self.error,
            "checked_ago": time.time() - self.checked_at if self.checked_at else None,
            "changed_ago": time.time() - self.changed_at,
        }

    async def close(self) -> None:
        await self._session.close()
