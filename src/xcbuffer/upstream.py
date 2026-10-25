"""HTTP client for the provider."""

from __future__ import annotations

import aiohttp

from .config import Settings
from .vpn import VpnMonitor

# Players often ask for less than this per read; bigger chunks mean fewer
# disk writes and wakeups.
CHUNK = 256 * 1024

STREAM_TIMEOUT = aiohttp.ClientTimeout(total=None, sock_connect=15, sock_read=30)
API_TIMEOUT = aiohttp.ClientTimeout(total=120, sock_connect=15, sock_read=60)


class UpstreamError(Exception):
    def __init__(self, status: int, message: str = "") -> None:
        super().__init__(message or f"provider answered {status}")
        self.status = status


class VpnDown(UpstreamError):
    def __init__(self) -> None:
        super().__init__(503, "VPN is down; provider traffic is blocked")


class Upstream:
    def __init__(self, settings: Settings, vpn: VpnMonitor | None = None) -> None:
        self.settings = settings
        self.vpn = vpn
        self.session = aiohttp.ClientSession(
            connector=aiohttp.TCPConnector(limit=0, force_close=True),
        )

    def url(self, path_qs: str) -> str:
        return self.settings.upstream_url + path_qs

    @property
    def allowed(self) -> bool:
        return self.vpn is None or self.vpn.up

    async def wait_allowed(self) -> None:
        if self.vpn is not None:
            await self.vpn.wait_up()

    async def get(
        self,
        url: str,
        headers: dict[str, str],
        *,
        timeout: aiohttp.ClientTimeout = STREAM_TIMEOUT,
    ) -> aiohttp.ClientResponse:
        if not self.allowed:
            raise VpnDown()
        return await self.session.get(
            url,
            headers=headers,
            allow_redirects=True,
            timeout=timeout,
            proxy=self.settings.upstream_proxy,
        )

    async def close(self) -> None:
        await self.session.close()


def forward_headers(request_headers, extra: dict[str, str] | None = None) -> dict[str, str]:
    """Headers worth passing to the provider; some providers check User-Agent."""
    headers = {}
    for name in ("User-Agent", "Accept", "Accept-Language"):
        if name in request_headers:
            headers[name] = request_headers[name]
    headers["Accept-Encoding"] = "identity"
    headers.update(extra or {})
    return headers
