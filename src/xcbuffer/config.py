"""Runtime settings, read once from the environment."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

GB = 1024**3


def _float(name: str, default: float) -> float:
    raw = os.environ.get(name, "").strip()
    return float(raw) if raw else default


@dataclass(frozen=True)
class Settings:
    # Provider base URL without credentials, e.g. http://provider.example:8080
    upstream_url: str
    # Optional HTTP proxy for all provider traffic, e.g. a Gluetun HTTP proxy
    upstream_proxy: str | None
    # Gluetun control server; when set, provider traffic needs the VPN to be up
    gluetun_control_url: str | None
    data_dir: Path
    # Downloaded VOD bytes kept on disk before old files are evicted
    vod_cache_bytes: int
    # Stop background downloads when the disk has less free space than this
    min_free_bytes: int
    # Keep a title's downloaded data this long after switching to something else
    vod_keep_seconds: float
    # Default buffer window; the status page can change it
    buffer_minutes: float
    # Disk used per live channel before the oldest data is dropped
    live_buffer_bytes: int
    # Keep a live channel running this long after its last viewer left
    live_idle_seconds: float
    # New viewers of a running live channel start this far behind live
    live_start_delay_seconds: float
    # Keep the provider connection reserved this long after a relayed stream ends
    passthrough_linger_seconds: float

    @property
    def configured(self) -> bool:
        return bool(self.upstream_url)

    @classmethod
    def from_env(cls) -> Settings:
        return cls(
            upstream_url=os.environ.get("UPSTREAM_URL", "").strip().rstrip("/"),
            upstream_proxy=os.environ.get("UPSTREAM_PROXY", "").strip() or None,
            gluetun_control_url=os.environ.get("GLUETUN_CONTROL_URL", "").strip() or None,
            data_dir=Path(os.environ.get("DATA_DIR", "/data")),
            vod_cache_bytes=int(_float("VOD_CACHE_GB", 10) * GB),
            min_free_bytes=int(_float("MIN_FREE_GB", 2) * GB),
            vod_keep_seconds=_float("VOD_KEEP_MINUTES", 30) * 60,
            buffer_minutes=_float("BUFFER_MINUTES", 30),
            live_buffer_bytes=int(_float("LIVE_BUFFER_GB", 3) * GB),
            live_idle_seconds=_float("LIVE_IDLE_MINUTES", 10) * 60,
            live_start_delay_seconds=_float("LIVE_START_DELAY_SECONDS", 0),
            passthrough_linger_seconds=_float("PASSTHROUGH_LINGER_SECONDS", 20),
        )
