"""Xtream Codes API helpers: URL rewriting and a catalog harvested from
the API responses the TV already fetches through the proxy."""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

log = logging.getLogger(__name__)

API_PATHS = {"/player_api.php", "/panel_api.php", "/get.php", "/xmltv.php"}


@dataclass
class CatalogEntry:
    name: str | None = None
    duration: float | None = None


@dataclass
class Catalog:
    """Names and durations keyed by (kind, stream id); kind is live, movie or series."""

    entries: dict[tuple[str, str], CatalogEntry] = field(default_factory=dict)
    path: Path | None = None

    def load(self) -> None:
        if not self.path or not self.path.exists():
            return
        try:
            for kind, stream_id, name, duration in json.loads(self.path.read_text()):
                self.entries[(kind, stream_id)] = CatalogEntry(name, duration)
        except (OSError, ValueError, TypeError):
            log.warning("ignoring unreadable %s", self.path)

    def save(self) -> None:
        if not self.path:
            return
        rows = [[k, i, e.name, e.duration] for (k, i), e in self.entries.items()]
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps(rows))
        tmp.replace(self.path)

    def get(self, kind: str, stream_id: str) -> CatalogEntry:
        return self.entries.get((kind, stream_id)) or CatalogEntry()

    def _put(self, kind: str, stream_id: Any, name: Any = None, duration: Any = None) -> None:
        if stream_id in (None, ""):
            return
        entry = self.entries.setdefault((kind, str(stream_id)), CatalogEntry())
        if isinstance(name, str) and name.strip():
            entry.name = name.strip()
        seconds = _seconds(duration)
        if seconds:
            entry.duration = seconds

    def harvest(self, action: str | None, payload: Any) -> None:
        self._harvest(action, payload)
        try:
            self.save()
        except OSError as exc:
            # Names are a convenience; the API answer must still go through.
            log.warning("could not save the catalog (%s)", exc)

    def _harvest(self, action: str | None, payload: Any) -> None:
        if action == "get_live_streams" and isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    self._put("live", item.get("stream_id"), item.get("name"))
        elif action == "get_vod_streams" and isinstance(payload, list):
            for item in payload:
                if isinstance(item, dict):
                    self._put("movie", item.get("stream_id"), item.get("name"))
        elif action == "get_vod_info" and isinstance(payload, dict):
            info = payload.get("info") if isinstance(payload.get("info"), dict) else {}
            movie = payload.get("movie_data") if isinstance(payload.get("movie_data"), dict) else {}
            self._put(
                "movie",
                movie.get("stream_id"),
                movie.get("name") or info.get("name"),
                info.get("duration_secs") or info.get("duration") or _minutes(info.get("episode_run_time")),
            )
        elif action == "get_series_info" and isinstance(payload, dict):
            info = payload.get("info") if isinstance(payload.get("info"), dict) else {}
            series_name = info.get("name") or ""
            episodes = payload.get("episodes")
            seasons = episodes.values() if isinstance(episodes, dict) else episodes or []
            for season in seasons:
                for ep in season if isinstance(season, list) else []:
                    if not isinstance(ep, dict):
                        continue
                    ep_info = ep.get("info") if isinstance(ep.get("info"), dict) else {}
                    title = ep.get("title") or ""
                    if series_name and series_name not in title:
                        title = f"{series_name} – {title}".strip(" –")
                    self._put(
                        "series",
                        ep.get("id"),
                        title,
                        ep_info.get("duration_secs") or ep_info.get("duration") or _minutes(ep_info.get("runtime")),
                    )


def _seconds(value: Any) -> float | None:
    if isinstance(value, (int, float)) and value > 0:
        return float(value)
    if isinstance(value, str):
        value = value.strip()
        if value.isdigit():
            return float(value) or None
        match = re.fullmatch(r"(\d+):(\d{2}):(\d{2})", value)
        if match:
            h, m, s = (int(g) for g in match.groups())
            return float(h * 3600 + m * 60 + s) or None
    return None


def _minutes(value: Any) -> float | None:
    """Fields such as episode_run_time are minutes."""
    minutes = _seconds(value)
    return minutes * 60 if minutes else None


def origin(url: str) -> str:
    parts = urlsplit(url)
    return f"{parts.scheme}://{parts.netloc}"


def rewrite_auth(payload: Any, proxy_host: str, proxy_port: int) -> Any:
    """Point server_info in the login response at the proxy, so the TV builds
    its stream URLs against us."""
    if isinstance(payload, dict) and isinstance(payload.get("server_info"), dict):
        info = payload["server_info"]
        info["url"] = proxy_host
        info["port"] = str(proxy_port)
        info["https_port"] = str(proxy_port)
        info["server_protocol"] = "http"
        info["rtmp_port"] = str(proxy_port)
    return payload


def rewrite_text(text: str, upstream_origin: str, proxy_origin: str) -> str:
    """Replace absolute provider URLs, e.g. in get.php playlists."""
    return text.replace(upstream_origin, proxy_origin)


def parse_json(body: bytes) -> Any:
    try:
        return json.loads(body)
    except ValueError:
        return None
