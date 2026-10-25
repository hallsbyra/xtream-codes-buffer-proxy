"""Settings changed from the status page, kept in the data directory."""

from __future__ import annotations

import json
import logging
from pathlib import Path

log = logging.getLogger(__name__)

MIN_BUFFER_MINUTES = 1
MAX_BUFFER_MINUTES = 240


class Tunables:
    def __init__(self, path: Path, buffer_minutes: float) -> None:
        self.path = path
        self.buffer_minutes = buffer_minutes
        try:
            saved = json.loads(path.read_text())
            self.buffer_minutes = self._valid(saved["buffer_minutes"])
        except FileNotFoundError:
            pass
        except (OSError, ValueError, KeyError, TypeError):
            log.warning("ignoring unreadable %s", path)

    @property
    def buffer_seconds(self) -> float:
        return self.buffer_minutes * 60

    def set_buffer_minutes(self, value: object) -> None:
        self.buffer_minutes = self._valid(value)
        tmp = self.path.with_suffix(".tmp")
        tmp.write_text(json.dumps({"buffer_minutes": self.buffer_minutes}))
        tmp.replace(self.path)

    @staticmethod
    def _valid(value: object) -> float:
        minutes = float(value)  # type: ignore[arg-type]
        if not MIN_BUFFER_MINUTES <= minutes <= MAX_BUFFER_MINUTES:
            raise ValueError(f"buffer must be {MIN_BUFFER_MINUTES}-{MAX_BUFFER_MINUTES} minutes")
        return minutes
