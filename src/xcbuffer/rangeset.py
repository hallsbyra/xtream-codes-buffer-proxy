"""Sorted set of half-open byte intervals."""

from __future__ import annotations

import bisect


class RangeSet:
    def __init__(self, ranges: list[list[int]] | None = None) -> None:
        self._ranges: list[list[int]] = []
        for start, end in ranges or []:
            self.add(start, end)

    def add(self, start: int, end: int) -> None:
        if end <= start:
            return
        ranges = self._ranges
        i = bisect.bisect_left(ranges, [start, start])
        if i > 0 and ranges[i - 1][1] >= start:
            i -= 1
        j = i
        while j < len(ranges) and ranges[j][0] <= end:
            start = min(start, ranges[j][0])
            end = max(end, ranges[j][1])
            j += 1
        ranges[i:j] = [[start, end]]

    def within(self, start: int, end: int) -> list[tuple[int, int]]:
        """The covered parts of [start, end)."""
        return [(max(a, start), min(b, end)) for a, b in self._ranges if a < end and b > start]

    def remove(self, start: int, end: int) -> list[tuple[int, int]]:
        """Remove [start, end) and return the byte ranges that were covered."""
        removed = []
        kept = []
        for a, b in self._ranges:
            if b <= start or a >= end:
                kept.append([a, b])
                continue
            removed.append((max(a, start), min(b, end)))
            if a < start:
                kept.append([a, start])
            if b > end:
                kept.append([end, b])
        self._ranges = kept
        return removed

    def covered_from(self, pos: int) -> int:
        """Number of contiguous bytes available starting at pos."""
        i = bisect.bisect_right(self._ranges, [pos, float("inf")]) - 1
        if i >= 0 and self._ranges[i][0] <= pos < self._ranges[i][1]:
            return self._ranges[i][1] - pos
        return 0

    def first_missing(self, pos: int, size: int) -> int | None:
        """First position >= pos and < size that is not covered."""
        while pos < size:
            covered = self.covered_from(pos)
            if not covered:
                return pos
            pos += covered
        return None

    def total(self) -> int:
        return sum(end - start for start, end in self._ranges)

    def to_list(self) -> list[list[int]]:
        return [list(r) for r in self._ranges]
