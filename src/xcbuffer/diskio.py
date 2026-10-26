"""Disk writes that either finish or raise."""

from __future__ import annotations

import errno
import os


def pwrite_all(fd: int, data: bytes, offset: int) -> None:
    """os.pwrite may write less than asked, e.g. when the disk fills up."""
    view = memoryview(data)
    while view:
        written = os.pwrite(fd, view, offset)
        if written <= 0:
            raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC))
        view = view[written:]
        offset += written
