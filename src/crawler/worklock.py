# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""One crawl per work directory: an OS-level lock on ``<work_dir>/run.lock`` (spec 0002).

arXiv allows one connection at a time for all machines under our control, so two
``run_pipeline`` processes on the same work directory (a shift started twice by
mistake) must not both talk to arXiv. The lock belongs to the open file, so the OS
releases it when the process ends, even after ``kill -9`` or a lost Colab session:
there is never a stale lock to delete by hand.

It protects one machine only. Two Colab sessions working on the same Google Drive
folder are not coordinated by this lock. Never run the same run ID concurrently.
Separate runtimes also do not share the arXiv rate limiter.
"""

from __future__ import annotations

import os
from pathlib import Path
from types import TracebackType

if os.name == "nt":
    import msvcrt
else:
    import fcntl


class WorkDirLockedError(RuntimeError):
    """Another process already holds the lock of this work directory."""


class WorkDirLock:
    def __init__(self, work_dir: Path) -> None:
        self.path = Path(work_dir) / "run.lock"
        self._fh = None

    def acquire(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        fh = open(self.path, "a+", encoding="utf-8")  # noqa: SIM115 - held until release()
        try:
            if os.name == "nt":
                fh.seek(0)
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError as exc:
            owner = self._read_owner(fh)
            fh.close()
            raise WorkDirLockedError(
                f"{self.path.parent} is already used by another run ({owner}); "
                "wait for it to finish or stop it first"
            ) from exc
        fh.seek(0)
        fh.truncate()
        fh.write(f"pid {os.getpid()}\n")
        fh.flush()
        self._fh = fh

    @staticmethod
    def _read_owner(fh) -> str:
        try:
            fh.seek(0)
            return fh.read().strip() or "unknown owner"
        except OSError:  # Windows: the locked byte cannot be read by another handle
            return "unknown owner"

    def release(self) -> None:
        if self._fh is None:
            return
        try:
            if os.name == "nt":
                self._fh.seek(0)
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        finally:
            self._fh.close()
            self._fh = None

    def __enter__(self) -> WorkDirLock:
        self.acquire()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.release()
