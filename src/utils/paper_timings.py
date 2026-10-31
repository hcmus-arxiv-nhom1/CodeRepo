"""Append-only per-attempt lifecycle events, including discovery and reference readiness."""

from __future__ import annotations

import json
import os
import threading
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path

from src.utils.ids import to_folder_name


class PaperTimings:
    def __init__(self, ids: list[str], data: Path, work: Path, references: bool):
        self.ids, self.data, self.path = ids, data, work / "paper_events.jsonl"
        self.references = references
        self.attempt_id = uuid.uuid4().hex
        self.started = time.perf_counter()
        self.lock = threading.RLock()
        self.remaining: dict[str, set[int]] = {}
        self.source: set[str] = set()
        self.refs: dict[str, str] = {}
        self.completed: set[str] = set()
        if self.path.exists() and self.path.stat().st_size:
            with self.path.open("rb+") as fh:
                fh.seek(-1, os.SEEK_END)
                if fh.read(1) != b"\n":
                    fh.seek(0, os.SEEK_END)
                    fh.write(b"\n")  # separate an interrupted tail from the new attempt
        for arxiv_id in ids:
            self.event(arxiv_id, "start")

    def event(self, arxiv_id: str, event: str, **fields) -> None:
        with self.lock, self.path.open("a", encoding="utf-8") as fh:
            rec = dict(
                attempt_id=self.attempt_id,
                arxiv_id=arxiv_id,
                event=event,
                at=datetime.now(UTC).isoformat(),
                elapsed_s=time.perf_counter() - self.started,
                **fields,
            )
            fh.write(json.dumps(rec) + "\n")
            fh.flush()
            os.fsync(fh.fileno())

    def discovered(self, found: dict, missing: dict, todo: list[tuple[str, int]]) -> None:
        for arxiv_id in self.ids:
            if not found.get(arxiv_id):
                self.event(arxiv_id, "discovery_failed", reason=missing.get(arxiv_id, "missing"))
                continue
            self.event(arxiv_id, "discovered", versions=found[arxiv_id])
            self.remaining[arxiv_id] = {v for i, v in todo if i == arxiv_id}
            if not self.references:
                self.reference({"arxiv_id": arxiv_id, "status": "disabled"})
            else:
                try:
                    record = json.loads(
                        (self.data / to_folder_name(arxiv_id) / "references.json").read_text(
                            encoding="utf-8"
                        )
                    )
                    if record.get("status") in {"ok", "not_found"}:
                        self.reference(record)
                except (OSError, ValueError):
                    pass
            if not self.remaining[arxiv_id]:
                self._source_complete(arxiv_id)

    def version(self, record: dict) -> None:
        with self.lock:
            arxiv_id = record["arxiv_id"]
            self.event(
                arxiv_id,
                "version",
                version=record["version"],
                status=record["status"],
                source_work_s=record.get("total_s", 0),
            )
            if record["status"] in {"ok", "no_tex", "unavailable"}:
                self.remaining[arxiv_id].discard(record["version"])
                if not self.remaining[arxiv_id]:
                    self._source_complete(arxiv_id)

    def _source_complete(self, arxiv_id: str) -> None:
        self.source.add(arxiv_id)
        self.event(arxiv_id, "source_complete")
        self._complete(arxiv_id)

    def reference(self, record: dict) -> None:
        with self.lock:
            arxiv_id = record["arxiv_id"]
            self.refs[arxiv_id] = record["status"]
            self.event(arxiv_id, "references_complete", status=record["status"])
            self._complete(arxiv_id)

    def _complete(self, arxiv_id: str) -> None:
        if arxiv_id in self.source and arxiv_id in self.refs and arxiv_id not in self.completed:
            self.completed.add(arxiv_id)
            self.event(arxiv_id, "complete", references_status=self.refs[arxiv_id])
