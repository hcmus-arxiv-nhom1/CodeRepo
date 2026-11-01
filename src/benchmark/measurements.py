"""Full-session resources with independently durable, checksummed sample segments.

The root is the runner/notebook process, including its baseline RSS and descendants.
Disk covers run data/work, telemetry staging and configured snapshot/HF staging/cache,
not packages or remote storage quota.
Peaks are sampled observations. An unfinalized session has only a measured lower bound.
"""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import threading
import time
from datetime import UTC, datetime
from pathlib import Path

import psutil

from src.benchmark.checkpoints import write_json
from src.utils.profiler import dir_size, tree_rss


def rows_digest(rows: list[dict]) -> str:
    return hashlib.sha256(json.dumps(rows, sort_keys=True).encode()).hexdigest()


def summarize_samples(rows: list[dict]) -> dict:
    """Left-hold time integration: each RSS observation holds until the next sample."""
    span = rows[-1]["t_sec"] - rows[0]["t_sec"] if len(rows) > 1 else 0
    integral = sum(
        a["rss_bytes"] * (b["t_sec"] - a["t_sec"]) for a, b in zip(rows, rows[1:], strict=False)
    )
    return dict(
        rss_max_bytes=max((r["rss_bytes"] for r in rows), default=0),
        rss_mean_bytes=integral / span if span else (rows[0]["rss_bytes"] if rows else 0),
        disk_max_bytes=max((r["disk_bytes"] for r in rows), default=0),
        disk_free_min_bytes=min((r["disk_free_bytes"] for r in rows), default=0),
        sampled_span_s=span,
    )


class SessionMeasurements:
    def __init__(
        self,
        stage: Path,
        remote: Path,
        local: Path,
        session_id: str,
        *,
        interval_s: float = 1,
        disk_every_s: float = 30,
        flush_s: float = 30,
        publisher=None,
        extra_disk_paths=(),
    ):
        self.stage, self.remote, self.local = stage, remote / "measurements" / session_id, local
        self.session_id = session_id
        self.publisher = publisher
        self.extra_disk_paths = tuple(Path(p) for p in extra_disk_paths)
        self.interval_s, self.disk_every_s, self.flush_s = interval_s, disk_every_s, flush_s
        self.started = time.monotonic()
        self.phase = "prepare-or-restore"
        self.rows: list[dict] = []
        self.sent, self.sequence = 0, 0
        self.paper_offset, self.paper_sequence = 0, 0
        self.last_disk, self.last_flush = -float("inf"), self.started
        self.disk = self.free = 0
        self.stop = threading.Event()
        self.thread: threading.Thread | None = None
        self.error: Exception | None = None
        self.publish_remote = True
        self.proc = psutil.Process(os.getpid())
        self.stage.mkdir(parents=True, exist_ok=False)
        self.remote.mkdir(parents=True, exist_ok=False)
        self._publish(
            "start.json",
            dict(
                schema=1,
                session_id=session_id,
                started_at=datetime.now(UTC).isoformat(),
                root_pid=self.proc.pid,
                interval_s=interval_s,
                disk_every_s=disk_every_s,
                flush_target_s=flush_s,
                rss_scope="runner/notebook process and all descendants, baseline included",
                disk_scope=(
                    "run data/work, telemetry, configured snapshot and HF staging/cache; "
                    "excludes environment"
                ),
            ),
        )

    def _publish(self, name: str, value: dict) -> None:
        write_json(self.stage / name, value)
        if not self.publish_remote:
            return
        if not self.remote.is_dir():
            raise FileNotFoundError("Telemetry destination disappeared; refusing to recreate it")
        write_json(self.remote / name, value)
        if json.loads((self.remote / name).read_text(encoding="utf-8")) != value:
            raise OSError("Telemetry read-back verification failed")
        if self.publisher is not None:
            self.publisher(self.remote / name, value)

    def sample(self, *, final: bool = False) -> None:
        now = time.monotonic()
        if final or now - self.last_disk >= self.disk_every_s:
            paths = (self.local / "data", self.local / "work", self.stage, *self.extra_disk_paths)
            self.disk = sum(p.stat().st_size if p.is_file() else dir_size(p) for p in paths)
            self.free = shutil.disk_usage(self.stage).free
            self.last_disk = now
        rss, processes = tree_rss(self.proc)
        self.rows.append(
            dict(
                t_sec=time.monotonic() - self.started,
                at=datetime.now(UTC).isoformat(),
                rss_bytes=rss,
                n_procs=processes,
                disk_bytes=self.disk,
                disk_free_bytes=self.free,
                phase=self.phase,
            )
        )
        if now - self.last_flush >= self.flush_s:
            self.flush()

    def flush(self) -> None:
        rows = self.rows[self.sent :]
        if rows:
            self._publish(
                f"samples-{self.sequence:06d}.json",
                dict(sequence=self.sequence, rows=rows, sha256=rows_digest(rows)),
            )
            self.sequence += 1
            self.sent = len(self.rows)
        journal = self.local / "work/paper_events.jsonl"
        if journal.exists():
            with journal.open("rb") as fh:
                fh.seek(self.paper_offset)
                text = fh.read()
            complete = text[: text.rfind(b"\n") + 1]
            if complete:
                papers = []
                for line in complete.splitlines():
                    try:
                        papers.append(json.loads(line))
                    except ValueError:
                        continue  # torn event remains an incomplete attempt
                self._publish(
                    f"papers-{self.paper_sequence:06d}.json",
                    dict(sequence=self.paper_sequence, rows=papers, sha256=rows_digest(papers)),
                )
                self.paper_sequence += 1
                self.paper_offset += len(complete)
        self.last_flush = time.monotonic()

    def start(self) -> None:
        self.sample()
        self.flush()

        def loop():
            while not self.stop.wait(self.interval_s):
                try:
                    self.sample()
                except Exception as exc:
                    self.error = exc
                    return

        self.thread = threading.Thread(target=loop, name="session-measurements", daemon=True)
        self.thread.start()

    def check(self) -> None:
        if self.error:
            raise RuntimeError("Session telemetry failed; measurements incomplete") from self.error

    def finish(self, *, publish_remote: bool = True) -> dict:
        self.stop.set()
        if self.thread is not None:
            self.thread.join()
        failure = self.error
        if not publish_remote and failure is None:
            failure = OSError("Remote finalization disabled after shift/checkpoint failure")
        self.publish_remote = publish_remote and failure is None
        try:
            self.sample(final=True)
            self.flush()
        except Exception as exc:
            failure = exc
            self.publish_remote = False
            self.flush()  # preserve buffered rows even while Drive is unavailable
        summary = dict(
            complete=failure is None,
            wall_s=self.rows[-1]["t_sec"],
            segments=self.sequence,
            paper_segments=self.paper_sequence,
            ended_at=datetime.now(UTC).isoformat(),
            output_bytes=dir_size(self.local / "data/corpus"),
            **summarize_samples(self.rows),
        )
        if failure is not None:
            summary["telemetry_error"] = f"{type(failure).__name__}: {failure}"
        try:
            self._publish("summary.json", summary)
        except Exception as exc:
            summary.update(complete=False, telemetry_error=f"{type(exc).__name__}: {exc}")
            write_json(self.stage / "summary.json", summary)
        target = self.local / "work/measurements" / self.session_id
        shutil.copytree(self.stage, target, dirs_exist_ok=True)
        return summary


def collect_measurements(local: Path, remote: Path) -> None:
    """Overlay telemetry newer than a corpus checkpoint, including incomplete sessions."""
    for folder in (remote / "measurements").glob("*"):
        if not folder.is_dir():
            continue
        target = local / "work/measurements" / folder.name
        target.mkdir(parents=True, exist_ok=True)
        for source in folder.glob("*.json"):
            # Copy even invalid JSON: audit must expose corruption, never hide it.
            destination = target / source.name
            tmp = destination.with_suffix(".tmp")
            shutil.copyfile(source, tmp)
            tmp.replace(destination)


def session_report(local: Path) -> tuple[dict, list[dict]]:
    """Exact completed-session wall or a lower bound when any session lacks evidence."""
    incomplete, samples, sessions = [], [], []
    folders = sorted((local / "work/measurements").glob("*"))
    for folder in folders:
        rows, valid, summary = [], True, {}
        try:
            json.loads((folder / "start.json").read_text(encoding="utf-8"))
            parts = sorted(folder.glob("samples-*.json"))
            for sequence, path in enumerate(parts):
                part = json.loads(path.read_text(encoding="utf-8"))
                if part["sequence"] != sequence or rows_digest(part["rows"]) != part["sha256"]:
                    raise ValueError("Invalid telemetry sequence/checksum")
                if path.name != f"samples-{sequence:06d}.json":
                    raise ValueError("Missing telemetry segment")
                rows.extend(part["rows"])
            if not rows or any(
                b["t_sec"] < a["t_sec"] for a, b in zip(rows, rows[1:], strict=False)
            ):
                raise ValueError("Missing/nonmonotonic samples")
            summary = json.loads((folder / "summary.json").read_text(encoding="utf-8"))
            valid = (
                summary.get("complete") is True
                and summary["segments"] == len(parts)
                and summary["wall_s"] == rows[-1]["t_sec"]
            )
        except (OSError, ValueError, KeyError, TypeError):
            valid = False
        if not valid:
            incomplete.append(folder.name)
        measured = rows[-1]["t_sec"] if rows else 0
        resources = summarize_samples(rows) if rows else summarize_samples([])
        sessions.append(dict(wall_s=measured, **resources))
        samples.extend(dict(session_id=folder.name, **r) for r in rows)
    duration = sum(s["sampled_span_s"] for s in sessions)
    observed = sum(s["wall_s"] for s in sessions)
    return (
        dict(
            complete=bool(folders) and not incomplete,
            sessions=len(folders),
            incomplete_sessions=incomplete,
            wall_s=observed if folders and not incomplete else None,
            observed_wall_lower_bound_s=observed,
            sampled_span_s=duration,
            rss_max_bytes=max((s["rss_max_bytes"] for s in sessions), default=0),
            rss_mean_bytes=(
                sum(s["rss_mean_bytes"] * s["sampled_span_s"] for s in sessions) / duration
                if duration
                else None
            ),
            disk_max_bytes=max((s["disk_max_bytes"] for s in sessions), default=0),
            disk_free_min_bytes=min((s["disk_free_min_bytes"] for s in sessions), default=0),
        ),
        samples,
    )
