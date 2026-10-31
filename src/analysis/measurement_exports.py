"""Session resource and per-paper attempt exports; no network or corpus mutation."""

from __future__ import annotations

import json
from pathlib import Path

from src.benchmark.measurements import rows_digest, session_report
from src.utils.profiler import dir_size

PAPER_TIMING_COLUMNS = [
    "arxiv_id",
    "attempt_id",
    "started_at",
    "completed_at",
    "complete",
    "lifecycle_s",
    "source_work_s",
    "references_status",
]


def paper_attempt_rows(work_dirs: list[Path], ids: list[str]) -> list[dict]:
    wanted, events = set(ids), {}
    for work in work_dirs:
        journal = work / "paper_events.jsonl"
        if journal.exists():
            for line in journal.read_text(encoding="utf-8").splitlines():
                try:
                    event = json.loads(line)
                    events[json.dumps(event, sort_keys=True)] = event
                except ValueError:
                    continue
        for path in sorted((work / "measurements").glob("*/papers-*.json")):
            part = json.loads(path.read_text(encoding="utf-8"))
            if rows_digest(part["rows"]) != part["sha256"]:
                raise ValueError(f"Paper telemetry checksum mismatch: {path}")
            for event in part["rows"]:
                events[json.dumps(event, sort_keys=True)] = event
    attempts = {}
    for event in events.values():
        if event["arxiv_id"] not in wanted:
            continue
        key = event["arxiv_id"], event["attempt_id"]
        attempt = attempts.setdefault(key, {})
        attempt.setdefault(event["event"], []).append(event)
    rows = []
    for (arxiv_id, attempt_id), events in attempts.items():
        start = (events.get("start") or [{}])[0]
        end = (events.get("complete") or [{}])[-1]
        complete = bool(start and end)
        rows.append(
            dict(
                arxiv_id=arxiv_id,
                attempt_id=attempt_id,
                started_at=start.get("at", ""),
                completed_at=end.get("at", ""),
                complete=complete,
                lifecycle_s=end["elapsed_s"] - start["elapsed_s"] if complete else None,
                source_work_s=sum(e.get("source_work_s", 0) for e in events.get("version", [])),
                references_status=end.get("references_status", ""),
            )
        )
    return sorted(rows, key=lambda r: (r["arxiv_id"], r["started_at"], r["attempt_id"]))


def session_exports(work_dirs: list[Path], data: Path, n_ids: int) -> tuple[list, list, dict]:
    reports, samples, offset = [], [], 0.0
    for work in dict.fromkeys(w.resolve() for w in work_dirs):
        report, rows = session_report(work.parent)
        if not report["sessions"]:
            continue
        reports.append(report)
        session_offsets = {}
        for row in rows:
            sid = row["session_id"]
            if sid not in session_offsets:
                session_offsets[sid] = offset
                offset += max(r["t_sec"] for r in rows if r["session_id"] == sid)
            samples.append(
                dict(
                    t_sec=session_offsets[sid] + row["t_sec"],
                    session_id=sid,
                    phase=row["phase"],
                    rss_total_mb=row["rss_bytes"] / 1e6,
                    disk_mb=row["disk_bytes"] / 1e6,
                    disk_free_bytes=row["disk_free_bytes"],
                )
            )
    if not reports:
        return [], [], dict(measurement_complete=False, scope="legacy chunk-only")
    complete = all(r["complete"] for r in reports)
    observed = sum(r["observed_wall_lower_bound_s"] for r in reports)
    duration = sum(r["sampled_span_s"] for r in reports)
    mean = sum((r["rss_mean_bytes"] or 0) * r["sampled_span_s"] for r in reports)
    tool = "full-session sampler; runner/notebook baseline + descendants; sampled observations"
    metrics = [
        ("observed_wall_lower_bound", observed, "s"),
        ("ram_max", max(r["rss_max_bytes"] for r in reports) / 1e6, "MB"),
        ("ram_mean", mean / duration / 1e6 if duration else None, "MB"),
        ("disk_max", max(r["disk_max_bytes"] for r in reports) / 1e6, "MB"),
        ("disk_free_min", min(r["disk_free_min_bytes"] for r in reports), "bytes"),
        ("output_size", dir_size(data) / 1e6, "MB"),
    ]
    if complete:
        metrics += [
            ("wall_time_e2e", observed, "s"),
            ("avg_paper_time", observed / n_ids if n_ids else None, "s"),
        ]
    audit = dict(
        measurement_complete=complete,
        scope=tool,
        incomplete_sessions=[s for r in reports for s in r["incomplete_sessions"]],
        observed_wall_lower_bound_s=observed,
        excluded=(
            "environment setup, final evidence download/export, offline gaps; lost tails unknown"
        ),
    )
    return (
        [dict(metric=k, value=v, unit=u, tool=tool) for k, v, u in metrics if v is not None],
        samples,
        audit,
    )
