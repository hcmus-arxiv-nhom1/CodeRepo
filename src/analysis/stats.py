# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""Export the M1 statistics CSVs read by the book (docs/data_contract.md, section 7).

Owner: Tiến (Member 2). Drafted by Thiên on 27/10/2026 so the book can switch from
fake to real numbers as soon as data exists.

    python -m src.analysis.stats --data-root data/corpus --work-dir work \\
        --out ../arxiv-book/data/m1     # profiles read from work/profile/<chunk>/

Writes into ``--out``:
    papers_stats.csv         one row per paper of the selected IDs
    references_stats.csv     one row per source (arxiv, semantic_scholar)
    benchmark_summary.csv    metric, value, unit, tool (only when a profile exists)
    resource_timeseries.csv  t_sec, rss_total_mb, disk_mb (only when a profile exists)

The IDs are ``--range`` (all 5,000 IDs of an owner), ``--ids``, or by default every ID
that discovery saw in the work directories. Every number is computed from the files
the crawler wrote, so the book can always be rebuilt from the corpus with one command.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
from collections import Counter
from pathlib import Path

from src import config
from src.parser.sample_set import range_owner
from src.run_pipeline import FINAL_STATUSES, load_version_state
from src.utils.ids import iter_id_range, to_folder_name

PAPER_COLUMNS = [
    "arxiv_id",
    "range_owner",
    "submission_date",
    "n_authors",
    "n_versions",
    "n_versions_ok",
    "has_source",
    "size_before_bytes",
    "size_after_bytes",
    "n_refs_total",
    "n_refs_arxiv",
    "n_refs_meta_ok",
    "n_refs_meta_complete",
    "ref_status",
    "process_sec",
]
# "GET https://export.arxiv.org/src/... -> 200 (" (http_client) and
# "S2 POST https://api.semanticscholar.org/... -> 429 (" (references)
RESPONSE_RE = re.compile(r"\b(?:GET|POST) (https?://[^\s/]+)\S* -> (\d{3})\b")
FAILURE_RE = re.compile(r"\b(?:GET|POST) (https?://[^\s/]+)\S* failed \(")
# "backing off 4.0 s before next arXiv request" and "... pausing 900 s, pacing now 5.0 s"
BACKOFF_RE = re.compile(r"\bbacking off ([\d.]+) s\b|\bpausing ([\d.]+) s\b")


def source_of(host_url: str) -> str | None:
    if "semanticscholar.org" in host_url:
        return "semantic_scholar"
    if "arxiv.org" in host_url:
        return "arxiv"
    return None


def load_discovery(work_dirs: list[Path]) -> dict[str, dict]:
    """Latest discovery.jsonl record per ID (found / missing with a reason)."""
    out: dict[str, dict] = {}
    for work_dir in work_dirs:
        path = work_dir / "discovery.jsonl"
        if not path.exists():
            continue
        for line in path.read_text(encoding="utf-8").splitlines():
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue  # torn last line after a hard kill
            out[rec["arxiv_id"]] = rec
    return out


# --------------------------------------------------------------------------- papers


def paper_row(arxiv_id: str, data_root: Path, state: dict) -> dict:
    folder = data_root / to_folder_name(arxiv_id)
    row = dict.fromkeys(PAPER_COLUMNS, 0)
    row.update(
        arxiv_id=arxiv_id,
        range_owner=range_owner(arxiv_id) or "",
        ref_status="missing",
        submission_date="",
    )
    try:
        meta = json.loads((folder / "metadata.json").read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return row  # not discovered: counts as a failed paper
    row["submission_date"] = meta.get("submission_date") or ""
    row["n_authors"] = len(meta.get("authors") or [])
    n = meta.get("num_versions") or 0
    row["n_versions"] = n
    process = 0.0
    for v in range(1, n + 1):
        rec = state.get((arxiv_id, v))
        if not rec:
            continue
        process += rec.get("total_s") or 0.0
        if rec["status"] in FINAL_STATUSES and (folder / f"v{v}").is_dir():
            row["n_versions_ok"] += 1
            row["size_before_bytes"] += rec.get("bytes_before") or 0
            row["size_after_bytes"] += rec.get("bytes_after") or 0
            row["has_source"] |= rec["status"] == "ok"
    row["has_source"] = int(row["has_source"])
    row["process_sec"] = round(process, 3)
    try:
        refs = json.loads((folder / "references.json").read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return row
    row["ref_status"] = refs.get("status", "missing")
    items = refs.get("references") or []
    row["n_refs_total"] = len(items)
    row["n_refs_arxiv"] = sum(1 for r in items if r.get("arxiv_id"))
    row["n_refs_meta_ok"] = sum(1 for r in items if r.get("s2_id"))
    row["n_refs_meta_complete"] = sum(
        bool(r.get("s2_id") and r.get("title") and r.get("authors") and r.get("submission_date"))
        for r in items
    )
    return row


# --------------------------------------------------------------------------- requests


def request_counts(work_dirs: list[Path]) -> dict[str, Counter]:
    """Per source: responses by status code and transport failures, read from run.log."""
    counts: dict[str, Counter] = {"arxiv": Counter(), "semantic_scholar": Counter()}
    for work_dir in work_dirs:
        log = work_dir / "run.log"
        if not log.exists():
            continue
        with open(log, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if m := RESPONSE_RE.search(line):
                    if src := source_of(m.group(1)):
                        counts[src][int(m.group(2))] += 1
                elif m := FAILURE_RE.search(line):
                    if src := source_of(m.group(1)):
                        counts[src]["failed"] += 1
    return counts


def backoff_seconds(work_dirs: list[Path]) -> tuple[float, int] | None:
    """Delay the arXiv client asked for after 429/5xx/timeouts/403 (seconds, number of waits).

    It is part of the wall time (the teaching team counts retries and delays), so the
    benchmark reports it next to the total. ``None`` when there is no run.log.
    """
    total, waits, seen = 0.0, 0, False
    for work_dir in work_dirs:
        log = work_dir / "run.log"
        if not log.exists():
            continue
        seen = True
        with open(log, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                if m := BACKOFF_RE.search(line):
                    total += float(m.group(1) or m.group(2))
                    waits += 1
    return (round(total, 1), waits) if seen else None


def checkpoint_seconds(work_dirs: list[Path]) -> tuple[float, float]:
    """Diagnostic components: periodic (already in wall time), external sync (additional)."""
    events = {}
    for work in work_dirs:
        path = work / "checkpoint_events.jsonl"
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                try:
                    event = json.loads(line)
                    events[event["generation"]] = event
                except (ValueError, KeyError):
                    continue
    return tuple(
        round(
            sum(
                e["seconds"] for e in events.values() if bool(e["included_in_pipeline"]) == included
            ),
            3,
        )
        for included in (True, False)
    )


def references_rows(ids: list[str], rows: list[dict], work_dirs: list[Path]) -> list[dict]:
    discovery = load_discovery(work_dirs)
    counts = request_counts(work_dirs)
    hits = {
        "arxiv": (
            sum(discovery.get(i, {}).get("status") == "found" for i in ids),
            sum(discovery.get(i, {}).get("status") == "missing" for i in ids),
        ),
        "semantic_scholar": (
            sum(r["ref_status"] == "ok" for r in rows),
            sum(r["ref_status"] == "not_found" for r in rows),
        ),
    }
    out = []
    for source, c in counts.items():
        out.append(
            {
                "source": source,
                "n_requests": sum(c.values()),
                "n_hit": hits[source][0],
                "n_miss": hits[source][1],
                "n_http_429": c[429],
                "n_http_5xx": sum(
                    n for code, n in c.items() if isinstance(code, int) and code >= 500
                ),
            }
        )
    return out


# --------------------------------------------------------------------------- benchmark


def parse_time_v(text: str) -> dict[str, float]:
    """Wall time (s) and peak RSS (MB) from the output of ``/usr/bin/time -v``."""
    out = {}
    if m := re.search(r"Elapsed \(wall clock\) time.*?: (?:(\d+):)?(\d+):([\d.]+)", text):
        h, mnt, s = m.groups()
        out["wall_s"] = int(h or 0) * 3600 + int(mnt) * 60 + float(s)
    if m := re.search(r"Maximum resident set size \(kbytes\): (\d+)", text):
        out["rss_max_mb"] = int(m.group(1)) * 1024 / 1_000_000
    return out


def profile_dirs(path: Path) -> list[Path]:
    """``path`` itself when it is one profile, else every chunk profile below it, in run order.

    The official crawl of all 20,000 IDs is the benchmark and runs in chunks across Colab
    sessions; each chunk writes ``work/profile/<chunk>/profile_summary.json``.
    """
    if (path / "profile_summary.json").exists():
        return [path]
    found = [p.parent for p in path.glob("*/profile_summary.json")]

    def started(d: Path) -> str:
        return json.loads((d / "profile_summary.json").read_text(encoding="utf-8"))["started_at"]

    return sorted(found, key=started)


def aggregate_profiles(dirs: list[Path], time_log: Path | None = None) -> tuple[dict, list[dict]]:
    """Whole-run figures over chunks (docs/data_contract.md, section 5) and the time series.

    Wall time is the sum of the chunks, peak RAM and peak disk the maxima, mean RAM the
    duration-weighted mean, output the corpus size after the last chunk.
    """
    agg: dict = {"chunks": len(dirs)}
    summaries = [json.loads((d / "profile_summary.json").read_text(encoding="utf-8")) for d in dirs]
    walls = [s.get("wall_s") or 0.0 for s in summaries]
    agg["wall_s"] = round(sum(walls), 2)
    peaks = [s["rss_max_mb"] for s in summaries if s.get("rss_max_mb") is not None]
    agg["rss_max_mb"] = max(peaks) if peaks else None
    weighted = [
        (s["rss_mean_mb"], w)
        for s, w in zip(summaries, walls, strict=True)
        if s.get("rss_mean_mb") is not None
    ]
    span = sum(w for _, w in weighted)
    agg["rss_mean_mb"] = round(sum(m * w for m, w in weighted) / span, 1) if span else None
    agg["disk_max_mb"] = max((s.get("disk_max_mb") or 0.0 for s in summaries), default=None)
    agg["output_mb"] = summaries[-1].get("output_mb") if summaries else None

    logs = [time_log] if time_log else [d / "time_v.log" for d in dirs]
    parsed = [parse_time_v(p.read_text()) for p in logs if p and p.exists()]
    if parsed:
        agg["time_v_wall_s"] = round(sum(t.get("wall_s", 0.0) for t in parsed), 2)
        agg["time_v_rss_max_mb"] = round(max(t.get("rss_max_mb", 0.0) for t in parsed), 1)

    samples, offset = [], 0.0
    for d, wall in zip(dirs, walls, strict=True):
        with open(d / "profile_samples.csv", encoding="utf-8") as fh:
            for r in csv.DictReader(fh):
                t = round(offset + float(r["t_sec"]), 2)
                samples.append(
                    {"t_sec": t, "rss_total_mb": r["rss_total_mb"], "disk_mb": r["disk_mb"]}
                )
        offset += wall
    return agg, samples


def discovery_seconds(work_dirs: list[Path]) -> float | None:
    """Total discovery time over every run recorded in runs.jsonl (one run per chunk)."""
    total, seen = 0.0, False
    for work_dir in work_dirs:
        for preparation in (work_dir / "snapshot_preparation").glob("*.json"):
            total += json.loads(preparation.read_text(encoding="utf-8"))["preparation_s"]
            seen = True
        path = work_dir / "runs.jsonl"
        if path.exists():
            for line in path.read_text(encoding="utf-8").splitlines():
                try:
                    total += json.loads(line).get("discovery_s") or 0.0
                    seen = True
                except json.JSONDecodeError:
                    continue
        elif (work_dir / "run_summary.json").exists():
            run = json.loads((work_dir / "run_summary.json").read_text(encoding="utf-8"))
            total += run.get("discovery_s") or 0.0
            seen = True
    return round(total, 1) if seen else None


def benchmark_rows(
    agg: dict,
    discovery_s: float | None,
    n_ids: int,
    backoff: tuple[float, int] | None = None,
) -> list[dict]:
    psutil_tool = "psutil, RSS of all processes (src/utils/profiler.py)"
    chunks = f"sum over {agg['chunks']} chunk(s)"
    rows = [
        ("wall_time_e2e", agg.get("wall_s"), "s", f"src/utils/profiler.py, {chunks}"),
        (
            "discovery_time",
            discovery_s,
            "s",
            "time.perf_counter, runs.jsonl + snapshot_preparation",
        ),
        (
            "avg_paper_time",
            round(agg["wall_s"] / n_ids, 3) if agg.get("wall_s") and n_ids else None,
            "s",
            "wall_time_e2e / required IDs",
        ),
        ("ram_max", agg.get("rss_max_mb"), "MB", psutil_tool),
        ("ram_mean", agg.get("rss_mean_mb"), "MB", f"{psutil_tool}, duration-weighted"),
        ("disk_max", agg.get("disk_max_mb"), "MB", "os.walk of data/ and work/"),
        ("output_size", agg.get("output_mb"), "MB", "os.walk of the corpus"),
        (
            "retry_backoff_wait",
            backoff[0] if backoff else None,
            "s",
            f"run.log, {backoff[1]} waits after 429/5xx/timeouts/403" if backoff else "",
        ),
        ("wall_time_time_v", agg.get("time_v_wall_s"), "s", "/usr/bin/time -v"),
        ("ram_max_main_process", agg.get("time_v_rss_max_mb"), "MB", "/usr/bin/time -v"),
    ]
    return [{"metric": m, "value": v, "unit": u, "tool": t} for m, v, u, t in rows if v is not None]


# --------------------------------------------------------------------------- main


def write_csv(path: Path, rows: list[dict], columns: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    columns = columns or list(rows[0])
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def select_ids(args: argparse.Namespace, work_dirs: list[Path]) -> list[str]:
    if args.ids:
        return [i.strip() for i in args.ids.split(",") if i.strip()]
    if args.range:
        start, end = config.RANGES[args.range]
        return list(iter_id_range(start, end, config.MONTH_END))
    return sorted(load_discovery(work_dirs))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Export the M1 statistics CSVs for the book.")
    which = parser.add_mutually_exclusive_group()
    which.add_argument("--range", choices=sorted(config.RANGES))
    which.add_argument("--ids", help="comma-separated IDs")
    parser.add_argument("--data-root", type=Path, default=Path(config.DEFAULT_DATA_ROOT))
    parser.add_argument("--work-dir", type=Path, action="append", help="repeatable")
    parser.add_argument(
        "--profile-dir",
        type=Path,
        help="one profiler output folder, or a folder of chunk profiles (default: <work>/profile)",
    )
    parser.add_argument(
        "--time-log", type=Path, help="/usr/bin/time -v output (default: time_v.log per chunk)"
    )
    parser.add_argument("--out", type=Path, required=True, help="e.g. ../arxiv-book/data/m1")
    args = parser.parse_args(argv)
    work_dirs = args.work_dir or [Path(config.DEFAULT_WORK_DIR)]

    ids = select_ids(args, work_dirs)
    state: dict = {}
    for work_dir in work_dirs:
        state.update(load_version_state(work_dir / "versions.jsonl"))
    rows = [paper_row(i, args.data_root, state) for i in ids]
    write_csv(args.out / "papers_stats.csv", rows, PAPER_COLUMNS)
    write_csv(args.out / "references_stats.csv", references_rows(ids, rows, work_dirs))
    from src.analysis.measurement_exports import (
        PAPER_TIMING_COLUMNS,
        paper_timing_report,
        session_exports,
    )

    paper_times, paper_audit = paper_timing_report(work_dirs, ids)
    write_csv(args.out / "paper_attempt_timings.csv", paper_times, PAPER_TIMING_COLUMNS)
    complete = [r for r in rows if r["n_versions"] > 0 and r["n_versions_ok"] == r["n_versions"]]
    n = len(ids)
    refs = sum(r["n_refs_total"] for r in rows)
    corpus_metrics = {
        "required_ids": (n, "papers"),
        "papers_complete": (len(complete), "papers"),
        "source_completion_rate_pct": (100 * len(complete) / n if n else 0, "% of required IDs"),
        "papers_with_tex": (sum(r["has_source"] for r in rows), "papers"),
        "mean_size_before_bytes": (
            sum(r["size_before_bytes"] for r in rows) / n if n else 0,
            "bytes/required ID",
        ),
        "mean_size_after_bytes": (
            sum(r["size_after_bytes"] for r in rows) / n if n else 0,
            "bytes/required ID",
        ),
        "mean_references": (refs / n if n else 0, "references/required ID"),
        "reference_list_success_pct": (
            100 * sum(r["ref_status"] == "ok" for r in rows) / n if n else 0,
            "% of required IDs",
        ),
        "references_resolved_s2_id_rate_pct": (
            100 * sum(r["n_refs_meta_ok"] for r in rows) / refs if refs else None,
            "% of citations",
        ),
        "references_metadata_complete_rate_pct": (
            100 * sum(r["n_refs_meta_complete"] for r in rows) / refs if refs else None,
            "% of citations: S2 ID + title + authors + date/year",
        ),
    }
    write_csv(
        args.out / "corpus_summary.csv",
        [dict(metric=k, value=v, unit=u) for k, (v, u) in corpus_metrics.items()],
    )

    written = ["papers_stats.csv", "references_stats.csv"]
    bench, samples = [], []
    dirs = profile_dirs(args.profile_dir or work_dirs[0] / "profile")
    if dirs:
        agg, samples = aggregate_profiles(dirs, args.time_log)
        bench = benchmark_rows(
            agg, discovery_seconds(work_dirs), len(ids), backoff_seconds(work_dirs)
        )
        periodic, external = checkpoint_seconds(work_dirs)
        bench.extend(
            [
                dict(
                    metric="checkpoint_periodic_time",
                    value=periodic,
                    unit="s",
                    tool="checkpoint_events.jsonl; already included in pipeline wall time",
                ),
                dict(
                    metric="checkpoint_external_time",
                    value=external,
                    unit="s",
                    tool="checkpoint_events.jsonl; additional sync outside pipeline profiles",
                ),
            ]
        )
    session_metrics, session_samples, audit = session_exports(work_dirs, args.data_root, len(ids))
    if session_metrics:
        renamed = {
            "wall_time_e2e": "pipeline_wall_time",
            "avg_paper_time": "pipeline_avg_paper_time",
            "ram_max": "crawler_ram_max",
            "ram_mean": "crawler_ram_mean",
            "disk_max": "chunk_disk_max",
            "output_size": "chunk_output_size",
            "ram_max_main_process": "time_v_max_rss",
        }
        for metric in bench:
            metric["metric"] = renamed.get(metric["metric"], metric["metric"])
        bench.extend(session_metrics)
        samples = session_samples
    audit["papers_with_completed_lifecycle"] = len(
        {r["arxiv_id"] for r in paper_times if r["complete"]}
    )
    audit["session_measurement_complete"] = audit["measurement_complete"]
    audit["paper_timing_complete"] = paper_audit["complete"]
    audit["paper_timing_errors"] = paper_audit["errors"]
    audit["measurement_complete"] = audit["measurement_complete"] and paper_audit["complete"]
    audit["incomplete_paper_attempts"] = sum(not r["complete"] for r in paper_times)
    (args.out / "measurement_audit.json").write_text(json.dumps(audit, indent=2), encoding="utf-8")
    if bench:
        write_csv(args.out / "benchmark_summary.csv", bench)
        write_csv(
            args.out / "resource_timeseries.csv",
            samples,
            list(samples[0]) if samples else ["t_sec", "rss_total_mb", "disk_mb"],
        )
        written += ["benchmark_summary.csv", "resource_timeseries.csv"]

    ok = sum(r["n_versions"] > 0 and r["n_versions_ok"] == r["n_versions"] for r in rows)
    print(f"{len(ids)} IDs, {ok} papers complete -> {args.out}: {', '.join(written)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
