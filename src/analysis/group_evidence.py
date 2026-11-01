"""Combine independent crawl evidence without inventing a single-machine benchmark."""

from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from datetime import datetime, timedelta
from pathlib import Path

from src import config
from src.benchmark.measurements import session_report
from src.utils.ids import iter_id_range


def assigned_ids() -> set[str]:
    return {
        paper
        for start, end in config.RANGES.values()
        for paper in iter_id_range(start, end, config.MONTH_END)
    }


def _json(path: Path) -> dict:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"Expected JSON object: {path}")
    return value


def _session_intervals(root: Path, complete: bool) -> list[tuple[datetime, datetime]]:
    if not complete:
        return []
    intervals = []
    for folder in sorted((root / "work/measurements").glob("*")):
        if not folder.is_dir():
            continue
        start = datetime.fromisoformat(_json(folder / "start.json")["started_at"])
        if start.tzinfo is None:
            raise ValueError(f"Missing timezone in {folder}")
        seconds = float(_json(folder / "summary.json")["wall_s"])
        if seconds < 0:
            raise ValueError(f"Negative duration in {folder}")
        intervals.append((start, start + timedelta(seconds=seconds)))
    return intervals


def aggregate_evidence(run_roots, output: Path, *, expected_ids=None) -> dict:
    """Write a new audit, attributed paper rows and run logs from extracted evidence.

    Session work is summed separately from timeline span. These numbers never certify
    the grading definition of a full benchmark across multiple machines.
    """
    roots = [Path(p).resolve() for p in run_roots]
    if not roots:
        raise ValueError("Provide at least one extracted evidence directory")
    out = Path(output).resolve()
    if out.exists():
        raise FileExistsError(f"Output exists: {out}")
    if any(out.is_relative_to(root) or root.is_relative_to(out) for root in roots):
        raise ValueError("Output must be separate from every input directory")
    if len(set(roots)) != len(roots):
        raise ValueError("The same input directory was supplied twice")
    expected = set(assigned_ids() if expected_ids is None else expected_ids)
    if not expected:
        raise ValueError("Expected ID selection is empty")

    manifests, counts, issues, timing_issues = [], Counter(), [], []
    papers, fields, intervals = [], [], []
    reports = []
    log_sources = []
    for root in roots:
        manifest = _json(root / "work/manifest.json")
        cfg = manifest["config"]
        run_id = cfg["run_id"]
        ids = manifest["required_ids"]
        if not isinstance(ids, list) or not all(isinstance(x, str) for x in ids):
            raise ValueError(f"Invalid ID list in {root}")
        counts.update(ids)
        manifests.append(manifest)
        log_sources.append((run_id, root / "work/run.log"))
        if cfg.get("mode") == "sample":
            issues.append(f"{run_id}: sample mode cannot certify final coverage")
        if not cfg.get("references"):
            issues.append(f"{run_id}: references disabled")
        try:
            coverage = _json(root / "work/coverage.json")
            if not (
                coverage.get("complete") is True
                and coverage.get("required_ids") == len(ids)
                and coverage.get("unique_ids") == len(set(ids))
                and coverage.get("papers_source_complete") == len(ids)
                and coverage.get("papers_with_reference_or_source_errors") == 0
            ):
                issues.append(f"{run_id}: coverage is incomplete or inconsistent")
        except (OSError, ValueError):
            issues.append(f"{run_id}: coverage evidence unavailable")
        run_rows = []
        try:
            with (root / "book_csv/papers_stats.csv").open(encoding="utf-8", newline="") as fh:
                reader = csv.DictReader(fh)
                if not reader.fieldnames or "arxiv_id" not in reader.fieldnames:
                    raise ValueError("missing arxiv_id")
                for name in reader.fieldnames:
                    if name != "run_id" and name not in fields:
                        fields.append(name)
                run_rows = list(reader)
            row_counts = Counter(row.get("arxiv_id") for row in run_rows)
            if set(row_counts) != set(ids) or any(n != 1 for n in row_counts.values()):
                issues.append(f"{run_id}: paper statistics do not match manifest")
            if any(
                not row.get("n_versions", "").isdigit()
                or int(row["n_versions"]) < 1
                or row.get("n_versions_ok") != row.get("n_versions")
                or row.get("ref_status") not in ("ok", "not_found")
                for row in run_rows
            ):
                issues.append(f"{run_id}: paper version or reference statistics incomplete")
            papers.extend(dict(row, run_id=run_id) for row in run_rows)
        except (OSError, ValueError, csv.Error):
            issues.append(f"{run_id}: paper statistics unavailable or invalid")
        try:
            discoveries = {}
            for line in (root / "work/discovery.jsonl").read_text(encoding="utf-8").splitlines():
                if line.strip():
                    entry = json.loads(line)
                    discoveries[entry["arxiv_id"]] = entry
            versions = {}
            for line in (root / "work/versions.jsonl").read_text(encoding="utf-8").splitlines():
                if line.strip():
                    entry = json.loads(line)
                    versions[(entry["arxiv_id"], int(entry["version"]))] = entry
            wanted = set(ids)
            for paper in ids:
                entry = discoveries.get(paper, {})
                count = entry.get("num_versions")
                if entry.get("status") != "found" or not isinstance(count, int) or count < 1:
                    issues.append(f"{run_id}: missing discovery for {paper}")
                    continue
                for number in range(1, count + 1):
                    if versions.get((paper, number), {}).get("status") not in (
                        "ok",
                        "no_tex",
                        "unavailable",
                    ):
                        issues.append(f"{run_id}: incomplete version {paper}v{number}")
            if any(paper not in wanted for paper, _ in versions):
                issues.append(f"{run_id}: version journal has an unexpected ID")
            if any(
                paper in wanted and number > (discoveries.get(paper, {}).get("num_versions") or 0)
                for paper, number in versions
            ):
                issues.append(f"{run_id}: version journal has an unexpected version")
            for row in run_rows:
                if row.get("arxiv_id") in wanted:
                    entry = discoveries.get(row["arxiv_id"], {})
                    if str(entry.get("num_versions")) != row.get("n_versions"):
                        issues.append(
                            f"{run_id}: statistics disagree with discovery for {row['arxiv_id']}"
                        )
        except (OSError, ValueError, KeyError, TypeError):
            issues.append(f"{run_id}: discovery/version journal unavailable or invalid")
        if not (root / "work/run.log").is_file():
            issues.append(f"{run_id}: run.log unavailable")
        session, _ = session_report(root)
        try:
            timing = _json(root / "work/timing_audit.json")
            timing_complete = timing.get("timing_complete") is True
        except (OSError, ValueError):
            timing_complete = False
        complete = bool(session["complete"] and timing_complete)
        if complete:
            try:
                intervals.extend(_session_intervals(root, True))
            except (OSError, ValueError, KeyError, TypeError, OverflowError):
                timing_issues.append(f"{run_id}: invalid session timestamps")
                complete = False
        reports.append(dict(session, run_id=run_id, complete=complete))

    run_counts = Counter(m["config"]["run_id"] for m in manifests)
    duplicate_runs = sorted(k for k, n in run_counts.items() if n > 1)
    if duplicate_runs:
        issues.append("Duplicate run IDs: " + ", ".join(duplicate_runs))
    incompatible = []
    for field in ("code_revision", "snapshot_sha256"):
        values = {m.get(field) for m in manifests}
        if len(values) != 1 or None in values:
            incompatible.append(field)
    duplicate = sorted(k for k, n in counts.items() if n > 1)
    missing = sorted(expected - counts.keys())
    extra = sorted(counts.keys() - expected)
    complete_measurements = all(r["complete"] for r in reports) and bool(intervals)
    spans = sorted(intervals)
    overlap = any(b[0] < a[1] for i, a in enumerate(spans) for b in spans[i + 1 :])
    active = sum((end - start).total_seconds() for start, end in spans)
    elapsed = (
        (max(x[1] for x in spans) - min(x[0] for x in spans)).total_seconds() if spans else None
    )
    report = dict(
        runs=reports,
        required_ids=len(expected),
        observed_unique_ids=len(counts),
        duplicate_ids=duplicate,
        missing_ids=missing,
        unexpected_ids=extra,
        incompatible_fields=incompatible,
        issues=issues,
        timing_issues=timing_issues,
        coverage_complete=not (
            duplicate or duplicate_runs or missing or extra or incompatible or issues
        ),
        measurement_complete=complete_measurements,
        active_wall_sum_s=active if complete_measurements else None,
        timeline_span_s=elapsed if complete_measurements else None,
        sessions_overlap=overlap if complete_measurements else None,
        timing_scope=(
            "Independent sampled session durations; timeline span includes offline gaps "
            "and relies on runtime UTC clocks. Not a single-machine benchmark."
        ),
        corpus_verification=(
            "Based on per-run QA reports and journals; corpus files are not "
            "revalidated by this aggregation."
        ),
        official_final_certified=False,
    )
    out.mkdir(parents=True, exist_ok=False)
    (out / "group_audit.json").write_text(
        json.dumps(report, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    with (out / "papers_stats.csv").open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=["run_id", *fields], extrasaction="ignore")
        writer.writeheader()
        writer.writerows(papers)
    with (out / "run_logs.jsonl").open("w", encoding="utf-8") as output_log:
        for run_id, path in log_sources:
            if path.is_file():
                with path.open(encoding="utf-8", errors="replace") as input_log:
                    for number, line in enumerate(input_log, 1):
                        output_log.write(
                            json.dumps(
                                dict(run_id=run_id, line=number, text=line.rstrip("\n")),
                                ensure_ascii=False,
                            )
                            + "\n"
                        )
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--run",
        action="append",
        type=Path,
        required=True,
        help="Extracted evidence directory (repeat for each run)",
    )
    parser.add_argument("--out", type=Path, required=True, help="New output directory")
    args = parser.parse_args()
    report = aggregate_evidence(args.run, args.out)
    print(
        json.dumps(
            {
                k: report[k]
                for k in (
                    "required_ids",
                    "observed_unique_ids",
                    "coverage_complete",
                    "measurement_complete",
                    "official_final_certified",
                )
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
