"""Tests for multi-run evidence provenance and timing audit."""

import csv
import json
from datetime import UTC, datetime, timedelta

from src.benchmark.measurements import rows_digest


def make_run(root, run_id, ids, *, started, revision="code1", snapshot="sha1", timing=True):
    work = root / "work"
    work.mkdir(parents=True)
    (work / "manifest.json").write_text(
        json.dumps(
            {
                "config": {"run_id": run_id, "mode": "partition", "references": True},
                "code_revision": revision,
                "snapshot_sha256": snapshot,
                "required_ids": ids,
            }
        )
    )
    (work / "coverage.json").write_text(
        json.dumps(
            {
                "complete": True,
                "required_ids": len(ids),
                "unique_ids": len(ids),
                "papers_source_complete": len(ids),
                "papers_with_reference_or_source_errors": 0,
            }
        )
    )
    (work / "run.log").write_text(f"{run_id} first log\n{run_id} second log\n")
    (work / "discovery.jsonl").write_text(
        "".join(
            json.dumps({"arxiv_id": i, "status": "found", "num_versions": 1}) + "\n" for i in ids
        )
    )
    (work / "versions.jsonl").write_text(
        "".join(json.dumps({"arxiv_id": i, "version": 1, "status": "ok"}) + "\n" for i in ids)
    )
    books = root / "book_csv"
    books.mkdir()
    (books / "papers_stats.csv").write_text(
        "arxiv_id,n_versions,n_versions_ok,ref_status\n" + "".join(f"{i},1,1,ok\n" for i in ids)
    )
    if timing:
        folder = work / "measurements" / (run_id + "-session")
        folder.mkdir(parents=True)
        (folder / "start.json").write_text(json.dumps({"started_at": started.isoformat()}))
        rows = [
            {
                "t_sec": t,
                "rss_bytes": 100,
                "disk_bytes": 200,
                "disk_free_bytes": 500,
                "phase": "crawl",
                "at": (started + timedelta(seconds=t)).isoformat(),
            }
            for t in (0, 10)
        ]
        (folder / "samples-000000.json").write_text(
            json.dumps({"sequence": 0, "rows": rows, "sha256": rows_digest(rows)})
        )
        (folder / "summary.json").write_text(
            json.dumps({"complete": True, "segments": 1, "wall_s": 10})
        )
        (work / "timing_audit.json").write_text(json.dumps({"timing_complete": True}))
    return root


def test_aggregate_preserves_run_attribution_and_does_not_sum_overlapping_wall(tmp_path):
    from src.analysis.group_evidence import aggregate_evidence

    t = datetime(2026, 1, 1, tzinfo=UTC)
    a = make_run(tmp_path / "a", "run-a", ["id1"], started=t)
    b = make_run(tmp_path / "b", "run-b", ["id2"], started=t + timedelta(seconds=5))
    out = tmp_path / "result"
    report = aggregate_evidence([a, b], out, expected_ids={"id1", "id2"})
    assert report["coverage_complete"] and report["measurement_complete"]
    assert report["active_wall_sum_s"] == 20
    assert report["timeline_span_s"] == 15
    assert report["sessions_overlap"] is True
    assert report["official_final_certified"] is False
    with (out / "papers_stats.csv").open(newline="") as f:
        papers = list(csv.DictReader(f))
    assert {(p["run_id"], p["arxiv_id"]) for p in papers} == {("run-a", "id1"), ("run-b", "id2")}
    lines = [json.loads(x) for x in (out / "run_logs.jsonl").read_text().splitlines()]
    assert len(lines) == 4 and {r["run_id"] for r in lines} == {"run-a", "run-b"}


def test_aggregate_reports_overlaps_missing_and_incompatible_runs(tmp_path):
    from src.analysis.group_evidence import aggregate_evidence

    t = datetime(2026, 1, 1, tzinfo=UTC)
    a = make_run(tmp_path / "a", "run-a", ["id1"], started=t)
    b = make_run(tmp_path / "b", "run-b", ["id1"], started=t, revision="different", timing=False)
    report = aggregate_evidence([a, b], tmp_path / "out", expected_ids={"id1", "id2"})
    assert report["duplicate_ids"] == ["id1"]
    assert report["missing_ids"] == ["id2"]
    assert not report["coverage_complete"] and not report["measurement_complete"]
    assert report["active_wall_sum_s"] is None
    assert "code_revision" in report["incompatible_fields"]


def test_aggregate_refuses_to_overwrite_output_or_write_inside_input(tmp_path):
    import pytest

    from src.analysis.group_evidence import aggregate_evidence

    a = make_run(tmp_path / "a", "run-a", ["id1"], started=datetime.now(UTC))
    with pytest.raises(ValueError):
        aggregate_evidence([a], a / "generated", expected_ids={"id1"})
    with pytest.raises(FileExistsError):
        aggregate_evidence([a], a, expected_ids={"id1"})


def test_group_cannot_claim_coverage_when_version_journal_missing(tmp_path):
    from src.analysis.group_evidence import aggregate_evidence

    run = make_run(tmp_path / "a", "run-a", ["id1"], started=datetime.now(UTC))
    (run / "work/versions.jsonl").write_text("")
    report = aggregate_evidence([run], tmp_path / "out", expected_ids={"id1"})
    assert not report["coverage_complete"]
    assert any("version" in issue for issue in report["issues"])


def test_group_treats_invalid_session_clock_as_incomplete(tmp_path):
    from src.analysis.group_evidence import aggregate_evidence

    run = make_run(tmp_path / "a", "run-a", ["id1"], started=datetime.now(UTC))
    folder = run / "work/measurements/run-a-session"
    (folder / "start.json").write_text(json.dumps({"started_at": "not-a-time"}))
    report = aggregate_evidence([run], tmp_path / "out", expected_ids={"id1"})
    assert not report["measurement_complete"]
    assert report["timeline_span_s"] is None
