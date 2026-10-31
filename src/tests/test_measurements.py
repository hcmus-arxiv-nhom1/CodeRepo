"""Auditable session resources survive independently of corpus checkpoints."""

import json
import os
import subprocess
import sys

import psutil

from src.tests.helpers import FakeClock


def monitor(tmp_path, monkeypatch):
    from src.benchmark import measurements as m

    clock = FakeClock()
    monkeypatch.setattr(m.time, "monotonic", clock)
    memory = [10_000_000]

    def rss(proc):
        assert proc.pid == os.getpid()  # parent runner, not only a child crawler
        return memory[0], 3

    monkeypatch.setattr(m, "tree_rss", rss)
    local, remote, stage = (tmp_path / n for n in ("run", "remote", "stage"))
    sample = m.SessionMeasurements(stage, remote, local, "session-1", disk_every_s=1)
    return sample, clock, memory, local, remote


def test_session_mean_is_time_weighted_and_includes_final_sync(tmp_path, monkeypatch):
    from src.benchmark.measurements import collect_measurements, session_report

    sample, clock, memory, local, remote = monitor(tmp_path, monkeypatch)
    sample.sample()
    clock.sleep(2)
    memory[0] = 30_000_000
    sample.phase = "final-sync"
    sample.sample()
    clock.sleep(6)
    sample.finish()
    collect_measurements(local, remote)
    result, rows = session_report(local)
    assert result["complete"]
    assert result["wall_s"] == 8
    assert result["rss_max_bytes"] == 30_000_000
    assert result["rss_mean_bytes"] == 25_000_000
    assert rows[-1]["phase"] == "final-sync"


def test_kill_retains_lower_bound_without_fabricating_exact_wall(tmp_path, monkeypatch):
    from src.benchmark.measurements import collect_measurements, session_report

    sample, clock, memory, local, remote = monitor(tmp_path, monkeypatch)
    sample.sample()
    clock.sleep(12)
    sample.sample()
    sample.flush()  # durable batch; simulate kill by never calling finish
    clock.sleep(5)  # lost tail must not be invented by the report
    collect_measurements(local, remote)
    result, _ = session_report(local)
    assert not result["complete"]
    assert result["wall_s"] is None
    assert result["observed_wall_lower_bound_s"] == 12
    assert result["incomplete_sessions"] == ["session-1"]


def test_restore_recovers_telemetry_newer_than_last_corpus_checkpoint(tmp_path, monkeypatch):
    from src.benchmark.checkpoints import Checkpoints
    from src.benchmark.measurements import session_report
    from src.tests.test_benchmark_runner import seed_version

    sample, clock, memory, local, remote = monitor(tmp_path, monkeypatch)
    seed_version(local)
    Checkpoints(local, remote).save(["2604.20535"])
    sample.sample()
    clock.sleep(5)
    sample.sample()
    sample.flush()
    restored = tmp_path / "restored"
    Checkpoints(restored, remote).restore()
    report, _ = session_report(restored)
    assert report["observed_wall_lower_bound_s"] == 5
    assert not report["complete"]


def test_corrupt_or_missing_segment_cannot_certify_complete_timings(tmp_path, monkeypatch):
    from src.benchmark.measurements import collect_measurements, session_report

    sample, clock, memory, local, remote = monitor(tmp_path, monkeypatch)
    sample.sample()
    clock.sleep(2)
    sample.finish()
    part = remote / "measurements/session-1/samples-000000.json"
    data = json.loads(part.read_text())
    data["rows"][0]["rss_bytes"] = 999
    part.write_text(json.dumps(data))
    collect_measurements(local, remote)
    result, _ = session_report(local)
    assert not result["complete"]
    assert result["wall_s"] is None


def test_sampler_disk_scope_includes_restore_and_spool(tmp_path, monkeypatch):
    sample, clock, memory, local, remote = monitor(tmp_path, monkeypatch)
    spool = local / "work/tmp/checkpoints/spool.zip"
    spool.parent.mkdir(parents=True)
    spool.write_bytes(b"x" * 1234)
    sample.phase = "restore"
    sample.sample()
    assert sample.rows[-1]["disk_bytes"] >= 1234
    assert sample.rows[-1]["disk_free_bytes"] > 0


def test_unknown_old_run_is_not_reported_as_zero_time(tmp_path):
    from src.benchmark.measurements import session_report

    result, rows = session_report(tmp_path)
    assert not result["complete"] and result["wall_s"] is None and rows == []


def test_live_paper_journal_is_durable_before_corpus_checkpoint(tmp_path, monkeypatch):
    from src.benchmark.measurements import collect_measurements

    sample, clock, memory, local, remote = monitor(tmp_path, monkeypatch)
    work = local / "work"
    work.mkdir(parents=True)
    event = dict(attempt_id="a", arxiv_id="x", event="start", elapsed_s=0)
    (work / "paper_events.jsonl").write_text(json.dumps(event) + "\n")
    sample.sample()
    sample.flush()
    collect_measurements(local, remote)
    files = list((work / "measurements").glob("*/papers-*.json"))
    assert len(files) == 1
    assert json.loads(files[0].read_text())["rows"] == [event]


def test_timing_audit_requires_complete_session_even_with_complete_chunk(tmp_path, monkeypatch):
    from src.benchmark.measurements import collect_measurements
    from src.benchmark.runner import timing_audit

    sample, clock, memory, local, remote = monitor(tmp_path, monkeypatch)
    sample.sample()
    sample.flush()
    collect_measurements(local, remote)
    profile = local / "work/profile/chunk"
    profile.mkdir(parents=True)
    (profile / "profile_summary.json").write_text("{}")
    assert not timing_audit(local)["timing_complete"]


def test_real_child_memory_and_background_publication_are_observed(tmp_path):
    from src.benchmark.measurements import SessionMeasurements, collect_measurements, session_report

    child = subprocess.Popen(
        [
            sys.executable,
            "-u",
            "-c",
            "import time; data=bytearray(20000000); print('ready', flush=True); time.sleep(30)",
        ],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        assert child.stdout.readline().strip() == "ready"
        child_rss = psutil.Process(child.pid).memory_info().rss
        sample = SessionMeasurements(
            tmp_path / "stage", tmp_path / "remote", tmp_path / "run", "real"
        )
        sample.start()
        summary = sample.finish()
        collect_measurements(tmp_path / "run", tmp_path / "remote")
        report, rows = session_report(tmp_path / "run")
        assert report["complete"] and summary["rss_max_bytes"] >= child_rss
        assert all(r["n_procs"] >= 2 for r in rows)
    finally:
        child.terminate()
        child.wait(timeout=10)
