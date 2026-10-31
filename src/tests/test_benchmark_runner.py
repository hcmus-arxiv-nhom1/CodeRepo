"""Offline contracts for full-scope Colab runs and durable checkpoints."""

import json
from pathlib import Path

import pytest

from src.benchmark.checkpoints import Checkpoints, prepare_drive
from src.benchmark.runner import BenchmarkConfig, plan_chunks, prepare_run


def test_final_scope_and_last_partial_chunk():
    chunks = plan_chunks(BenchmarkConfig(run_id="final-1", chunk_size=137))
    ids = [i for c in chunks for i in c.ids]
    assert len(ids) == len(set(ids)) == 20_000
    assert {c.owner for c in chunks} == {"dang", "thien", "tien", "hieu"}
    assert all(len(c.ids) <= 137 for c in chunks)
    sample = plan_chunks(BenchmarkConfig("test", "sample", ("dang",), 0, 23, 10))
    assert [len(c.ids) for c in sample] == [10, 10, 3]
    assert sample[0].ids[0] == "2604.20535"
    assert sample[-1].ids[-1] == "2604.20557"


@pytest.mark.parametrize(
    "changes",
    [
        dict(chunk_size=0),
        dict(run_id="../x"),
        dict(mode="final", count=100),
        dict(ranges=("dang",)),
    ],
)
def test_reject_invalid_final_config(changes):
    with pytest.raises(ValueError):
        plan_chunks(BenchmarkConfig(**({"run_id": "test"} | changes)))


def test_drive_identity_is_explicit_and_not_silently_created(tmp_path):
    drive = tmp_path / "missing"
    with pytest.raises(FileNotFoundError):
        prepare_drive(drive, "team")
    drive.mkdir()
    with pytest.raises(ValueError):
        prepare_drive(drive, "team")
    prepare_drive(drive, "team", initialize=True)
    prepare_drive(drive, "team")
    with pytest.raises(ValueError):
        prepare_drive(drive, "another")


def test_new_run_refuses_existing_and_resume_checks_revision(tmp_path):
    cfg = BenchmarkConfig("sample", "sample", ("dang",), count=1)
    prepare_run(cfg, tmp_path, "abc", resume=False)
    with pytest.raises(FileExistsError):
        prepare_run(cfg, tmp_path, "abc", resume=False)
    prepare_run(cfg, tmp_path, "abc", resume=True)
    with pytest.raises(ValueError):
        prepare_run(cfg, tmp_path, "def", resume=True)


def test_rejected_revision_never_finalizes_or_changes_saved_run(tmp_path):
    from src.benchmark.runner import run_benchmark

    drive = tmp_path / "drive"
    drive.mkdir()
    prepare_drive(drive, "team", initialize=True)
    cfg = BenchmarkConfig("sample", "sample", ("dang",), count=1, references=False)
    manifest = prepare_run(cfg, drive / "runs", "old", resume=False)
    local, remote = tmp_path / "local/sample", drive / "runs/sample"
    seed_version(local)
    (local / "work/manifest.json").write_text(json.dumps(manifest))
    first = Checkpoints(local, remote).save(["2604.20535"])
    before = (local / "work/manifest.json").read_bytes()
    with pytest.raises(ValueError, match="revision differs"):
        run_benchmark(
            cfg,
            drive_root=drive,
            local_root=tmp_path / "local",
            revision="new",
            team_id="team",
            owner="dang",
            resume=True,
        )
    index = json.loads((remote / "INDEX.json").read_text())
    assert index["generation"] == first["generation"]
    assert (local / "work/manifest.json").read_bytes() == before


def seed_version(root, *, status="ok"):
    work = root / "work"
    paper = root / "data/corpus/2604-20535"
    (paper / "v1").mkdir(parents=True)
    work.mkdir(parents=True)
    (paper / "metadata.json").write_text(json.dumps({"arxiv_id": "2604.20535"}))
    (paper / "v1/main.tex").write_text("complete source")
    rec = dict(arxiv_id="2604.20535", version=1, status=status, files=["main.tex"])
    (work / "versions.jsonl").write_text(json.dumps(rec) + "\n")
    return paper


def test_checkpoint_survives_kill_and_ignores_stray_upload(tmp_path):
    local, remote, restored = (tmp_path / n for n in ("local", "remote", "restored"))
    seed_version(local)
    cp = Checkpoints(local, remote)
    first = cp.save(["2604.20535"])
    assert first["versions"] == 1
    # A kill mid-copy leaves a stray temp file next to the real one; it must be ignored.
    (remote / "data/corpus/2604-20535/v1/main.tex.tmp-deadbeef").write_bytes(b"cut upload")
    Checkpoints(restored, remote).restore()
    assert (restored / "data/corpus/2604-20535/v1/main.tex").read_text() == "complete source"
    assert not (restored / "work/run.lock").exists()
    assert not (restored / "data/corpus/2604-20535/v1/main.tex.tmp-deadbeef").exists()


def test_failed_version_is_not_committed_and_corruption_is_detected(tmp_path):
    local, remote = tmp_path / "local", tmp_path / "remote"
    paper = seed_version(local)
    cp = Checkpoints(local, remote)
    first = cp.save(["2604.20535"])
    # A failed, partially extracted version must never enter the durable corpus.
    (paper / "v2").mkdir()
    (paper / "v2/main.tex").write_text("partial")
    with (local / "work/versions.jsonl").open("a") as fh:
        fh.write(json.dumps(dict(arxiv_id="2604.20535", version=2, status="failed")) + "\n")
    second = cp.save(["2604.20535"])
    assert second["versions"] == first["versions"] == 1
    assert not (remote / "data/corpus/2604-20535/v2").exists()
    # A bit flip on an already-mirrored file must be reported, never silently reused.
    (remote / "data/corpus/2604-20535/v1/main.tex").write_bytes(b"corrupted")
    restored = tmp_path / "restored"
    with pytest.raises(ValueError, match="mismatch"):
        Checkpoints(restored, remote).restore()


def test_verification_failure_never_publishes_completion(tmp_path, monkeypatch):
    local, remote = tmp_path / "local", tmp_path / "remote"
    seed_version(local)
    cp = Checkpoints(local, remote)
    monkeypatch.setattr(cp, "_verify", lambda *a: False)
    with pytest.raises(OSError):
        cp.save(["2604.20535"])
    assert not (remote / "INDEX.json").exists()


def test_checkpoint_requires_every_recorded_source_file(tmp_path):
    local, remote = tmp_path / "local", tmp_path / "remote"
    paper = seed_version(local)
    (paper / "v1/main.tex").unlink()
    with pytest.raises(ValueError):
        Checkpoints(local, remote).save(["2604.20535"])


def test_empty_version_directory_round_trips(tmp_path):
    local, remote = tmp_path / "local", tmp_path / "remote"
    paper = seed_version(local, status="no_tex")
    (paper / "v1/main.tex").unlink()
    (local / "work/versions.jsonl").write_text(
        json.dumps(dict(arxiv_id="2604.20535", version=1, status="no_tex", files=[])) + "\n"
    )
    Checkpoints(local, remote).save(["2604.20535"])
    restored = tmp_path / "restored"
    Checkpoints(restored, remote).restore()
    assert (restored / "data/corpus/2604-20535/v1").is_dir()


def test_restore_rejects_unsafe_archive_path():
    assert not Checkpoints.safe_name("../escape")
    assert not Checkpoints.safe_name("/absolute")
    assert Checkpoints.safe_name("data/corpus/a/v1/main.tex")


def test_checkpoint_copies_land_directly_under_remote_mirror(tmp_path):
    local, remote = tmp_path / "local", tmp_path / "remote"
    seed_version(local)
    Checkpoints(local, remote).save(["2604.20535"])
    assert (remote / "data/corpus/2604-20535/v1/main.tex").read_text() == "complete source"
    assert (remote / "data/corpus/2604-20535/metadata.json").is_file()
    assert not list(remote.glob("**/*.zip"))


def test_all_checkpoint_phases_have_durable_duration_events(tmp_path):
    local, remote = tmp_path / "local", tmp_path / "remote"
    seed_version(local)
    rec = Checkpoints(local, remote).save(["2604.20535"], phase="final")
    event = json.loads((remote / "checkpoint-events" / (rec["generation"] + ".json")).read_text())
    assert event["seconds"] >= 0 and event["phase"] == "final"
    assert not event["included_in_pipeline"]


def test_timing_audit_does_not_hide_lost_profile_summary(tmp_path):
    from src.benchmark.runner import timing_audit

    profile = tmp_path / "work/profile/interrupted"
    profile.mkdir(parents=True)
    (profile / "profile_samples.csv").write_text("t_sec,rss_total_mb,n_procs,disk_mb\n12,8,1,1\n")
    result = timing_audit(tmp_path)
    assert not result["timing_complete"]
    assert result["unfinished_profiles"] == ["interrupted"]


def test_timing_audit_with_no_started_chunk_is_incomplete(tmp_path):
    from src.benchmark.runner import timing_audit

    assert not timing_audit(tmp_path)["timing_complete"]


def test_same_owner_second_session_still_requires_explicit_takeover(tmp_path):
    from src.benchmark.runner import Shift

    first = Shift(tmp_path, "dang")
    with pytest.raises(RuntimeError):
        Shift(tmp_path, "dang")
    first.thread.start()
    first.close(release=True)
    second = Shift(tmp_path, "tien")
    second.thread.start()
    second.close(release=True)


@pytest.mark.parametrize("readback_failure", [False, True])
def test_run_orchestrator_profiles_checks_and_exports_without_network(
    tmp_path, monkeypatch, readback_failure
):
    from src.benchmark import runner
    from src.tests import test_run_pipeline as fixture

    monkeypatch.setattr(fixture, "VERSIONS", {"2604.20535": 1})
    monkeypatch.setattr(
        "src.run_pipeline.HttpClient", lambda *a: fixture.client_for(fixture.FakeArxiv())
    )
    drive = tmp_path / "drive"
    drive.mkdir()
    prepare_drive(drive, "team", initialize=True)

    class FinishedProcess:
        def __init__(self, command, **kwargs):
            payload = json.loads(Path(command[-1]).read_text())
            profile = Path(command[command.index("--out") + 1])
            before_launch = tmp_path / "before-launch"
            Checkpoints(before_launch, drive / "runs/smoke").restore()
            assert (before_launch / "work/profile" / profile.name / "attempt.json").is_file()
            assert not runner.timing_audit(before_launch)["timing_complete"]
            runner.run_child(payload)
            (profile / "profile_summary.json").write_text(
                json.dumps(
                    dict(
                        wall_s=1,
                        rss_max_mb=10,
                        rss_mean_mb=8,
                        disk_max_mb=1,
                        output_mb=1,
                        started_at="2026-10-30T00:00:00Z",
                        exit_code=0,
                    )
                )
            )
            (profile / "profile_samples.csv").write_text(
                "t_sec,rss_total_mb,n_procs,disk_mb\n0,8,1,1\n"
            )
            self.returncode = 0

        def poll(self):
            return self.returncode

    monkeypatch.setattr(runner.subprocess, "Popen", FinishedProcess)
    cfg = BenchmarkConfig(
        "smoke", "sample", ("dang",), count=1, references=False, checkpoint_s=0.000001
    )
    if readback_failure:
        from src.benchmark.measurements import SessionMeasurements, session_report

        publish = SessionMeasurements._publish

        def failed_readback(self, name, value):
            publish(self, name, value)
            if name == "summary.json":
                raise OSError("Drive summary read-back failed")

        monkeypatch.setattr(SessionMeasurements, "_publish", failed_readback)
        with pytest.raises(RuntimeError, match="Cleanup errors"):
            runner.run_benchmark(
                cfg,
                drive_root=drive,
                local_root=tmp_path / "local",
                revision="abc",
                team_id="team",
                owner="dang",
            )
        measured, _ = session_report(tmp_path / "local/smoke")
        assert not measured["complete"]
        return
    result = runner.run_benchmark(
        cfg,
        drive_root=drive,
        local_root=tmp_path / "local",
        revision="abc",
        team_id="team",
        owner="dang",
    )
    assert result["complete"] and not result["final_complete"]
    assert (tmp_path / "local/smoke/bench_results.zip").is_file()
    from src.benchmark.measurements import session_report

    measured, _ = session_report(tmp_path / "local/smoke")
    assert measured["complete"] and measured["wall_s"] > 0
    assert measured["rss_max_bytes"] > 0
    assert result["timing_complete"] and result["measurement_complete"]
    restored = tmp_path / "restored"
    Checkpoints(restored, drive / "runs/smoke").restore()
    assert (restored / "work/coverage.json").is_file()
    assert json.loads((drive / "SHIFT.json").read_text())["released"]


def test_missing_shift_does_not_mask_original_error_or_prevent_local_evidence(
    tmp_path, monkeypatch
):
    from src.benchmark import runner

    drive = tmp_path / "drive"
    drive.mkdir()
    prepare_drive(drive, "team", initialize=True)
    original = Checkpoints.save

    def failed(cp, ids, **kwargs):
        if kwargs.get("phase") == "prelaunch":
            (drive / "SHIFT.json").unlink()
            raise ValueError("original chunk failure")
        return original(cp, ids, **kwargs)

    monkeypatch.setattr(Checkpoints, "save", failed)
    cfg = BenchmarkConfig("failed", "sample", ("dang",), count=1, references=False)
    with pytest.raises(ValueError, match="original chunk failure"):
        runner.run_benchmark(
            cfg,
            drive_root=drive,
            local_root=tmp_path / "local",
            revision="abc",
            team_id="team",
            owner="dang",
        )
    local = tmp_path / "local/failed"
    assert (local / "bench_results.zip").is_file()
    errors = json.loads((local / "work/cleanup_errors.json").read_text())
    assert errors["errors"]
    assert list((local / "work/measurements").glob("*/summary.json"))


def test_shift_read_retries_transient_disappearance_without_recreating_marker(
    tmp_path, monkeypatch
):
    from src.benchmark.runner import Shift

    shift = Shift(tmp_path, "dang")
    original = Path.read_text
    failures = [True]

    def transient(path, *args, **kwargs):
        if path == shift.path and failures:
            failures.pop()
            raise FileNotFoundError("temporary missing marker")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", transient)
    shift.check()
    assert json.loads(original(shift.path))["token"] == shift.token


def test_export_failure_keeps_primary_error_and_creates_raw_rescue(tmp_path, monkeypatch):
    from zipfile import ZipFile

    from src.benchmark import runner

    drive = tmp_path / "drive"
    drive.mkdir()
    prepare_drive(drive, "team", initialize=True)
    original = Checkpoints.save

    def failed(cp, ids, **kwargs):
        if kwargs.get("phase") == "prelaunch":
            raise ValueError("original failure")
        return original(cp, ids, **kwargs)

    def no_export(*a):
        raise RuntimeError("stats export failed")

    monkeypatch.setattr(Checkpoints, "save", failed)
    monkeypatch.setattr(runner, "export_evidence", no_export)
    cfg = BenchmarkConfig("failed", "sample", ("dang",), count=1, references=False)
    with pytest.raises(ValueError, match="original failure"):
        runner.run_benchmark(
            cfg,
            drive_root=drive,
            local_root=tmp_path / "local",
            revision="abc",
            team_id="team",
            owner="dang",
        )
    with ZipFile(tmp_path / "local/failed/rescue_results.zip") as archive:
        assert "work/cleanup_errors.json" in archive.namelist()
        assert "work/manifest.json" in archive.namelist()
