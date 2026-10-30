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


def test_checkpoint_survives_kill_and_ignores_uncommitted_upload(tmp_path):
    local, remote, restored = (tmp_path / n for n in ("local", "remote", "restored"))
    seed_version(local)
    cp = Checkpoints(local, remote)
    first = cp.save(["2604.20535"])
    assert first["versions"] == 1
    (remote / "checkpoints/incomplete.zip").write_bytes(b"cut upload")
    Checkpoints(restored, remote).restore()
    assert (restored / "data/corpus/2604-20535/v1/main.tex").read_text() == "complete source"
    assert not (restored / "work/run.lock").exists()


def test_failed_version_is_not_committed_and_corrupt_latest_falls_back(tmp_path):
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
    (remote / "checkpoints" / second["archive"]).write_bytes(b"corrupted")
    restored = tmp_path / "restored"
    result = Checkpoints(restored, remote).restore()
    assert result["generation"] == first["generation"]
    assert not (restored / "data/corpus/2604-20535/v2").exists()
    third = Checkpoints(restored, remote).save(["2604.20535"])
    assert third["parent"] == first["generation"]


def test_verification_failure_never_publishes_completion(tmp_path, monkeypatch):
    local, remote = tmp_path / "local", tmp_path / "remote"
    seed_version(local)
    cp = Checkpoints(local, remote)
    monkeypatch.setattr(cp, "_verify", lambda *a: False)
    with pytest.raises(OSError):
        cp.save(["2604.20535"])
    assert not list((remote / "checkpoints").glob("*.json"))


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


def test_checkpoint_spool_is_inside_measured_work_tree(tmp_path, monkeypatch):
    import src.benchmark.checkpoints as module

    local, remote = tmp_path / "local", tmp_path / "remote"
    seed_version(local)
    original = module.shutil.copyfile

    def copy(source, destination):
        assert Path(source).is_relative_to(local / "work")
        return original(source, destination)

    monkeypatch.setattr(module.shutil, "copyfile", copy)
    Checkpoints(local, remote).save(["2604.20535"])


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


def test_run_orchestrator_profiles_checks_and_exports_without_network(tmp_path, monkeypatch):
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
    restored = tmp_path / "restored"
    Checkpoints(restored, drive / "runs/smoke").restore()
    assert (restored / "work/coverage.json").is_file()
    assert json.loads((drive / "SHIFT.json").read_text())["released"]
