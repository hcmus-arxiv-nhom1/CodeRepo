"""Offline tests for versioned Hugging Face raw checkpoints."""

import json
from pathlib import Path

import pytest

from src.tests.helpers_hub import MemoryHub
from src.tests.test_benchmark_runner import seed_version


def test_hf_raw_roundtrip_and_interrupted_upload_retains_old_checkpoint(tmp_path):
    from src.benchmark.huggingface import HFCheckpoints

    hub = MemoryHub()
    local = tmp_path / "local"
    remote = tmp_path / "mirror"
    seed_version(local)
    cp = HFCheckpoints(local, remote, hub, "runs/smoke")
    cp.save(["2604.20535"])
    first = hub.read("runs/smoke/LATEST.json")
    assert any("/data/corpus/2604/205/2604-20535/v1/main.tex" in p for p in hub.files())
    (local / "work/run.log").write_text("new log")
    hub.fail = True
    with pytest.raises(OSError):
        cp.save(["2604.20535"])
    assert hub.read("runs/smoke/LATEST.json") == first
    hub.fail = False
    restored = tmp_path / "restored"
    HFCheckpoints(restored, tmp_path / "newmirror", hub, "runs/smoke").restore()
    assert (restored / "data/corpus/2604-20535/v1/main.tex").read_text() == "complete source"
    assert not (restored / "work/run.log").exists()
    assert not any(p.endswith(".zip") for p in hub.files())


def test_hf_restore_rejects_corrupt_source(tmp_path):
    from src.benchmark.huggingface import HFCheckpoints

    hub = MemoryHub()
    local = tmp_path / "local"
    seed_version(local)
    HFCheckpoints(local, tmp_path / "mirror", hub, "runs/smoke").save(["2604.20535"])
    point = json.loads(hub.read("runs/smoke/LATEST.json"))
    tree = hub.revisions[point["revision"]]
    key = next(k for k in tree if k.endswith("main.tex"))
    tree[key] = b"corrupt"
    with pytest.raises(ValueError, match="mismatch"):
        HFCheckpoints(tmp_path / "fresh", tmp_path / "mirror2", hub, "runs/smoke").restore()


def test_hf_empty_version_and_independent_telemetry_survive_restore(tmp_path):
    from src.benchmark.huggingface import HFCheckpoints

    hub = MemoryHub()
    local = tmp_path / "local"
    paper = seed_version(local, status="no_tex")
    (paper / "v1/main.tex").unlink()
    (local / "work/versions.jsonl").write_text(
        json.dumps(dict(arxiv_id="2604.20535", version=1, status="no_tex", files=[])) + "\n"
    )
    HFCheckpoints(local, tmp_path / "mirror", hub, "runs/smoke").save(["2604.20535"])
    hub.commit({"runs/smoke/measurements/s1/start.json": b'{"session_id":"s1"}'})
    restored = tmp_path / "restored"
    HFCheckpoints(restored, tmp_path / "mirror2", hub, "runs/smoke").restore()
    assert (restored / "data/corpus/2604-20535/v1").is_dir()
    assert (restored / "work/measurements/s1/start.json").is_file()


def test_partial_multi_commit_upload_cannot_replace_completed_pointer(tmp_path, monkeypatch):
    from src.benchmark.huggingface import HFCheckpoints

    hub = MemoryHub()
    local = tmp_path / "local"
    seed_version(local)
    cp = HFCheckpoints(local, tmp_path / "mirror", hub, "runs/smoke")
    cp.save(["2604.20535"])
    first = hub.read("runs/smoke/LATEST.json")
    (local / "work/run.log").write_text("partly uploaded log")
    commit = hub.commit

    def interrupted(files, *args, **kwargs):
        commit({k: v for k, v in files.items() if k.endswith("run.log")})
        raise OSError("second commit failed")

    monkeypatch.setattr(hub, "commit", interrupted)
    with pytest.raises(OSError):
        cp.save(["2604.20535"])
    assert hub.read("runs/smoke/LATEST.json") == first
    monkeypatch.setattr(hub, "commit", commit)
    restored = tmp_path / "restored"
    HFCheckpoints(restored, tmp_path / "newmirror", hub, "runs/smoke").restore()
    assert not (restored / "work/run.log").exists()


def test_checkpoint_timing_includes_hf_uploads(tmp_path, monkeypatch):
    from src.benchmark.huggingface import HFCheckpoints
    from src.tests.helpers import FakeClock

    clock = FakeClock()
    monkeypatch.setattr("src.benchmark.huggingface.time.perf_counter", clock)
    hub = MemoryHub()
    commit = hub.commit

    def upload(*args, **kwargs):
        clock.sleep(2)
        return commit(*args, **kwargs)

    monkeypatch.setattr(hub, "commit", upload)
    local = tmp_path / "local"
    seed_version(local)
    HFCheckpoints(local, tmp_path / "mirror", hub, "runs/smoke").save(["2604.20535"])
    event = json.loads((local / "work/checkpoint_events.jsonl").read_text().splitlines()[-1])
    assert event["seconds"] >= 4


def test_hf_missing_token_fails_before_any_network(tmp_path, monkeypatch):
    from src.benchmark.huggingface import HubStore

    monkeypatch.delenv("HF_TOKEN", raising=False)
    with pytest.raises(ValueError, match="HF_TOKEN missing"):
        HubStore("team/private", tmp_path / "cache")


def test_hub_sdk_batches_commits_and_uses_pinned_download(tmp_path, monkeypatch):
    from types import SimpleNamespace

    import huggingface_hub

    from src.benchmark.huggingface import HubStore

    monkeypatch.setenv("HF_TOKEN", "test-only-token")
    calls = []

    class Api:
        def __init__(self, token):
            self.token = token
            self.sha = "0"

        def repo_info(self, repo_id, repo_type):
            assert repo_type == "dataset"
            return SimpleNamespace(private=True, sha=self.sha)

        def create_commit(self, repo_id, **kwargs):
            assert kwargs["parent_commit"] == self.sha
            calls.append(len(kwargs["operations"]))
            self.sha = str(int(self.sha) + 1)
            return SimpleNamespace(oid=self.sha)

    monkeypatch.setattr(huggingface_hub, "HfApi", Api)
    downloads = []

    def download(repo_id, path, **kwargs):
        assert kwargs["revision"] == "2" and kwargs["repo_type"] == "dataset"
        downloads.append(kwargs["local_dir"])
        p = Path(kwargs["local_dir"]) / "result"
        p.write_bytes(b"verified")
        return str(p)

    monkeypatch.setattr(huggingface_hub, "hf_hub_download", download)
    store = HubStore("team/private", tmp_path / "cache")
    assert store.commit({f"file-{i}.txt": b"data" for i in range(120)}) == "3"
    assert calls == [50, 50, 20]
    assert store.read("INDEX.json", "2") == b"verified"
    assert downloads and not list((tmp_path / "cache").iterdir())


def test_hf_shift_refuses_active_owner_and_detects_takeover(tmp_path):
    from src.benchmark.runner import Shift

    hub = MemoryHub()
    shift = Shift(tmp_path, "dang", hub=hub)
    shift.thread.start()
    try:
        with pytest.raises(RuntimeError, match="Another shift"):
            Shift(tmp_path, "hieu", hub=hub)
        hub.commit({"SHIFT.json": json.dumps(dict(token="other", released=False)).encode()})
        shift.last_remote_check = -float("inf")
        with pytest.raises(RuntimeError, match="ownership changed"):
            shift.check()
    finally:
        shift.close(release=False)
