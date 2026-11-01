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
    assert store.commit({f"file-{i}.txt": b"data" for i in range(1200)}) == "3"
    assert calls == [500, 500, 200]
    assert store.read("INDEX.json", "2") == b"verified"
    assert downloads and not list((tmp_path / "cache").glob("hf-read-*"))


def test_commit_budget_survives_new_store_and_waits_for_window(tmp_path, monkeypatch):
    from src.benchmark import huggingface as h
    from src.tests.helpers import FakeClock

    clock = FakeClock()
    monkeypatch.setattr(h.time, "time", clock)
    monkeypatch.setattr(h.time, "sleep", clock.sleep)
    path = tmp_path / "shared-budget.json"
    h.CommitBudget(path, limit=2, window_s=60).reserve()
    h.CommitBudget(path, limit=2, window_s=60).reserve()
    h.CommitBudget(path, limit=2, window_s=60).reserve()
    assert clock() == 60


@pytest.mark.parametrize("retry_after, expected_delay", [("2", 2), (None, 3600)])
def test_hub_commit_retries_429_using_server_delay(
    tmp_path, monkeypatch, retry_after, expected_delay
):
    from types import SimpleNamespace

    import httpx
    import huggingface_hub
    from huggingface_hub.errors import HfHubHTTPError

    from src.benchmark import huggingface as h
    from src.tests.helpers import FakeClock

    clock = FakeClock()
    monkeypatch.setattr(h.time, "time", clock)
    monkeypatch.setattr(h.time, "sleep", clock.sleep)
    monkeypatch.setenv("HF_TOKEN", "test-only-token")
    calls = []

    class Api:
        def __init__(self, token):
            self.token = token

        def repo_info(self, *args, **kwargs):
            return SimpleNamespace(private=True, sha="parent")

        def create_commit(self, *args, **kwargs):
            calls.append(kwargs)
            if len(calls) == 1:
                response = httpx.Response(
                    429,
                    headers={"Retry-After": retry_after} if retry_after else {},
                    request=httpx.Request("POST", "https://huggingface.co/test"),
                )
                raise HfHubHTTPError("rate limit", response=response)
            return SimpleNamespace(oid="done")

    monkeypatch.setattr(huggingface_hub, "HfApi", Api)
    store = h.HubStore("team/private", tmp_path / "cache")
    assert store.commit({"INDEX.json": b"{}"}) == "done"
    assert len(calls) == 2 and clock() == expected_delay


def test_commit_batches_respect_byte_limit_with_paths_and_bytes(tmp_path):
    from src.benchmark.huggingface import commit_batches

    file = tmp_path / "source.tex"
    file.write_bytes(b"123456")
    batches = list(
        commit_batches({"a": b"12345", "b": file, "c": b"12"}, max_files=500, max_bytes=10)
    )
    assert [[name for name, _ in batch] for batch in batches] == [["a"], ["b", "c"]]
    # One oversized source file stays intact and is sent alone.
    assert list(commit_batches({"large": file, "small": b"1"}, max_bytes=4)) == [
        [("large", file)],
        [("small", b"1")],
    ]


def test_checkpoint_does_not_reupload_unchanged_control_files(tmp_path, monkeypatch):
    from src.benchmark.huggingface import HFCheckpoints

    hub = MemoryHub()
    local, remote = tmp_path / "local", tmp_path / "mirror"
    seed_version(local)
    remote.mkdir()
    (remote / "MANIFEST.json").write_text('{"run_id":"smoke"}')
    sessions = remote / "sessions"
    sessions.mkdir()
    (sessions / "s1.json").write_text('{"seconds":10}')
    cp = HFCheckpoints(local, remote, hub, "runs/smoke")
    cp.save(["2604.20535"])
    batches = []
    commit = hub.commit

    def capture(files, *args, **kwargs):
        batches.append(set(files))
        return commit(files, *args, **kwargs)

    monkeypatch.setattr(hub, "commit", capture)
    (local / "work/run.log").write_text("new data")
    cp.save(["2604.20535"])
    names = set.union(*batches)
    assert "runs/smoke/work/run.log" in names
    assert "runs/smoke/MANIFEST.json" not in names
    assert "runs/smoke/sessions/s1.json" not in names


def test_hf_revision_history_avoids_local_drive_backup_objects(tmp_path):
    from src.benchmark.huggingface import HFCheckpoints

    hub = MemoryHub()
    local, mirror = tmp_path / "local", tmp_path / "mirror"
    seed_version(local)
    cp = HFCheckpoints(local, mirror, hub, "runs/smoke")
    cp.save(["2604.20535"])
    (local / "work/versions.jsonl").write_text((local / "work/versions.jsonl").read_text() + "\n")
    cp.save(["2604.20535"])
    assert not (mirror / ".checkpoint-objects").exists()
    restored = tmp_path / "restored"
    HFCheckpoints(restored, tmp_path / "mirror2", hub, "runs/smoke").restore()
    assert (restored / "data/corpus/2604-20535/v1/main.tex").read_text() == "complete source"


def test_hf_run_claim_is_unique_without_shift_marker():
    from src.benchmark.runner import claim_hf_run

    hub = MemoryHub()
    claim_hf_run(hub, "run-a", {"config": {"run_id": "run-a"}})
    claim_hf_run(hub, "run-b", {"config": {"run_id": "run-b"}})
    with pytest.raises(FileExistsError):
        claim_hf_run(hub, "run-a", {"config": {"run_id": "run-a"}})
    assert "SHIFT.json" not in hub.files()
    assert "runs/run-a/MANIFEST.json" in hub.files()
    assert "runs/run-b/MANIFEST.json" in hub.files()

@pytest.mark.parametrize("same_id", [False, True])
def test_hf_claim_handles_concurrent_repo_commit(monkeypatch, same_id):
    import httpx
    from huggingface_hub.errors import HfHubHTTPError

    from src.benchmark.runner import claim_hf_run

    hub = MemoryHub()
    commit = hub.commit
    calls = 0

    def race(files, message="M1 checkpoint", parent=None):
        nonlocal calls
        calls += 1
        if calls == 1:
            name = "run-a" if same_id else "other-run"
            commit({f"runs/{name}/MANIFEST.json": b"{}"})
            response = httpx.Response(409, request=httpx.Request("POST", "https://example.test"))
            raise HfHubHTTPError("conflict", response=response)
        return commit(files, message, parent=parent)

    monkeypatch.setattr(hub, "commit", race)
    if same_id:
        with pytest.raises(FileExistsError):
            claim_hf_run(hub, "run-a", {"config": {"run_id": "run-a"}})
        assert hub.read("runs/run-a/MANIFEST.json") == b"{}"
    else:
        claim_hf_run(hub, "run-a", {"config": {"run_id": "run-a"}})
        assert hub.read("runs/other-run/MANIFEST.json") == b"{}"
        assert hub.read("runs/run-a/MANIFEST.json") is not None
