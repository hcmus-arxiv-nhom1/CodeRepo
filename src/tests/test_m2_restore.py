import hashlib
import json

import pytest

from src.benchmark.m2_restore import restore_m2


def make_checkpoint(root):
    corpus = root / "data/corpus/2604-00001/v1/main.tex"
    corpus.parent.mkdir(parents=True)
    corpus.write_bytes(b"tex contents")
    metadata = root / "data/corpus/2604-00001/metadata.json"
    metadata.write_bytes(b"{}")
    work = root / "work"
    work.mkdir()
    (work / "versions.jsonl").write_bytes(b"{}\n")
    (work / "large-log").write_bytes(b"not needed")
    paths = [
        "data/corpus/2604-00001/v1/main.tex",
        "data/corpus/2604-00001/metadata.json",
        "work/versions.jsonl",
        "work/large-log",
    ]
    index = {
        "generation": "g1",
        "directories": ["data/corpus/2604-00001/v1/"],
        "files": {p: hashlib.sha256((root / p).read_bytes()).hexdigest() for p in paths},
    }
    (root / "INDEX.json").write_text(json.dumps(index), encoding="utf8")
    return index


def test_restore_only_copies_parser_inputs_and_verifies(tmp_path):
    remote = tmp_path / "remote"
    index = make_checkpoint(remote)
    local = tmp_path / "local"
    events = []
    result = restore_m2(local, remote, index, workers=2, on_progress=events.append)
    assert result["copied"] == 3
    assert (local / "data/corpus/2604-00001/v1/main.tex").read_bytes() == b"tex contents"
    assert (local / "work/versions.jsonl").is_file()
    assert not (local / "work/large-log").exists()
    assert events[-1]["completed"] == 3


def test_partial_recovery_reuses_valid_and_replaces_corrupt_file(tmp_path):
    remote = tmp_path / "remote"
    index = make_checkpoint(remote)
    local = tmp_path / "local"
    valid = local / "data/corpus/2604-00001/v1/main.tex"
    valid.parent.mkdir(parents=True)
    valid.write_bytes(b"tex contents")
    bad = local / "data/corpus/2604-00001/metadata.json"
    bad.write_bytes(b"bad")
    with pytest.raises(FileExistsError):
        restore_m2(local, remote, index, workers=2)
    result = restore_m2(local, remote, index, workers=2, reuse_partial=True)
    assert result["reused"] == 1 and result["copied"] == 2
    assert bad.read_bytes() == b"{}"


def test_restore_uses_verified_fallback_when_primary_is_bad(tmp_path):
    remote = tmp_path / "remote"
    index = make_checkpoint(remote)
    name = "data/corpus/2604-00001/v1/main.tex"
    sha = index["files"][name]
    backup = remote / ".checkpoint-objects" / sha[:2] / sha
    backup.parent.mkdir(parents=True)
    backup.write_bytes(b"tex contents")
    (remote / name).write_bytes(b"incorrect")
    result = restore_m2(tmp_path / "local", remote, index, workers=2)
    assert result["fallback"] == 1


def test_partial_recovery_rejects_unexpected_corpus_file(tmp_path):
    remote = tmp_path / "remote"
    index = make_checkpoint(remote)
    local = tmp_path / "local"
    extra = local / "data/corpus/evil.tex"
    extra.parent.mkdir(parents=True)
    extra.write_text("extra")
    with pytest.raises(ValueError, match="extra"):
        restore_m2(local, remote, index, workers=2, reuse_partial=True)
    assert extra.read_text() == "extra"


def test_rejects_checkpoint_path_traversal(tmp_path):
    remote = tmp_path / "remote"
    index = make_checkpoint(remote)
    index["files"]["data/corpus/../../evil.tex"] = "a" * 64
    (remote / "INDEX.json").write_text(json.dumps(index), encoding="utf8")
    with pytest.raises(ValueError, match="Unsafe"):
        restore_m2(tmp_path / "local", remote, index)


def test_failed_restore_keeps_existing_corrupt_file_and_no_marker(tmp_path):
    remote = tmp_path / "remote"
    index = make_checkpoint(remote)
    name = "data/corpus/2604-00001/v1/main.tex"
    (remote / name).write_bytes(b"bad remote")
    local = tmp_path / "local"
    file = local / name
    file.parent.mkdir(parents=True)
    file.write_bytes(b"original partial")
    with pytest.raises(ValueError, match="mismatch"):
        restore_m2(local, remote, index, reuse_partial=True, workers=1)
    assert file.read_bytes() == b"original partial"
    assert not (local / ".m2-restore.json").exists()


def test_rejects_changed_index_for_existing_restore(tmp_path):
    remote = tmp_path / "remote"
    index = make_checkpoint(remote)
    local = tmp_path / "local"
    restore_m2(local, remote, index)
    changed = dict(index, generation="g2")
    (remote / "INDEX.json").write_text(json.dumps(changed), encoding="utf8")
    with pytest.raises(ValueError, match="another checkpoint"):
        restore_m2(local, remote, changed)


def test_verified_primary_is_read_once_from_source(tmp_path, monkeypatch):
    from pathlib import Path

    remote = tmp_path / "remote"
    index = make_checkpoint(remote)
    target = remote / "data/corpus/2604-00001/v1/main.tex"
    original = Path.open
    seen = []

    def observe(path, *args, **kwargs):
        if path == target and args and args[0] == "rb":
            seen.append(path)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "open", observe)
    restore_m2(tmp_path / "local", remote, index, workers=1)
    assert len(seen) == 1


def test_legacy_rescue_restores_all_indexed_files_and_a_compatible_head(tmp_path):
    from src.benchmark.m2_inputs import verify_restored_checkpoint

    remote = tmp_path / "remote"
    index = make_checkpoint(remote)
    local = tmp_path / "local"
    result = restore_m2(local, remote, index, workers=2, legacy_checkpoint=True)
    assert result["copied"] == 4
    assert (local / "work/large-log").read_bytes() == b"not needed"
    verify_restored_checkpoint(local, index)


def test_legacy_rescue_cli_supports_explicit_partial_reuse(tmp_path):
    import subprocess
    import sys

    remote = tmp_path / "remote"
    make_checkpoint(remote)
    pinned = hashlib.sha256((remote / "INDEX.json").read_bytes()).hexdigest()
    local = tmp_path / "local"
    existing = local / "data/corpus/2604-00001/v1/main.tex"
    existing.parent.mkdir(parents=True)
    existing.write_bytes(b"tex contents")
    result = subprocess.run(
        [
            sys.executable,
            "-m",
            "src.benchmark.m2_restore",
            "--remote",
            str(remote),
            "--local",
            str(local),
            "--pinned-index-sha256",
            pinned,
            "--workers",
            "2",
            "--reuse-partial",
            "--legacy-checkpoint",
        ],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert '"reused": 1' in result.stdout
    assert (local / ".checkpoint-head.json").is_file()


def test_restore_refuses_wrong_pinned_index_before_writing(tmp_path):
    remote = tmp_path / "remote"
    index = make_checkpoint(remote)
    local = tmp_path / "local"
    with pytest.raises(ValueError, match="pinned hash"):
        restore_m2(local, remote, index, expected_index_sha256="0" * 64)
    assert not local.exists()


def test_restore_rejects_unsafe_worker_count(tmp_path):
    remote = tmp_path / "remote"
    index = make_checkpoint(remote)
    with pytest.raises(ValueError, match="workers"):
        restore_m2(tmp_path / "local", remote, index, workers=33)


@pytest.mark.parametrize("local_kind", ["same", "inside", "parent"])
def test_restore_rejects_overlapping_local_and_remote_roots(tmp_path, local_kind):
    remote = tmp_path / "remote"
    index = make_checkpoint(remote)
    local = {"same": remote, "inside": remote / "nested", "parent": tmp_path}[local_kind]
    with pytest.raises(ValueError, match="overlap"):
        restore_m2(local, remote, index, reuse_partial=True)
    assert not (remote / ".m2-restore.json").exists()


def test_rejects_noncanonical_duplicate_destination(tmp_path):
    remote = tmp_path / "remote"
    index = make_checkpoint(remote)
    index["files"]["data/corpus/2604-00001/v1/./main.tex"] = "a" * 64
    (remote / "INDEX.json").write_text(json.dumps(index), encoding="utf8")
    with pytest.raises(ValueError, match="canonical|duplicate"):
        restore_m2(tmp_path / "local", remote, index)


def test_restore_reports_preparation_phases(tmp_path):
    remote = tmp_path / "remote"
    index = make_checkpoint(remote)
    phases = []
    restore_m2(tmp_path / "local", remote, index, on_phase=phases.append)
    assert phases == [
        "validating-index",
        "scanning-existing-local",
        "creating-directories",
        "copy-and-verify",
    ]


def test_restore_rejects_path_alias_to_remote(tmp_path):
    remote = tmp_path / "remote"
    index = make_checkpoint(remote)
    alias = tmp_path / "alias" / ".." / "remote"
    with pytest.raises(ValueError, match="overlap"):
        restore_m2(alias, remote, index, reuse_partial=True)


def test_restore_rejects_symlink_alias_to_remote(tmp_path):
    remote = tmp_path / "remote"
    index = make_checkpoint(remote)
    alias = tmp_path / "alias"
    try:
        alias.symlink_to(remote, target_is_directory=True)
    except OSError:
        pytest.skip("filesystem does not permit symlink creation")
    with pytest.raises(ValueError, match="overlap"):
        restore_m2(alias / "nested", remote, index, reuse_partial=True)


def test_partial_recovery_rejects_unindexed_empty_version_directory(tmp_path):
    remote = tmp_path / "remote"
    index = make_checkpoint(remote)
    local = tmp_path / "local"
    extra = local / "data/corpus/2604-00001/v2"
    extra.mkdir(parents=True)
    with pytest.raises(ValueError, match="extra"):
        restore_m2(local, remote, index, reuse_partial=True)
    assert extra.is_dir()
