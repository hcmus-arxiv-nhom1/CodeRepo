"""Bounded, verified M2 restore with safe reuse of existing local corpus files."""

from __future__ import annotations

import hashlib
import itertools
import json
import os
import uuid
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from pathlib import Path, PurePosixPath

from src.benchmark.checkpoints import Checkpoints


def _hash(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def _safe_local(root: Path, relative: str) -> Path:
    if not Checkpoints.safe_name(relative):
        raise ValueError(f"Unsafe checkpoint path: {relative}")
    target = root / relative
    current = target
    while current != root:
        if current.is_symlink():
            raise ValueError(f"Symlink in restore path: {current}")
        current = current.parent
    return target


def restore_m2(
    local: Path,
    remote: Path,
    index: dict,
    *,
    workers: int = 8,
    reuse_partial: bool = False,
    on_progress=None,
    on_phase=None,
    expected_index_sha256: str | None = None,
    legacy_checkpoint: bool = False,
) -> dict:
    """Restore corpus plus version journal; reuse only files verified against the index."""
    local, remote = Path(local), Path(remote)
    if workers < 1 or workers > 32:
        raise ValueError("Restore workers must be between 1 and 32")
    if local.is_symlink():
        raise ValueError("Restore root may not be a symlink")
    local, remote = local.resolve(), remote.resolve()
    if local == remote or local.is_relative_to(remote) or remote.is_relative_to(local):
        raise ValueError("Local and remote restore roots overlap")
    index_path = remote / "INDEX.json"
    original = index_path.read_bytes()
    if json.loads(original) != index:
        raise ValueError("Checkpoint index changed before restore")
    index_hash = hashlib.sha256(original).hexdigest()
    if expected_index_sha256 is not None and index_hash != expected_index_sha256:
        raise ValueError("Checkpoint index does not match the pinned hash")
    marker = local / ".m2-restore.json"
    if marker.exists():
        if marker.is_symlink() or json.loads(marker.read_text(encoding="utf8")) != {
            "index_sha256": index_hash,
            "scope": "m2-corpus-v1",
        }:
            raise ValueError("Local M2 restore belongs to another checkpoint")
    elif local.exists() and any(local.iterdir()) and not reuse_partial:
        raise FileExistsError("Partial local restore exists; explicit reuse_partial is required")
    old_head = local / ".checkpoint-head.json"
    old_repairs = set()
    if old_head.is_symlink():
        raise ValueError("Checkpoint head may not be a symlink")
    if old_head.exists():
        old = json.loads(old_head.read_text(encoding="utf8"))
        if any(old.get(key) != index.get(key) for key in ("generation", "files", "directories")):
            raise ValueError("Local checkpoint head differs from pinned index")
        old_repairs = set(old.get("repairs", []))
    indexed = index["files"]
    if "work/versions.jsonl" not in indexed:
        raise ValueError("Checkpoint lacks work/versions.jsonl")
    selected = {
        name: expected
        for name, expected in indexed.items()
        if legacy_checkpoint or name.startswith("data/corpus/") or name == "work/versions.jsonl"
    }
    if on_phase:
        on_phase("validating-index")
    destinations = set()
    for name, expected in selected.items():
        if PurePosixPath(name).as_posix() != name:
            raise ValueError(f"Non-canonical checkpoint file path: {name}")
        target = _safe_local(local, name)
        key = os.path.normcase(str(target))
        if key in destinations:
            raise ValueError(f"Duplicate checkpoint destination: {name}")
        destinations.add(key)
        if len(expected) != 64 or any(c not in "0123456789abcdef" for c in expected):
            raise ValueError(f"Invalid checkpoint hash: {name}")
    expected_dirs = set()
    corpus_prefix = PurePosixPath("data/corpus")
    for name in itertools.chain(selected, index["directories"]):
        path = PurePosixPath(name)
        if not path.is_relative_to(corpus_prefix):
            continue
        parent = path if name.endswith("/") else path.parent
        while parent != corpus_prefix and parent.is_relative_to(corpus_prefix):
            expected_dirs.add(parent.as_posix())
            parent = parent.parent
    if on_phase:
        on_phase("scanning-existing-local")
    corpus = local / "data/corpus"
    if corpus.exists():
        for path in corpus.rglob("*"):
            if path.is_symlink():
                raise ValueError(f"Symlink in local corpus: {path}")
            relative = path.relative_to(local).as_posix()
            if path.is_file() and relative not in selected:
                raise ValueError(f"Unexpected extra corpus file: {path}")
            if path.is_dir() and relative not in expected_dirs:
                raise ValueError(f"Unexpected extra corpus directory: {path}")
    if on_phase:
        on_phase("creating-directories")
    local.mkdir(parents=True, exist_ok=True)
    for name in index["directories"]:
        if not Checkpoints.safe_name(name):
            raise ValueError(f"Unsafe checkpoint directory: {name}")
        if legacy_checkpoint or name.startswith("data/corpus/"):
            _safe_local(local, name).mkdir(parents=True, exist_ok=True)
    temporary = local / ".m2-restore-temp"
    if temporary.is_symlink():
        raise ValueError("Unsafe restore temporary directory")
    temporary.mkdir(exist_ok=True)

    def copy_one(item):
        name, expected = item
        destination = _safe_local(local, name)
        if destination.is_file() and _hash(destination) == expected:
            return "reused", name
        if destination.exists() and not destination.is_file():
            raise ValueError(f"Restore destination is not a file: {name}")
        destination.parent.mkdir(parents=True, exist_ok=True)
        temp = temporary / uuid.uuid4().hex
        primary = remote / name
        backup = remote / ".checkpoint-objects" / expected[:2] / expected
        try:
            for source, status in ((primary, "copied"), (backup, "fallback")):
                digest = hashlib.sha256()
                try:
                    with source.open("rb") as src, temp.open("wb") as dst:
                        while chunk := src.read(1024 * 1024):
                            dst.write(chunk)
                            digest.update(chunk)
                except OSError:
                    continue
                if digest.hexdigest() == expected:
                    if _hash(temp) != expected:
                        raise ValueError(f"Local copy hash mismatch: {name}")
                    os.replace(temp, destination)
                    return status, name
            raise ValueError(f"Restored file mismatch: {name}")
        finally:
            temp.unlink(missing_ok=True)

    result = {"copied": 0, "reused": 0, "fallback": 0, "total": len(selected)}
    repairs = []
    if on_phase:
        on_phase("copy-and-verify")
    items = iter(selected.items())
    with ThreadPoolExecutor(max_workers=workers) as pool:
        pending = {pool.submit(copy_one, item) for item in itertools.islice(items, workers * 8)}
        while pending:
            completed, pending = wait(pending, return_when=FIRST_COMPLETED)
            for future in completed:
                status, name = future.result()
                if status == "fallback":
                    repairs.append(name)
                    result["fallback"] += 1
                    result["copied"] += 1
                else:
                    result[status] += 1
                if on_progress:
                    on_progress({**result, "completed": result["copied"] + result["reused"]})
                next_item = next(items, None)
                if next_item is not None:
                    pending.add(pool.submit(copy_one, next_item))
    if hashlib.sha256(index_path.read_bytes()).hexdigest() != index_hash:
        raise ValueError("Checkpoint index changed during restore")
    if legacy_checkpoint:
        head = {
            "generation": index["generation"],
            "files": index["files"],
            "directories": index["directories"],
            "repairs": sorted(old_repairs | set(repairs)),
        }
        temp_head = temporary / uuid.uuid4().hex
        temp_head.write_text(json.dumps(head, indent=2), encoding="utf8")
        os.replace(temp_head, old_head)
    temp_marker = temporary / uuid.uuid4().hex
    temp_marker.write_text(
        json.dumps({"index_sha256": index_hash, "scope": "m2-corpus-v1"}), encoding="utf8"
    )
    os.replace(temp_marker, marker)
    return result


def main(argv: list[str] | None = None) -> int:
    """Restore one checkpoint in a separate process without replacing live notebook modules."""
    import argparse

    from tqdm import tqdm

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--remote", type=Path, required=True)
    parser.add_argument("--local", type=Path, required=True)
    parser.add_argument("--pinned-index-sha256", required=True)
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--reuse-partial", action="store_true")
    parser.add_argument("--legacy-checkpoint", action="store_true")
    args = parser.parse_args(argv)
    index = json.loads((args.remote / "INDEX.json").read_bytes())
    total = sum(
        args.legacy_checkpoint or name.startswith("data/corpus/") or name == "work/versions.jsonl"
        for name in index["files"]
    )
    with tqdm(total=total, desc="Restore", unit="file", mininterval=0) as bar:

        def progress(event):
            pending = event["completed"] - bar.n
            if pending >= 25 or event["completed"] == event["total"]:
                bar.update(pending)

        result = restore_m2(
            args.local,
            args.remote,
            index,
            workers=args.workers,
            reuse_partial=args.reuse_partial,
            legacy_checkpoint=args.legacy_checkpoint,
            on_progress=progress,
            on_phase=lambda phase: tqdm.write(f"Restore phase: {phase}"),
            expected_index_sha256=args.pinned_index_sha256,
        )
    print(json.dumps(result))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
