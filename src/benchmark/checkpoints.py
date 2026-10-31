"""Verified, raw-folder Drive checkpoints. No live extraction directory is copied.

Every completed version/metadata/journal file is mirrored byte-for-byte under
the run's remote folder (not zipped); M2 can read the Drive corpus directly.
`INDEX.json` is the single, atomically-replaced pointer to the current
mirrored state, written only after every file in this save has been copied
and read back verified.
"""

# Drafted with assistance from OpenAI Codex, disclosed per course rules.

from __future__ import annotations

import hashlib
import json
import shutil
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath

from src.run_pipeline import FINAL_STATUSES, load_version_state
from src.utils.ids import to_folder_name


def digest(path: Path) -> str:
    with path.open("rb") as fh:
        return hashlib.file_digest(fh, "sha256").hexdigest()


def write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(value, indent=2), encoding="utf-8")
    tmp.replace(path)


def prepare_drive(root: Path, team_id: str, *, initialize: bool = False) -> None:
    """Initialization is explicit; never silently create a lookalike team folder."""
    if not root.is_dir():
        raise FileNotFoundError(f"Create the intended Drive folder first: {root}")
    marker = root / "TEAM.json"
    if not marker.exists():
        if not initialize:
            raise ValueError("TEAM.json missing: initialize the intended folder explicitly")
        write_json(marker, {"team_id": team_id})
    if json.loads(marker.read_text(encoding="utf-8")).get("team_id") != team_id:
        raise ValueError("Wrong team Drive folder")
    probe = root / f".write-probe-{uuid.uuid4().hex}"
    try:
        probe.write_text(team_id, encoding="utf-8")
        if probe.read_text(encoding="utf-8") != team_id:
            raise OSError("Drive write/read preflight failed")
    finally:
        probe.unlink(missing_ok=True)


class Checkpoints:
    """A plain mirror of the run's raw corpus/work tree, kept on Drive as real folders.

    Each file is copied through a sibling temp name and verified by read-back hash
    before the atomic rename, so a kill mid-copy never corrupts a previously
    committed file. `INDEX.json` (the manifest of every mirrored file's hash) is
    replaced last; a kill between file copies just leaves it pointing at the
    previous, still-valid, generation.
    """

    def __init__(self, local: Path, remote: Path):
        self.local, self.remote = Path(local), Path(remote)
        self.remote.mkdir(parents=True, exist_ok=True)
        self.generation = None
        self.hashes: dict[str, str] = {}
        self.directories: set[str] = set()
        self._loaded = False

    @staticmethod
    def safe_name(name: str) -> bool:
        path = PurePosixPath(name)
        return (
            bool(name)
            and not path.is_absolute()
            and ".." not in path.parts
            and ":" not in name
            and "\\" not in name
        )

    def _verify(self, path: Path, expected: str) -> bool:
        return digest(path) == expected

    def _load_index(self) -> None:
        index_path = self.remote / "INDEX.json"
        head_path = self.local / ".checkpoint-head.json"
        if index_path.exists() and not head_path.exists():
            raise ValueError("Restore a verified checkpoint before saving into this run")
        if head_path.exists():
            head = json.loads(head_path.read_text(encoding="utf-8"))
            self.generation = head["generation"]
            self.hashes = dict(head["files"])
            self.directories = set(head["directories"])
        self._loaded = True

    def save(
        self, ids: list[str], *, phase: str = "checkpoint", included_in_pipeline: bool = False
    ) -> dict:
        started = time.perf_counter()
        rec = self._save(ids)
        event = dict(
            generation=rec["generation"],
            seconds=time.perf_counter() - started,
            bytes=rec["bytes"],
            versions=rec["versions"],
            phase=phase,
            included_in_pipeline=included_in_pipeline,
        )
        with (self.local / "work/checkpoint_events.jsonl").open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(event) + "\n")
        # Independent event records preserve final checkpoint costs after session handoff.
        path = self.remote / "checkpoint-events" / (rec["generation"] + ".json")
        write_json(path, event)
        if json.loads(path.read_text(encoding="utf-8")) != event:
            raise OSError("Checkpoint timing event could not be read back")
        return rec

    def _save(self, ids: list[str]) -> dict:
        if not self._loaded:
            self._load_index()
        state = load_version_state(self.local / "work/versions.jsonl")
        candidates: dict[str, Path] = {}
        directories = set()
        versions = 0
        for rec in state.values():
            if rec.get("status") not in FINAL_STATUSES:
                continue
            rel = f"data/corpus/{to_folder_name(rec['arxiv_id'])}/v{rec['version']}"
            directory = self.local / rel
            if not directory.is_dir():
                raise ValueError(f"Final version directory missing: {rel}")
            versions += 1
            directories.add(rel + "/")
            # Journal paths are untrusted even if originally derived from an archive.
            for name in rec.get("files", []):
                target = f"{rel}/{name}"
                if not self.safe_name(target) or not (self.local / target).is_file():
                    raise ValueError(f"Final source file missing/unsafe: {target}")
                # Completed sources are immutable; hash only files not committed yet.
                if target not in self.hashes:
                    candidates[target] = self.local / target
        for arxiv_id in ids:
            for filename in ("metadata.json", "references.json"):
                name = f"data/corpus/{to_folder_name(arxiv_id)}/{filename}"
                path = self.local / name
                if path.is_file():
                    candidates[name] = path
        work = self.local / "work"
        if work.exists():
            for path in work.rglob("*"):
                if (
                    path.is_file()
                    and path.name != "run.lock"
                    and "tmp" not in path.relative_to(work).parts
                ):
                    candidates[path.relative_to(self.local).as_posix()] = path
        for rel in directories:
            (self.remote / rel).mkdir(parents=True, exist_ok=True)
        files = dict(self.hashes)
        copied_bytes = 0
        for name, path in candidates.items():
            if name.startswith("data/") and name.endswith(".json"):
                try:
                    json.loads(path.read_bytes())
                except ValueError:
                    continue  # torn metadata/references write; retry at the next checkpoint
            sha = digest(path)
            if self.hashes.get(name) == sha:
                files[name] = sha
                continue
            destination = self.remote / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            tmp = destination.parent / f"{destination.name}.tmp-{uuid.uuid4().hex[:8]}"
            try:
                shutil.copyfile(path, tmp)
                if not self._verify(tmp, sha):
                    raise OSError(f"Drive checkpoint verification failed: {name}")
                tmp.replace(destination)
            finally:
                tmp.unlink(missing_ok=True)
            files[name] = sha
            copied_bytes += path.stat().st_size
        generation = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f") + "-" + uuid.uuid4().hex[:8]
        rec = dict(
            generation=generation,
            files=files,
            directories=sorted(self.directories | directories),
            versions=versions,
            bytes=copied_bytes,
            committed_at=datetime.now(UTC).isoformat(),
        )
        # Publish last: a torn/partial write here still leaves the previous INDEX valid.
        write_json(self.remote / "INDEX.json", rec)
        if json.loads((self.remote / "INDEX.json").read_text(encoding="utf-8")) != rec:
            raise OSError("Checkpoint index could not be verified")
        self.generation, self.hashes, self.directories = generation, files, set(rec["directories"])
        write_json(
            self.local / ".checkpoint-head.json",
            dict(generation=generation, files=files, directories=rec["directories"]),
        )
        return rec

    def restore(self) -> dict:
        index_path = self.remote / "INDEX.json"
        if not index_path.exists():
            raise ValueError("No completed checkpoint available")
        index = json.loads(index_path.read_text(encoding="utf-8"))
        # Refuse merging restored state with arbitrary/stale local data.
        if self.local.exists() and any(self.local.iterdir()):
            raise FileExistsError("Restore needs a fresh local directory")
        self.local.mkdir(parents=True, exist_ok=True)
        for rel in index["directories"]:
            if not self.safe_name(rel):
                raise ValueError("Unsafe checkpoint member")
            (self.local / rel).mkdir(parents=True, exist_ok=True)
        for name, expected in index["files"].items():
            if not self.safe_name(name):
                raise ValueError("Unsafe checkpoint member")
            source = self.remote / name
            if not source.is_file() or not self._verify(source, expected):
                raise ValueError(f"Restored file mismatch: {name}")
            destination = self.local / name
            destination.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(source, destination)
        self.generation, self.hashes, self.directories = (
            index["generation"],
            dict(index["files"]),
            set(index["directories"]),
        )
        write_json(
            self.local / ".checkpoint-head.json",
            dict(generation=self.generation, files=self.hashes, directories=index["directories"]),
        )
        self._loaded = True
        # The latest duration event is published after its data checkpoint.
        events = []
        for path in sorted((self.remote / "checkpoint-events").glob("*.json")):
            try:
                events.append(json.loads(path.read_text(encoding="utf-8")))
            except (ValueError, OSError):
                continue
        if events:
            (self.local / "work/checkpoint_events.jsonl").write_text(
                "".join(json.dumps(e) + "\n" for e in events), encoding="utf-8"
            )
        for path in (self.remote / "sessions").glob("*.json"):
            try:
                session = json.loads(path.read_text(encoding="utf-8"))
            except (ValueError, OSError):
                continue
            write_json(self.local / "work/session_totals" / path.name, session)
        from src.benchmark.measurements import collect_measurements

        collect_measurements(self.local, self.remote)
        return index
