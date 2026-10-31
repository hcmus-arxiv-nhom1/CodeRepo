"""Verified, append-only Drive checkpoints. No live extraction directory is copied."""

# Drafted with assistance from OpenAI Codex, disclosed per course rules.

from __future__ import annotations

import hashlib
import json
import shutil
import time
import uuid
from datetime import UTC, datetime
from pathlib import Path, PurePosixPath
from zipfile import ZIP_DEFLATED, ZipFile

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
    """Immutable zip deltas plus a completion record written after read-back verification.

    Completion records contain hashes of changed files and the parent generation.
    Restore validates the complete chain before touching the local run directory.
    Drive markers are not claimed to be an atomic distributed transaction/lock.
    """

    def __init__(self, local: Path, remote: Path):
        self.local, self.remote = Path(local), Path(remote)
        self.folder = self.remote / "checkpoints"
        self.folder.mkdir(parents=True, exist_ok=True)
        self.parent = None
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

    def _records(self) -> dict[str, dict]:
        records = {}
        for path in sorted(self.folder.glob("*.json")):
            try:
                rec = json.loads(path.read_text(encoding="utf-8"))
                if rec["generation"] == path.stem and rec["archive"] == path.stem + ".zip":
                    records[path.stem] = rec
            except (ValueError, KeyError, OSError):
                continue  # interrupted completion record
        return records

    def _chain(self, records: dict, head: str) -> list[dict]:
        chain, seen = [], set()
        while head is not None:
            if head in seen or head not in records:
                raise ValueError("Broken checkpoint chain")
            seen.add(head)
            rec = records[head]
            chain.append(rec)
            head = rec["parent"]
        return list(reversed(chain))

    def _load_index(self, chain: list[dict]) -> None:
        self.hashes, self.directories = {}, set()
        for rec in chain:
            self.hashes.update(rec["files"])
            self.directories.update(rec["directories"])
        self.parent = chain[-1]["generation"] if chain else None
        self._loaded = True

    def _verify(self, path: Path, expected: str) -> bool:
        return digest(path) == expected

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
            records = self._records()
            head = self.local / ".checkpoint-head.json"
            if records and not head.exists():
                raise ValueError("Restore a verified checkpoint before saving into this run")
            generation = json.loads(head.read_text())["generation"] if records else None
            self._load_index(self._chain(records, generation) if records else [])
        state = load_version_state(self.local / "work/versions.jsonl")
        candidates: dict[str, bytes | Path] = {}
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
        newdirs = sorted(directories - self.directories)
        files = {}
        generation = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f") + "-" + uuid.uuid4().hex[:8]
        tmp = self.local / "work/tmp/checkpoints" / (generation + ".zip")
        tmp.parent.mkdir(parents=True, exist_ok=True)
        try:
            with ZipFile(tmp, "w", ZIP_DEFLATED) as archive:
                for name in newdirs:
                    archive.writestr(name, b"")
                for name, content in candidates.items():
                    # Snapshot one mutable journal/JSON at a time; never buffer the corpus.
                    if isinstance(content, Path) and (
                        name.startswith("work/") or name.endswith(".json")
                    ):
                        content = content.read_bytes()
                        if name.startswith("data/"):
                            try:
                                json.loads(content)
                            except ValueError:
                                continue
                    sha = (
                        digest(content)
                        if isinstance(content, Path)
                        else hashlib.sha256(content).hexdigest()
                    )
                    if self.hashes.get(name) == sha:
                        continue
                    files[name] = sha
                    if isinstance(content, Path):
                        archive.write(content, name)
                    else:
                        archive.writestr(name, content)
            expected = digest(tmp)
            destination = self.folder / tmp.name
            shutil.copyfile(tmp, destination)
            if not self._verify(destination, expected):
                raise OSError("Drive checkpoint verification failed; previous checkpoint retained")
            rec = dict(
                generation=generation,
                parent=self.parent,
                archive=tmp.name,
                sha256=expected,
                bytes=destination.stat().st_size,
                files=files,
                directories=newdirs,
                versions=versions,
                committed_at=datetime.now(UTC).isoformat(),
            )
            # Publish last. A torn JSON record is ignored on restore.
            (self.folder / (generation + ".json")).write_text(json.dumps(rec), encoding="utf-8")
            if (
                json.loads((self.folder / (generation + ".json")).read_text(encoding="utf-8"))
                != rec
            ):
                raise OSError("Checkpoint completion record could not be verified")
            self.hashes.update(files)
            self.directories.update(newdirs)
            self.parent = generation
            write_json(self.local / ".checkpoint-head.json", {"generation": generation})
            return rec
        finally:
            tmp.unlink(missing_ok=True)

    def restore(self) -> dict:
        records = self._records()
        if not records:
            raise ValueError("No completed checkpoint available")
        chain = None
        verified = set()
        for head in sorted(records, reverse=True):
            try:
                attempt = self._chain(records, head)
                for rec in attempt:
                    if rec["generation"] not in verified:
                        if not self._verify(self.folder / rec["archive"], rec["sha256"]):
                            raise ValueError("Checkpoint checksum mismatch")
                        verified.add(rec["generation"])
                chain = attempt
                break
            except (OSError, ValueError, KeyError):
                continue
        if chain is None:
            raise ValueError("No valid checkpoint chain remains")
        # Refuse merging restored state with arbitrary/stale local data.
        if self.local.exists() and any(self.local.iterdir()):
            raise FileExistsError("Restore needs a fresh local directory")
        self.local.mkdir(parents=True, exist_ok=True)
        for rec in chain:
            with ZipFile(self.folder / rec["archive"]) as archive:
                for name in archive.namelist():
                    if not self.safe_name(name):
                        raise ValueError("Unsafe checkpoint member")
                archive.extractall(self.local)  # names checked; archives generated by this class
        self._load_index(chain)
        # File hashes validate restoration, including later work-log replacements.
        for name, expected in self.hashes.items():
            if digest(self.local / name) != expected:
                raise ValueError(f"Restored file mismatch: {name}")
        write_json(self.local / ".checkpoint-head.json", {"generation": self.parent})
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
        return chain[-1]
