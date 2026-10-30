"""One resumable full-crawl benchmark, used by the Colab entry notebook.

The parent owns the shift and profiles each child. Children checkpoint only at
completed-version boundaries, within the profiler's measured process tree.
"""

# Drafted with assistance from OpenAI Codex, disclosed per course rules.

from __future__ import annotations

import argparse
import json
import logging
import platform
import re
import subprocess
import sys
import threading
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from importlib.metadata import distributions
from pathlib import Path
from zipfile import ZIP_DEFLATED, ZipFile

from src import config
from src.benchmark.checkpoints import Checkpoints, prepare_drive, write_json
from src.utils.ids import iter_id_range


@dataclass(frozen=True)
class BenchmarkConfig:
    run_id: str
    mode: str = "final"
    ranges: tuple[str, ...] = ("thien", "tien", "hieu", "dang")
    offset: int = 0
    count: int | None = None
    chunk_size: int = 100
    references: bool = True
    checkpoint_s: float = 300.0


@dataclass(frozen=True)
class Chunk:
    owner: str
    offset: int
    ids: tuple[str, ...]


def plan_chunks(cfg: BenchmarkConfig) -> list[Chunk]:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,79}", cfg.run_id):
        raise ValueError("Run ID must be a simple unique name")
    if cfg.mode not in ("sample", "final") or cfg.chunk_size < 1 or cfg.offset < 0:
        raise ValueError("Invalid benchmark selection")
    if cfg.checkpoint_s <= 0 or (cfg.count is not None and cfg.count < 1):
        raise ValueError("Count and checkpoint interval must be positive")
    if (
        not cfg.ranges
        or len(set(cfg.ranges)) != len(cfg.ranges)
        or set(cfg.ranges) - config.RANGES.keys()
    ):
        raise ValueError("Invalid or duplicate ranges")
    if cfg.mode == "final" and (
        set(cfg.ranges) != set(config.RANGES)
        or cfg.offset
        or cfg.count is not None
        or not cfg.references
    ):
        raise ValueError("Final mode needs all four full ranges and references")
    chunks = []
    for owner in cfg.ranges:
        start, end = config.RANGES[owner]
        ids = list(iter_id_range(start, end, config.MONTH_END))[cfg.offset :]
        if cfg.count is not None:
            ids = ids[: cfg.count]
        if not ids:
            raise ValueError("Selection is empty")
        for pos in range(0, len(ids), cfg.chunk_size):
            chunks.append(Chunk(owner, cfg.offset + pos, tuple(ids[pos : pos + cfg.chunk_size])))
    return chunks


def prepare_run(cfg: BenchmarkConfig, root: Path, revision: str, *, resume: bool) -> dict:
    chunks = plan_chunks(cfg)
    identity = {"config": json.loads(json.dumps(asdict(cfg))), "code_revision": revision}
    path = root / cfg.run_id / "MANIFEST.json"
    if resume:
        manifest = json.loads(path.read_text(encoding="utf-8"))
        if any(manifest.get(k) != v for k, v in identity.items()):
            raise ValueError("Run configuration/code revision differs from the saved run")
        return manifest
    if (root / cfg.run_id).exists():
        raise FileExistsError("Run already exists; select resume or a new run ID")
    manifest = dict(
        identity,
        started_at=datetime.now(UTC).isoformat(),
        required_ids=[i for c in chunks for i in c.ids],
    )
    write_json(path, manifest)
    return manifest


class Shift:
    """Advisory Drive lease; group coordination is still required (Drive is not a DB)."""

    def __init__(self, drive: Path, owner: str, *, takeover: bool = False):
        self.path = drive / "SHIFT.json"
        self.owner, self.token = owner, uuid.uuid4().hex
        self.stop = threading.Event()
        self.error: Exception | None = None
        if self.path.exists():
            try:
                old = json.loads(self.path.read_text(encoding="utf-8"))
            except ValueError as exc:
                raise ValueError("Unreadable shift marker: inspect it before takeover") from exc
            if not old.get("released") and not takeover:
                raise RuntimeError(
                    "Another shift may still run; confirm it stopped before takeover"
                )
        self._beat()
        self.thread = threading.Thread(target=self._loop, daemon=True)

    def _beat(self, *, released: bool = False) -> None:
        write_json(
            self.path, dict(owner=self.owner, token=self.token, at=time.time(), released=released)
        )

    def _loop(self) -> None:
        while not self.stop.wait(30):
            try:
                self.check()
                self._beat()
            except Exception as exc:
                self.error = exc
                return

    def check(self) -> None:
        if self.error:
            raise RuntimeError("Shift heartbeat failed; stop crawling") from self.error
        if json.loads(self.path.read_text(encoding="utf-8")).get("token") != self.token:
            raise RuntimeError("Shift ownership changed; stop crawling")

    def close(self, *, release: bool) -> None:
        self.stop.set()
        self.thread.join()
        if release:
            self.check()
            self._beat(released=True)


def coverage(cfg: BenchmarkConfig, local: Path) -> dict:
    """QA over exact requested IDs, not just the most recent chunk."""
    from src.qa.validate import load_versions_log, validate_range, write_report

    ids = [i for c in plan_chunks(cfg) for i in c.ids]
    data, work = local / "data/corpus", local / "work"
    journal = work / "versions.jsonl"
    logs = load_versions_log(journal) if journal.exists() else {}
    source_issues, _ = validate_range(ids, data, owner="benchmark", versions_log=logs)
    ref_issues, _ = validate_range(
        ids, data, owner="benchmark", versions_log=logs, require_references=cfg.references
    )
    write_report(work / "qa_source.csv", source_issues)
    write_report(work / "qa_report.csv", ref_issues)
    source_bad = {i.arxiv_id for i in source_issues if i.severity == "error"}
    all_bad = {i.arxiv_id for i in ref_issues if i.severity == "error"}
    result = dict(
        mode=cfg.mode,
        required_ids=len(ids),
        unique_ids=len(set(ids)),
        papers_source_complete=len(ids) - len(source_bad),
        papers_with_reference_or_source_errors=len(all_bad),
        complete=not all_bad and len(ids) == len(set(ids)),
        final_complete=cfg.mode == "final" and len(ids) == 20_000 and not all_bad,
    )
    write_json(work / "coverage.json", result)
    return result


def _chunk_complete(chunk: Chunk, local: Path, references: bool) -> bool:
    """Skip only chunks that pass the actual corpus/metadata/journal QA."""
    from src.qa.validate import load_versions_log, validate_range

    journal = local / "work/versions.jsonl"
    if not journal.exists():
        return False
    issues, _ = validate_range(
        list(chunk.ids),
        local / "data/corpus",
        owner=chunk.owner,
        versions_log=load_versions_log(journal),
        require_references=references,
    )
    return not any(i.severity == "error" for i in issues)


def run_child(payload: dict) -> None:
    from src.run_pipeline import run

    local, remote = Path(payload["local"]), Path(payload["remote"])
    cp = Checkpoints(local, remote)
    last = time.monotonic()

    def checkpoint(_record):
        nonlocal last
        if time.monotonic() - last < payload["checkpoint_s"]:
            return
        t0 = time.perf_counter()
        rec = cp.save(payload["ids"], phase="periodic", included_in_pipeline=True)
        elapsed = time.perf_counter() - t0
        print(f"Checkpoint verified: {rec['versions']} versions, {elapsed:.1f}s", flush=True)
        last = time.monotonic()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(local / "work/run.log", encoding="utf-8"),
        ],
    )
    run(
        payload["ids"],
        data_root=local / "data/corpus",
        work_dir=local / "work",
        references=payload["references"],
        progress=False,
        label=payload["owner"],
        on_version=checkpoint,
    )


def export_evidence(cfg: BenchmarkConfig, local: Path) -> Path:
    from src.analysis.stats import main as stats

    ids = [i for c in plan_chunks(cfg) for i in c.ids]
    stats(
        [
            "--ids",
            ",".join(ids),
            "--data-root",
            str(local / "data/corpus"),
            "--work-dir",
            str(local / "work"),
            "--out",
            str(local / "book_csv"),
        ]
    )
    path = local / "bench_results.zip"
    with ZipFile(path, "w", ZIP_DEFLATED) as archive:
        for folder in (local / "work", local / "book_csv"):
            for file in folder.rglob("*"):
                if file.is_file() and file.name != "run.lock":
                    archive.write(file, file.relative_to(local).as_posix())
    return path


def timing_audit(local: Path) -> dict:
    """A hard kill can lose the last interval; never invent an exact final runtime."""
    profiles = list((local / "work/profile").glob("*"))
    unfinished = [
        p.name for p in profiles if p.is_dir() and not (p / "profile_summary.json").exists()
    ]
    result = dict(
        timing_complete=bool(profiles) and not unfinished, unfinished_profiles=sorted(unfinished)
    )
    write_json(local / "work/timing_audit.json", result)
    return result


def run_benchmark(
    cfg: BenchmarkConfig,
    *,
    drive_root: Path,
    local_root: Path,
    revision: str,
    team_id: str,
    owner: str,
    resume: bool = False,
    takeover: bool = False,
    session_budget_s: float = 14400,
) -> dict:
    """Run in the notebook's foreground; all real crawl work lives in profiled children."""
    import psutil

    prepare_drive(drive_root, team_id)
    shift = Shift(drive_root, owner, takeover=takeover)
    shift.thread.start()
    local, remote = local_root / cfg.run_id, drive_root / "runs" / cfg.run_id
    safe_to_release = False
    child = None
    session_start = time.perf_counter()
    try:
        manifest = prepare_run(cfg, drive_root / "runs", revision, resume=resume)
        cp = Checkpoints(local, remote)
        if resume:
            cp.restore()  # fresh local destination required; never delete existing data
        elif local.exists():
            raise FileExistsError("Local run directory exists; use a new run ID")
        work = local / "work"
        work.mkdir(parents=True, exist_ok=True)
        write_json(work / "manifest.json", manifest)
        session_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%S%f")
        write_json(
            work / f"sessions/{session_id}.json",
            dict(
                code_revision=revision,
                python=sys.version,
                platform=platform.platform(),
                cpu_count=psutil.cpu_count(),
                ram_bytes=psutil.virtual_memory().total,
                dependencies={d.metadata["Name"]: d.version for d in distributions()},
                started_at=datetime.now(UTC).isoformat(),
            ),
        )
        chunks = plan_chunks(cfg)
        all_ids = [i for c in chunks for i in c.ids]
        # Initial durable manifest/session, even if discovery never succeeds.
        cp.save([], phase="initial")
        last_exit = None
        for chunk in chunks:
            shift.check()
            if time.perf_counter() - session_start >= session_budget_s:
                print(
                    "Session budget reached: checkpoint and resume in a fresh runtime.", flush=True
                )
                break
            if _chunk_complete(chunk, local, cfg.references):
                print(f"Verified complete: {chunk.owner} {chunk.offset}", flush=True)
                continue
            if last_exit is not None:
                time.sleep(max(0.0, 3.1 - (time.monotonic() - last_exit)))
            attempt = f"{chunk.owner}-{chunk.offset:04d}-{uuid.uuid4().hex}"
            prof = work / "profile" / attempt
            prof.mkdir(parents=True)
            payload = dict(
                ids=list(chunk.ids),
                owner=chunk.owner,
                local=str(local),
                remote=str(remote),
                checkpoint_s=cfg.checkpoint_s,
                references=cfg.references,
            )
            payload_path = work / "chunk.json"
            write_json(payload_path, payload)
            write_json(
                prof / "attempt.json",
                dict(
                    attempt=attempt,
                    owner=chunk.owner,
                    ids=list(chunk.ids),
                    planned_at=datetime.now(UTC).isoformat(),
                    state="launch-intent",
                ),
            )
            # Durable intent BEFORE Popen: even a kill before first periodic save
            # must leave evidence of an attempt with potentially missing runtime.
            Checkpoints(local, remote).save(list(chunk.ids), phase="prelaunch")
            cmd = [
                sys.executable,
                "-m",
                "src.utils.profiler",
                "--out",
                str(prof),
                "--watch",
                str(local / "data"),
                "--watch",
                str(work),
                "--output",
                str(local / "data/corpus"),
                "--",
                sys.executable,
                "-m",
                "src.benchmark.runner",
                "--chunk",
                str(payload_path),
            ]
            if Path("/usr/bin/time").is_file():
                cmd = ["/usr/bin/time", "-v", "-o", str(prof / "time_v.log"), *cmd]
            print(f"=== {chunk.owner} offset={chunk.offset} IDs={len(chunk.ids)} ===", flush=True)
            child = subprocess.Popen(cmd, start_new_session=sys.platform != "win32")
            try:
                while child.poll() is None:
                    shift.check()
                    time.sleep(1)
            except BaseException:
                # Stop the entire profiler/crawler process tree before checkpointing.
                try:
                    process = psutil.Process(child.pid)
                    processes = process.children(recursive=True) + [process]
                    for proc in processes:
                        try:
                            proc.terminate()
                        except psutil.NoSuchProcess:
                            pass
                    _, alive = psutil.wait_procs(processes, timeout=10)
                    for proc in alive:
                        try:
                            proc.kill()
                        except psutil.NoSuchProcess:
                            pass
                except psutil.NoSuchProcess:
                    pass
                finally:
                    child.wait()
                    child = None
                raise
            last_exit = time.monotonic()
            code, child = child.returncode, None
            # Child may have published periodic checkpoints; refresh the parent index.
            cp = Checkpoints(local, remote)
            sync_start = time.perf_counter()
            checkpoint = cp.save(list(chunk.ids), phase="post-chunk")
            sync_s = time.perf_counter() - sync_start
            with (work / "attempts.jsonl").open("a", encoding="utf-8") as fh:
                fh.write(
                    json.dumps(
                        dict(
                            attempt=attempt,
                            exit_code=code,
                            sync_s=sync_s,
                            checkpoint=checkpoint["generation"],
                        )
                    )
                    + "\n"
                )
            print(f"Drive checkpoint verified; post-chunk sync={sync_s:.1f}s", flush=True)
            if code:
                raise RuntimeError(f"Chunk exit {code}; inspect work/run.log, then resume")
            if not _chunk_complete(chunk, local, cfg.references):
                raise RuntimeError("Chunk QA incomplete; evidence retained, resume to retry")
        result = coverage(cfg, local)
        result["final_corpus_complete"] = result["final_complete"]
        result.update(timing_audit(local))
        result["final_complete"] = result["final_complete"] and result["timing_complete"]
        write_json(work / "coverage.json", result)
        export_evidence(cfg, local)
        cp.save(all_ids, phase="final-reports")
        safe_to_release = True
        return result
    finally:
        # A hard runtime loss cannot execute this block: only verified checkpoints survive.
        if child is None and local.is_dir() and (local / "work/manifest.json").exists():
            try:
                Checkpoints(local, remote).save(
                    [i for c in plan_chunks(cfg) for i in c.ids], phase="final"
                )
                safe_to_release = True
            except Exception:
                logging.exception("Final checkpoint failed; shift kept active")
                safe_to_release = False
        shift.close(release=safe_to_release)
        if (local / "work/manifest.json").exists() and "session_id" in locals():
            session = dict(
                seconds_through_final_sync=time.perf_counter() - session_start,
                shift_released=safe_to_release,
                ended_at=datetime.now(UTC).isoformat(),
            )
            write_json(local / f"work/session_totals/{session_id}.json", session)
            if safe_to_release:
                write_json(remote / f"sessions/{session_id}.json", session)
            # Include the last checkpoint event and session total in the downloadable ZIP.
            export_evidence(cfg, local)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--chunk", type=Path, required=True)
    args = parser.parse_args()
    run_child(json.loads(args.chunk.read_text(encoding="utf-8")))
