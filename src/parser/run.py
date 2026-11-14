# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""Run the whole M2 parser over a corpus with one command; stop it and run it again.

    python -m src.parser.run --data-root data/corpus --out-dir data/m2 --work-dir work/m2
    python -m src.parser.run --data-root data/corpus --out-dir data/m2 --limit 20
    python -m src.parser.run --data-root data/corpus --workers 2 --shard-papers 100
    python -m src.parser.run --data-root data/corpus --scan          # gather + references only

Per paper (papers are independent, deduplication is per publication) and per version:
``gather`` -> ``expand`` -> ``normalize`` -> ``build_tree`` and ``extract_references``; then
``dedup`` over all versions of the paper. The stages are plain functions named in ``STAGES``
(``module:function``, see ``src/parser/model.py`` for the signatures); ``--stage
build_tree=pkg.mod:fn`` swaps one. Nothing is written until every stage can be imported.

Versions without TeX (statement: "record it as empty, do not skip it"):
- ``empty``    the ``vN/`` folder has no ``.tex`` (PDF-only, 404 or empty record of M1);
- ``no_root``  ``.tex`` files but no ``\\documentclass``;
- ``missing``  no ``vN/`` folder although ``metadata.json`` says the version exists (crawl
  failed or not finished): reported, not passed to ``dedup``;
- ``error``    a stage raised; the other versions of the paper are still parsed.
Each is handed to ``dedup`` as a ``VersionParse`` without tree (except ``missing``), so the tables
can list the version; the journal keeps the status for the coverage statistics.

Resume. Papers are processed in folder order and written in *shards* of ``--shard-papers``
papers (or ``--shard-rows`` rows, whichever comes first). A shard is written to
``<out-dir>/.staging/``, renamed to ``<out-dir>/shard-NNNNN/`` and only then appended to
``<work-dir>/shards.jsonl`` (flushed and fsynced): that line is the commit. After a kill,
the same command skips every committed paper, deletes the half-written shard and goes on, so
at most one shard is lost. To parse a *changed* corpus (the crawl went on), use a new
``--out-dir``: committed rows are never rewritten (``--verify-done`` stops with an error if
a committed paper changed on disk, ``--retry-errors`` re-runs papers that failed and have no rows).

Outputs:
    <out-dir>/shard-NNNNN/…       whatever ``write_tables`` writes (Parquet: Hiếu)
    <work-dir>/shards.jsonl       commit journal, one line per shard with a record per paper
    <work-dir>/m2_versions.csv    one row per version: status, root, files, references (for stats)
    <work-dir>/m2_papers.csv      one row per paper: outcome and rows written
    <work-dir>/run_summary.json   counts and timings of the last run (runs.jsonl keeps all)
    <work-dir>/run.log, run.lock

A distributed crawl (the group's five runs) is parsed as one corpus: repeat ``--data-root``
(and ``--crawl-work-dir``, one per run). A paper found in two runs stops the run; with
``--expect-assigned`` it also stops unless the papers are exactly the 20,000 assigned IDs
(``<work-dir>/assigned_check.json`` lists what is missing). Exit codes: 0 done, 2 another run
uses the work directory, 3 a stage is missing, 4 ``--verify-done`` found a changed paper, 5 the
corpus is not the assigned IDs or a paper is in two runs, 130 interrupted.

``--workers N`` runs N processes over the papers (results are written in folder order by the
parent, so shards do not depend on N). ``--paper-timeout`` (POSIX only) turns a paper that
runs too long into an ``error`` instead of stalling the run.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib
import itertools
import json
import logging
import os
import shutil
import signal
import sys
import threading
import time
from collections import Counter, deque
from collections.abc import Callable, Iterator
from concurrent.futures import ProcessPoolExecutor
from contextlib import contextmanager, suppress
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from pathlib import Path

from tqdm import tqdm

from src import config
from src.crawler.worklock import WorkDirLock, WorkDirLockedError
from src.parser.bibitem import BBLIntegrityError, extract_references
from src.parser.gather import TexVersion
from src.parser.model import (
    EMPTY,
    ERROR,
    MISSING,
    NO_ROOT,
    PARSED,
    Rows,
    VersionParse,
    count_rows,
    empty_rows,
    extend_rows,
)
from src.utils.ids import from_folder_name, to_folder_name

log = logging.getLogger("src.parser.run")

STAGES = {
    "normalize": "src.parser.normalize:normalize",
    "build_tree": "src.parser.hierarchy:build_tree",
    "dedup": "src.parser.dedup:dedup",
    "write_tables": "src.parser.writer:write_tables",
}
SHARD_DIR = "shard-{:05d}"
STAGING = ".staging"
RECYCLE_AFTER = 500  # papers per worker process before it is replaced (bounds memory growth)
VERSION_COLUMNS = (
    "arxiv_id version status reason crawl_status root n_files n_unused n_other_roots "
    "n_missing_includes n_bibitems n_bbl n_bib_cited n_bib_unused missing_bib n_nodes n_refs secs"
).split()
PAPER_COLUMNS = (
    "arxiv_id shard n_versions n_parsed n_empty n_no_root n_missing n_error error "
    "elements hierarchy bibtex elements_before bibtex_before elapsed_s"
).split()


class StageError(RuntimeError):
    """A stage cannot be imported."""


class ChangedCorpusError(RuntimeError):
    """A paper committed earlier no longer matches the corpus on disk."""


class PaperTimeout(BaseException):  # not an Exception: no stage may swallow it
    pass


@dataclass(frozen=True)
class Options:
    """Everything a worker process needs; plain data so that it can be pickled."""

    stages: dict[str, str] = field(default_factory=lambda: dict(STAGES))
    scan: bool = False  # gather + references only: no tree, no tables
    paper_timeout_s: float = 0.0  # 0 = none
    bbl_index: dict[tuple[str, int], list[Path]] = field(default_factory=dict)
    bbl_hashes: dict[Path, str] = field(default_factory=dict)


@dataclass
class PaperOutcome:
    arxiv_id: str
    folder: str
    fingerprint: str
    versions: list[dict]
    rows: Rows
    error: str | None
    elapsed_s: float


# --------------------------------------------------------------------------- stages

_loaded: dict[str, Callable] = {}


def load_stage(spec: str) -> Callable:
    """``"package.module:function"`` -> the function (cached per process)."""
    if spec not in _loaded:
        module, _, attr = spec.partition(":")
        try:
            _loaded[spec] = getattr(importlib.import_module(module), attr)
        except (ImportError, AttributeError) as exc:
            raise StageError(f"{spec}: {exc}") from exc
    return _loaded[spec]


def check_stages(options: Options) -> list[str]:
    """Problems that would stop the run, found before anything is processed."""
    if options.scan:
        return []
    problems = []
    for name, spec in options.stages.items():
        try:
            load_stage(spec)
        except StageError as exc:
            problems.append(f"stage {name!r}: {exc}")
    return problems


def _normalize(options: Options, text: str) -> str:
    return text if options.scan else load_stage(options.stages["normalize"])(text)


def _build_tree(options: Options, text: str):
    return None if options.scan else load_stage(options.stages["build_tree"])(text)


def _dedup(options: Options, arxiv_id: str, versions: list[VersionParse]) -> Rows:
    return empty_rows() if options.scan else load_stage(options.stages["dedup"])(arxiv_id, versions)


# --------------------------------------------------------------------------- one paper


@contextmanager
def time_limit(seconds: float):
    """Raise ``PaperTimeout`` after ``seconds`` (POSIX main thread only; otherwise a no-op)."""
    usable = (
        seconds
        and hasattr(signal, "SIGALRM")
        and threading.current_thread() is threading.main_thread()
    )
    if not usable:
        yield
        return

    def expired(signum, frame):
        raise PaperTimeout(f"timeout after {seconds:g} s")

    previous = signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, seconds)
    try:
        yield
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous)


def fingerprint(paper_dir: Path) -> str:
    """Hash of the relative path and size of every file of a paper (cheap, no content read)."""
    digest = hashlib.sha1()
    for path in sorted(p for p in paper_dir.rglob("*") if p.is_file()):
        digest.update(f"{path.relative_to(paper_dir).as_posix()}\0{path.stat().st_size}\n".encode())
    return digest.hexdigest()[:16]


def _read_metadata(paper_dir: Path) -> dict:
    return json.loads((paper_dir / "metadata.json").read_text(encoding="utf-8"))


def _short(exc: BaseException) -> str:
    return f"{type(exc).__name__}: {exc}"[:300]


def _count_nodes(tree) -> int | None:
    """Elements of a version before deduplication: every node below the document root."""
    if hasattr(tree, "walk"):
        return sum(1 for _ in tree.walk()) - 1
    return len(tree) if isinstance(tree, list | tuple) else None


def _parse_version(
    vdir: Path,
    title: str | None,
    version: int,
    options: Options,
    crawl: dict | None,
    arxiv_id: str = "",
) -> tuple[dict, VersionParse | None]:
    """One version -> its journal record and what ``dedup`` gets (None for a missing folder)."""
    info = dict.fromkeys(VERSION_COLUMNS, None)
    info.update(version=version, crawl_status=(crawl or {}).get("status"))
    if not vdir.is_dir():
        info.update(status=MISSING, reason="no version folder")
        return info, None
    started = time.perf_counter()
    try:
        tex = TexVersion(vdir, title)
        if not tex.tex:
            reason = info["crawl_status"] or "no .tex file"
            info.update(status=EMPTY, reason=reason)
            return info, VersionParse(version, EMPTY)
        found = tex.gather()
        info.update(
            n_files=len(found.files),
            n_unused=len(found.unused),
            n_other_roots=len(found.other_roots),
            n_missing_includes=len(found.missing),
        )
        if found.root is None:
            info.update(status=NO_ROOT, reason="no \\documentclass")
            return info, VersionParse(version, NO_ROOT)
        refs = extract_references(
            tex,
            found.files,
            external_bbl=options.bbl_index.get((arxiv_id, version)),
            external_hashes=options.bbl_hashes or None,
        )
        tree = _build_tree(options, _normalize(options, tex.expand(found.root)))
        info.update(
            status=PARSED,
            root=found.root,
            n_bibitems=refs.n_bibitems,
            n_bbl=len(getattr(refs, "bbl_files", ())),
            n_bib_cited=refs.n_bib_cited,
            n_bib_unused=refs.n_bib_unused,
            missing_bib=";".join(refs.missing_bib) or None,
            n_nodes=_count_nodes(tree),
            n_refs=len(refs.entries),
        )
        return info, VersionParse(version, PARSED, tree, refs.entries)
    except BBLIntegrityError:
        raise
    except Exception as exc:  # one bad version must not cost the paper or the run
        info.update(status=ERROR, reason=_short(exc))
        return info, VersionParse(version, ERROR)
    finally:
        info["secs"] = round(time.perf_counter() - started, 4)


def process_paper(
    paper_dir: Path, options: Options, crawl: dict[int, dict] | None = None
) -> PaperOutcome:
    """Parse every version of one paper and deduplicate them. Never raises (except Ctrl+C)."""
    started = time.perf_counter()
    paper_dir = Path(paper_dir)
    folder, error = paper_dir.name, None
    versions: list[dict] = []
    rows = empty_rows()
    arxiv_id = from_folder_name(folder) if _is_folder_name(folder) else folder
    try:
        with time_limit(options.paper_timeout_s):
            meta = _read_metadata(paper_dir)
            arxiv_id = meta.get("arxiv_id") or arxiv_id
            n_versions = int(meta.get("num_versions") or 0) or _count_version_dirs(paper_dir)
            parses = []
            for number in range(1, n_versions + 1):
                info, parse = _parse_version(
                    paper_dir / f"v{number}",
                    meta.get("title"),
                    number,
                    options,
                    (crawl or {}).get(number),
                    arxiv_id,
                )
                versions.append(info)
                if parse is not None:
                    parses.append(parse)
            if parses:
                rows = _dedup(options, arxiv_id, parses)
    except BBLIntegrityError:
        raise
    except (Exception, PaperTimeout) as exc:
        error, rows = _short(exc), empty_rows()
    return PaperOutcome(
        arxiv_id=arxiv_id,
        folder=folder,
        fingerprint=fingerprint(paper_dir),
        versions=versions,
        rows=rows,
        error=error,
        elapsed_s=round(time.perf_counter() - started, 4),
    )


def _is_folder_name(name: str) -> bool:
    try:
        from_folder_name(name)
    except ValueError:
        return False
    return True


def _count_version_dirs(paper_dir: Path) -> int:
    numbers = [int(p.name[1:]) for p in paper_dir.iterdir() if p.is_dir() and p.name[1:].isdigit()]
    return max(numbers, default=0)


# --------------------------------------------------------------------------- journal


class Journal:
    """``shards.jsonl``: one line per committed shard, appended last, flushed and fsynced."""

    def __init__(self, path: Path) -> None:
        self.path = path

    def load(self) -> tuple[dict[str, dict], list[int]]:
        """(latest record per paper, committed shard numbers); a torn last line is dropped."""
        papers: dict[str, dict] = {}
        shards: list[int] = []
        if not self.path.exists():
            return papers, shards
        data = self.path.read_bytes()
        if data and not data.endswith(b"\n"):  # killed in the middle of a write
            data = data[: data.rfind(b"\n") + 1]
            self.path.write_bytes(data)
        for line in data.decode("utf-8").splitlines():
            try:
                shard = json.loads(line)
            except json.JSONDecodeError:
                continue
            shards.append(shard["shard"])
            for record in shard["papers"]:
                papers[record["arxiv_id"]] = {**record, "shard": shard["shard"]}
        return papers, shards

    def append(self, record: dict) -> None:
        with open(self.path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
            fh.flush()
            os.fsync(fh.fileno())


def clean_orphans(out_dir: Path, committed: list[int]) -> list[str]:
    """Delete the staging area and shard folders the journal does not know (a killed run)."""
    removed = []
    if (out_dir / STAGING).exists():
        shutil.rmtree(out_dir / STAGING)
        removed.append(STAGING)
    known = {SHARD_DIR.format(n) for n in committed}
    for path in sorted(out_dir.glob("shard-*")):
        if path.is_dir() and path.name not in known:
            shutil.rmtree(path)
            removed.append(path.name)
    return removed


# --------------------------------------------------------------------------- corpus


class DuplicatePaperError(RuntimeError):
    """The same paper is in two of the crawl runs given as ``--data-root``."""


class IncompleteCorpusError(RuntimeError):
    """``--expect-assigned``: the papers found are not exactly the assigned IDs."""


def as_paths(value) -> list[Path]:
    """One path or several (the runs of a distributed crawl) as a list."""
    if value is None:
        return []
    return [Path(value)] if isinstance(value, str | Path) else [Path(v) for v in value]


def list_papers(data_root) -> list[Path]:
    """Paper folders of one crawl run, or of several runs (partitions of one corpus).

    The result is sorted by folder name whatever the order of the roots, so that shards and
    outputs do not depend on it. A paper found in two runs is an error: the partitions of the
    group crawl are disjoint, and parsing one paper twice would hide a mistake.
    """
    found: dict[str, Path] = {}
    for root in as_paths(data_root):
        for p in root.iterdir():
            if p.is_dir() and (p / "metadata.json").is_file():
                if p.name in found:
                    raise DuplicatePaperError(
                        f"{p.name} is in both {found[p.name].parent} and {root}"
                    )
                found[p.name] = p
    return [found[name] for name in sorted(found)]


def assigned_folders() -> set[str]:
    """Folder names of every assigned ID (the four ranges of ``config.RANGES``): 20,000."""
    from src.utils.ids import iter_id_range

    return {
        to_folder_name(pid)
        for first, last in config.RANGES.values()
        for pid in iter_id_range(first, last, config.MONTH_END)
    }


def check_assigned(papers: list[Path]) -> dict:
    """Papers found versus the assigned IDs: ``expected``, ``found``, ``missing``, ``extra``."""
    expected, found = assigned_folders(), {p.name for p in papers}
    return {
        "expected": len(expected),
        "found": len(found),
        "missing": sorted(expected - found),
        "extra": sorted(found - expected),
    }


def select_papers(
    papers: list[Path], ids: list[str] | None, offset: int, limit: int | None
) -> list[Path]:
    if ids:
        wanted = {to_folder_name(i) for i in ids}
        papers = [p for p in papers if p.name in wanted]
    papers = papers[offset:]
    return papers[:limit] if limit is not None else papers


def crawl_index(work_dir) -> dict[str, dict[int, dict]]:
    """arxiv_id -> {version: crawler record} from the ``versions.jsonl`` of one or more runs."""
    index: dict[str, dict[int, dict]] = {}
    from src.run_pipeline import load_version_state

    for directory in as_paths(work_dir):
        for (arxiv_id, version), rec in load_version_state(directory / "versions.jsonl").items():
            index.setdefault(arxiv_id, {})[version] = {
                "status": rec.get("status"),
                "kind": rec.get("kind"),
            }
    return index


# --------------------------------------------------------------------------- run


def _outcomes(
    papers: list[Path], options: Options, crawl: dict[str, dict[int, dict]], workers: int
) -> Iterator[PaperOutcome]:
    """Outcomes in the order of ``papers``, whatever the number of workers."""

    def crawl_of(path: Path) -> dict[int, dict] | None:
        return crawl.get(from_folder_name(path.name)) if _is_folder_name(path.name) else None

    by_paper: dict[str, dict[tuple[str, int], list[Path]]] = {}
    for key, files in options.bbl_index.items():
        by_paper.setdefault(key[0], {})[key] = files

    def options_of(path: Path) -> Options:
        paper_index = by_paper.get(_id_of(path), {})
        paper_files = {file for files in paper_index.values() for file in files}
        return replace(
            options,
            bbl_index=paper_index,
            bbl_hashes={p: options.bbl_hashes[p] for p in paper_files if p in options.bbl_hashes},
        )

    if workers <= 1:
        for path in papers:
            yield process_paper(path, options_of(path), crawl_of(path))
        return
    with ProcessPoolExecutor(max_workers=workers, max_tasks_per_child=RECYCLE_AFTER) as pool:
        remaining = iter(papers)
        window = deque(
            pool.submit(process_paper, p, options_of(p), crawl_of(p))
            for p in itertools.islice(remaining, workers * 2)
        )
        while window:
            outcome = window.popleft().result()
            if (nxt := next(remaining, None)) is not None:
                window.append(pool.submit(process_paper, nxt, options_of(nxt), crawl_of(nxt)))
            yield outcome


def _record(outcome: PaperOutcome) -> dict:
    return {
        "arxiv_id": outcome.arxiv_id,
        "folder": outcome.folder,
        "fingerprint": outcome.fingerprint,
        "error": outcome.error,
        "elapsed_s": outcome.elapsed_s,
        "rows": count_rows(outcome.rows),
        "versions": outcome.versions,
    }


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _atomic_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    tmp = path.with_suffix(".csv.tmp")
    with open(tmp, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)


def export_csv(work_dir: Path, papers: dict[str, dict]) -> None:
    """Rebuild ``m2_versions.csv`` and ``m2_papers.csv`` from the journal (always consistent)."""
    version_rows, paper_rows = [], []
    for arxiv_id in sorted(papers):
        rec = papers[arxiv_id]
        counts = Counter(v["status"] for v in rec["versions"])
        paper_rows.append(
            {
                "arxiv_id": arxiv_id,
                "shard": rec["shard"],
                "n_versions": len(rec["versions"]),
                "n_parsed": counts[PARSED],
                "n_empty": counts[EMPTY],
                "n_no_root": counts[NO_ROOT],
                "n_missing": counts[MISSING],
                "n_error": counts[ERROR],
                "error": rec["error"],
                "elapsed_s": rec["elapsed_s"],
                **rec["rows"],
                "elements_before": sum(v.get("n_nodes") or 0 for v in rec["versions"]),
                "bibtex_before": sum(v.get("n_refs") or 0 for v in rec["versions"]),
            }
        )
        version_rows.extend({**v, "arxiv_id": arxiv_id} for v in rec["versions"])
    _atomic_csv(work_dir / "m2_versions.csv", VERSION_COLUMNS, version_rows)
    _atomic_csv(work_dir / "m2_papers.csv", PAPER_COLUMNS, paper_rows)


def run(
    data_root,
    out_dir: Path,
    work_dir: Path,
    *,
    options: Options | None = None,
    ids: list[str] | None = None,
    offset: int = 0,
    limit: int | None = None,
    workers: int = 1,
    shard_papers: int = 100,
    shard_rows: int = 200_000,
    retry_errors: bool = False,
    verify_done: bool = False,
    crawl_work_dir=None,
    expect_assigned: bool = False,
    progress: bool = True,
) -> dict:
    """Parse the selected papers not committed yet; returns the summary (also written)."""
    options = options or Options()
    if problems := check_stages(options):
        raise StageError("; ".join(problems))
    roots = as_paths(data_root)
    out_dir, work_dir = Path(out_dir), Path(work_dir)
    work_dir.mkdir(parents=True, exist_ok=True)
    lock = WorkDirLock(work_dir)
    lock.acquire()  # raises WorkDirLockedError: two runs must not share a journal
    started_at, t0 = _now(), time.perf_counter()
    interrupted = True
    journal = Journal(work_dir / "shards.jsonl")
    done, committed = journal.load()
    papers_this_run = 0
    papers_failed = 0
    version_status: Counter = Counter()
    written = Counter()
    assigned: dict | None = None
    try:
        if not options.scan:
            out_dir.mkdir(parents=True, exist_ok=True)
            if removed := clean_orphans(out_dir, committed):
                log.warning("removed unfinished output of an earlier run: %s", removed)
        every_paper = list_papers(roots)
        if expect_assigned:
            assigned = check_assigned(every_paper)
            (work_dir / "assigned_check.json").write_text(
                json.dumps(assigned, indent=2) + "\n", encoding="utf-8"
            )
            if assigned["missing"] or assigned["extra"]:
                raise IncompleteCorpusError(
                    f"{assigned['found']} papers found for {assigned['expected']} assigned IDs: "
                    f"{len(assigned['missing'])} missing (first {assigned['missing'][:5]}), "
                    f"{len(assigned['extra'])} not assigned (first {assigned['extra'][:5]})"
                )
        selected = select_papers(every_paper, ids, offset, limit)
        if verify_done:
            changed = [
                p.name
                for p in selected
                if (rec := done.get(_id_of(p))) and rec["fingerprint"] != fingerprint(p)
            ]
            if changed:
                raise ChangedCorpusError(
                    f"{len(changed)} committed paper(s) changed on disk (first: {changed[:5]}); "
                    "parse into a new --out-dir"
                )
        pending = [
            p
            for p in selected
            if (rec := done.get(_id_of(p))) is None or (retry_errors and rec["error"])
        ]
        log.info(
            "%d papers selected, %d committed, %d to parse",
            len(selected),
            len(selected) - len(pending),
            len(pending),
        )

        buffer: list[PaperOutcome] = []
        buffered_rows = 0
        next_shard = max(committed, default=-1) + 1

        def commit() -> None:
            nonlocal buffer, buffered_rows, next_shard
            if not buffer:
                return
            rows = empty_rows()
            for item in buffer:
                extend_rows(rows, item.rows)
            if not options.scan:
                staging = out_dir / STAGING / SHARD_DIR.format(next_shard)
                shutil.rmtree(staging, ignore_errors=True)
                staging.mkdir(parents=True)
                load_stage(options.stages["write_tables"])(rows, staging)
                os.replace(staging, out_dir / SHARD_DIR.format(next_shard))
                with suppress(OSError):  # only if nothing else is staged
                    staging.parent.rmdir()
            records = [_record(item) for item in buffer]
            journal.append({"shard": next_shard, "committed_at": _now(), "papers": records})
            for record in records:
                done[record["arxiv_id"]] = {**record, "shard": next_shard}
            written.update(count_rows(rows))
            written["shards"] += 1
            committed.append(next_shard)
            next_shard += 1
            buffer, buffered_rows = [], 0

        bar = tqdm(
            total=len(selected),
            initial=len(selected) - len(pending),
            unit="paper",
            disable=not progress,
            dynamic_ncols=True,
            mininterval=0,
        )
        unreported = 0
        try:
            for outcome in _outcomes(pending, options, crawl_index(crawl_work_dir), workers):
                papers_this_run += 1
                papers_failed += outcome.error is not None
                version_status.update(v["status"] for v in outcome.versions)
                buffer.append(outcome)
                buffered_rows += sum(count_rows(outcome.rows).values())
                if outcome.error:
                    log.warning("%s failed: %s", outcome.arxiv_id, outcome.error)
                unreported += 1
                if unreported == 25:
                    bar.update(unreported)
                    unreported = 0
                if len(buffer) >= shard_papers or buffered_rows >= shard_rows:
                    commit()
        finally:
            if unreported:
                bar.update(unreported)
            bar.close()
            commit()  # a Ctrl+C or an error keeps the papers already parsed
        interrupted = False
    finally:
        if committed or done:
            export_csv(work_dir, done)
        summary = _summary(
            started_at,
            time.perf_counter() - t0,
            interrupted,
            papers_this_run,
            papers_failed,
            version_status,
            done,
            written,
            workers,
            options,
        )
        summary["data_roots"] = [str(r) for r in roots]
        if assigned is not None:
            summary["assigned_check"] = {k: assigned[k] for k in ("expected", "found")}
        (work_dir / "run_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        with open(work_dir / "runs.jsonl", "a", encoding="utf-8") as fh:
            fh.write(json.dumps(summary) + "\n")
        lock.release()
    return summary


def _id_of(path: Path) -> str:
    """The arXiv ID a folder is journaled under (``metadata.json`` first, else the name)."""
    try:
        return _read_metadata(path).get("arxiv_id") or from_folder_name(path.name)
    except (OSError, ValueError, json.JSONDecodeError):
        return path.name


def _summary(
    started_at, wall_s, interrupted, papers, failed, versions, done, written, workers, options
) -> dict:
    return {
        "started_at": started_at,
        "finished_at": _now(),
        "interrupted": interrupted,
        "scan_only": options.scan,
        "workers": workers,
        "wall_s": round(wall_s, 3),
        "papers_this_run": papers,
        "papers_failed_this_run": failed,
        "avg_paper_s": round(wall_s / papers, 4) if papers else None,
        "papers_committed_total": len(done),
        "version_status_this_run": dict(versions),
        "rows_written_this_run": {k: written[k] for k in ("elements", "hierarchy", "bibtex")},
        "shards_written_this_run": written["shards"],
    }


# --------------------------------------------------------------------------- CLI


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--data-root",
        type=Path,
        action="append",
        help="corpus folder; repeat it for the runs of a distributed crawl (they are merged)",
    )
    parser.add_argument("--bbl-recovery-dir", type=Path, help="verified BBL recovery input")
    parser.add_argument("--out-dir", type=Path, default=Path("data/m2"))
    parser.add_argument("--work-dir", type=Path, default=Path("work/m2"))
    parser.add_argument(
        "--crawl-work-dir",
        type=Path,
        action="append",
        help="crawler work/ with versions.jsonl (repeat it, one per --data-root)",
    )
    parser.add_argument(
        "--expect-assigned",
        action="store_true",
        help="stop unless the papers found are exactly the 20,000 assigned IDs",
    )
    parser.add_argument("--ids", help="comma-separated arXiv IDs, or @file with one per line")
    parser.add_argument("--offset", type=int, default=0, help="skip the first N papers")
    parser.add_argument("--limit", type=int, help="parse at most N papers")
    parser.add_argument("--workers", type=int, default=1, help="processes (default 1)")
    parser.add_argument("--shard-papers", type=int, default=100)
    parser.add_argument("--shard-rows", type=int, default=200_000)
    parser.add_argument(
        "--paper-timeout", type=float, default=0.0, help="seconds per paper (POSIX)"
    )
    parser.add_argument("--retry-errors", action="store_true", help="re-run papers that failed")
    parser.add_argument(
        "--verify-done", action="store_true", help="fail if a committed paper changed"
    )
    parser.add_argument("--scan", action="store_true", help="gather + references only, no tables")
    parser.add_argument(
        "--stage",
        action="append",
        default=[],
        metavar="NAME=MODULE:FUNCTION",
        help="replace a stage",
    )
    parser.add_argument("--no-progress", action="store_true")
    return parser


def _read_ids(value: str | None) -> list[str] | None:
    if not value:
        return None
    raw = (
        Path(value[1:]).read_text(encoding="utf-8").split()
        if value.startswith("@")
        else value.split(",")
    )
    return [i.strip() for i in raw if i.strip()]


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    args.data_root = args.data_root or [Path(config.DEFAULT_DATA_ROOT)]
    stages = dict(STAGES)
    for item in args.stage:
        name, _, spec = item.partition("=")
        if name not in STAGES or ":" not in spec:
            raise SystemExit(f"--stage expects NAME=MODULE:FUNCTION with NAME in {sorted(STAGES)}")
        stages[name] = spec
    bbl_index = {}
    bbl_hashes = {}
    if not args.bbl_recovery_dir and (args.work_dir / "bbl-input-identity.json").exists():
        raise ValueError("BBL input is required to resume this run")
    if args.bbl_recovery_dir:
        from src.benchmark.m2_bbl_input import (
            load_bbl_hashes,
            validate_recovery_complete,
            verify_run_identity,
        )
        from src.benchmark.m2_inputs import verify_corpus_identity, verify_run_config

        if args.expect_assigned:
            validate_recovery_complete(args.bbl_recovery_dir)
        verify_run_config(
            args.work_dir,
            {
                "stages": stages,
                "scan": args.scan,
                "workers": args.workers,
                "shard_papers": args.shard_papers,
                "shard_rows": args.shard_rows,
                "paper_timeout": args.paper_timeout,
            },
        )
        verify_corpus_identity(args.data_root, args.work_dir)
        bbl_index = verify_run_identity(args.bbl_recovery_dir, args.work_dir)
        bbl_hashes = load_bbl_hashes(args.bbl_recovery_dir, bbl_index)
        identity = json.loads(
            (args.work_dir / "bbl-input-identity.json").read_text(encoding="utf8")
        )
        journal_hash = hashlib.sha256(
            (args.bbl_recovery_dir / "versions.jsonl").read_bytes()
        ).hexdigest()
        if journal_hash != identity["journal_sha256"]:
            raise ValueError("BBL journal changed during preflight")
        papers = {}
        for paper in list_papers(as_paths(args.data_root)):
            metadata = _read_metadata(paper)
            arxiv_id = metadata.get("arxiv_id")
            if not isinstance(arxiv_id, str) or to_folder_name(arxiv_id) != paper.name:
                raise ValueError(f"Paper metadata does not match folder: {paper}")
            if arxiv_id in papers:
                raise ValueError(f"Duplicate paper metadata: {arxiv_id}")
            papers[arxiv_id] = paper
        for arxiv_id, version in bbl_index:
            paper = papers.get(arxiv_id)
            if (
                paper is None
                or not (paper / f"v{version}").is_dir()
                or version > int(_read_metadata(paper).get("num_versions") or 0)
            ):
                raise ValueError(f"BBL input has no matching corpus version: {arxiv_id}v{version}")
            if any((paper / f"v{version}").rglob("*.bbl")):
                raise ValueError(f"Local BBL conflicts with external input: {arxiv_id}v{version}")
    options = Options(
        stages=stages,
        scan=args.scan,
        paper_timeout_s=args.paper_timeout,
        bbl_index=bbl_index,
        bbl_hashes=bbl_hashes,
    )

    args.work_dir.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(args.work_dir / "run.log", encoding="utf-8")
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
    root_logger = logging.getLogger()
    previous_level = root_logger.level
    root_logger.addHandler(file_handler)
    root_logger.setLevel(logging.INFO)
    try:
        summary = run(
            args.data_root,
            args.out_dir,
            args.work_dir,
            options=options,
            ids=_read_ids(args.ids),
            offset=args.offset,
            limit=args.limit,
            workers=args.workers,
            shard_papers=args.shard_papers,
            shard_rows=args.shard_rows,
            retry_errors=args.retry_errors,
            verify_done=args.verify_done,
            crawl_work_dir=args.crawl_work_dir,
            expect_assigned=args.expect_assigned,
            progress=not args.no_progress,
        )
    except StageError as exc:
        print(
            f"not started, a stage is missing: {exc}\n(use --scan to run without the stages)",
            file=sys.stderr,
        )
        return 3
    except WorkDirLockedError as exc:
        print(f"not started: {exc}", file=sys.stderr)
        return 2
    except ChangedCorpusError as exc:
        print(f"stopped: {exc}", file=sys.stderr)
        return 4
    except (IncompleteCorpusError, DuplicatePaperError) as exc:
        print(f"not started: {exc}", file=sys.stderr)
        return 5
    except KeyboardInterrupt:
        print("interrupted: run the same command to resume", file=sys.stderr)
        return 130
    finally:
        root_logger.removeHandler(file_handler)
        file_handler.close()
        root_logger.setLevel(previous_level)
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
