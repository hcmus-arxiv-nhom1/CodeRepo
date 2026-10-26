# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""Crawl one ID range end to end, with per-version checkpoint and resume.

Usage:
    python -m src.run_pipeline --range thien                    # assigned range (config.RANGES)
    python -m src.run_pipeline --range thien --offset 1207 --limit 200   # trial slice
    python -m src.run_pipeline --start 2606.30734 --end 2607.03693 --month-end 2606=32040
    python -m src.run_pipeline --ids 2606.30762,2607.00001

Outputs:
    <data-root>/<yymm-id>/metadata.json, vN/   the corpus (vN/ empty when there is no TeX)
    <work-dir>/versions.jsonl     one line per processed version: status, kind, sizes, times
    <work-dir>/discovery.jsonl    one line per resolved ID
    <work-dir>/failed.csv         what is still failing after this run
    <work-dir>/run_summary.json   counts and timings of the last run (runs.jsonl keeps all)
    <work-dir>/run.log            every arXiv request with its timestamp

Re-running the same command skips every version whose latest record is ok / no_tex /
unavailable and whose vN/ folder exists, so a killed run resumes where it stopped and
failed versions are retried. Only one run at a time may use a work directory
(``<work-dir>/run.lock``), so arXiv never sees two connections from one machine.

Reference metadata (Semantic Scholar) is crawled in a background thread when
``src/crawler/references.py`` provides ``crawl(arxiv_ids: list[str], data_root: Path)``:
it must write ``<yymm-id>/references.json``, skip papers already done, and use
``semantic_scholar_limiter()``.
"""

from __future__ import annotations

import argparse
import csv
import importlib
import importlib.util
import json
import logging
import os
import shutil
import sys
import threading
import time
from collections import Counter
from datetime import UTC, datetime
from pathlib import Path

from tqdm import tqdm

from src import config
from src.crawler.discovery import discover
from src.crawler.downloader import download_version
from src.crawler.extract import CorruptArchiveError, extract_version
from src.crawler.http_client import HttpClient
from src.crawler.ratelimit import get_arxiv_limiter
from src.crawler.worklock import WorkDirLock, WorkDirLockedError
from src.utils.ids import iter_id_range, to_folder_name

log = logging.getLogger("src.run_pipeline")

FINAL_STATUSES = frozenset({"ok", "no_tex", "unavailable"})
SUSPICIOUS_KINDS = frozenset({"empty", "html"})  # 200 responses that are probably errors
MAX_DOWNLOADS_PER_VERSION = 3  # re-download when the archive turns out to be corrupt


def _now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


class JsonlWriter:
    """Append-only JSON lines, flushed and fsynced per record so a hard kill loses nothing."""

    def __init__(self, path: Path) -> None:
        self._fh = open(path, "a", encoding="utf-8")

    def write(self, record: dict) -> None:
        self._fh.write(json.dumps(record, ensure_ascii=False) + "\n")
        self._fh.flush()
        os.fsync(self._fh.fileno())

    def close(self) -> None:
        self._fh.close()


def load_version_state(path: Path) -> dict[tuple[str, int], dict]:
    """Latest record per (arxiv_id, version) from versions.jsonl."""
    state: dict[tuple[str, int], dict] = {}
    if path.exists():
        with open(path, encoding="utf-8") as fh:
            for line in fh:
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    continue  # torn last line after a hard kill
                state[(rec["arxiv_id"], rec["version"])] = rec
    return state


def version_dir(data_root: Path, arxiv_id: str, version: int) -> Path:
    return data_root / to_folder_name(arxiv_id) / f"v{version}"


def is_done(rec: dict | None, data_root: Path) -> bool:
    return (
        rec is not None
        and rec.get("status") in FINAL_STATUSES
        and version_dir(data_root, rec["arxiv_id"], rec["version"]).is_dir()
    )


def process_version(
    client: HttpClient,
    arxiv_id: str,
    version: int,
    *,
    data_root: Path,
    tmp_dir: Path,
    keep_extensions: frozenset[str],
    archive_dir: Path | None = None,
) -> dict:
    """Download + extract one version and return its versions.jsonl record."""
    start = time.perf_counter()
    vdir = version_dir(data_root, arxiv_id, version)
    rec: dict = {"arxiv_id": arxiv_id, "version": version}
    reason = None
    for _ in range(MAX_DOWNLOADS_PER_VERSION):
        dl = download_version(client, arxiv_id, version, tmp_dir)
        rec.update(
            http_status=dl.http_status,
            content_type=dl.content_type,
            archive_name=dl.filename,
            download_s=round(dl.elapsed_s, 3),
            attempts=dl.attempts,
        )
        if dl.status == "unavailable":
            shutil.rmtree(vdir, ignore_errors=True)
            vdir.mkdir(parents=True)  # empty record
            return {**rec, "status": "unavailable", "reason": dl.reason, "total_s": _since(start)}
        if dl.status == "failed":
            return {
                **rec,
                "status": "failed",
                "stage": "download",
                "reason": dl.reason,
                "total_s": _since(start),
            }

        t_extract = time.perf_counter()
        try:
            res = extract_version(dl.path, vdir, keep_extensions, dl.filename)
        except CorruptArchiveError as exc:
            reason = str(exc)
            log.warning("%sv%d: corrupt archive (%s), downloading again", arxiv_id, version, reason)
            dl.path.unlink(missing_ok=True)
            continue
        if archive_dir is not None:  # keep the raw response (fixtures for the spike)
            archive_dir.mkdir(parents=True, exist_ok=True)
            os.replace(dl.path, archive_dir / Path(dl.filename or dl.path.name).name)
        else:
            dl.path.unlink(missing_ok=True)

        rec.update(
            kind=res.kind,
            files=res.files,
            archive_bytes=res.archive_bytes,
            bytes_before=res.bytes_before,
            bytes_after=res.bytes_after,
            skipped_unsafe=res.skipped_unsafe,
            dropped=res.dropped,
            extract_s=_since(t_extract),
        )
        if res.kind in SUSPICIOUS_KINDS:
            shutil.rmtree(vdir, ignore_errors=True)
            return {
                **rec,
                "status": "failed",
                "stage": "extract",
                "reason": f"unexpected {res.kind} response",
                "total_s": _since(start),
            }
        return {**rec, "status": "ok" if res.has_tex else "no_tex", "total_s": _since(start)}

    shutil.rmtree(vdir, ignore_errors=True)
    return {
        **rec,
        "status": "failed",
        "stage": "extract",
        "reason": f"corrupt archive after {MAX_DOWNLOADS_PER_VERSION} downloads: {reason}",
        "total_s": _since(start),
    }


def _since(start: float) -> float:
    return round(time.perf_counter() - start, 3)


def _start_references(ids: list[str], data_root: Path) -> threading.Thread | None:
    if importlib.util.find_spec("src.crawler.references") is None:
        log.warning("references: src/crawler/references.py not available yet, skipped")
        return None
    crawl = getattr(importlib.import_module("src.crawler.references"), "crawl", None)
    if not callable(crawl):
        log.warning("references: references.crawl(arxiv_ids, data_root) not found, skipped")
        return None

    def target() -> None:
        try:
            crawl(ids, data_root)
        except Exception:
            log.exception("references: crawl failed")

    thread = threading.Thread(target=target, name="semantic-scholar", daemon=True)
    thread.start()
    return thread


def summarize(
    ids: list[str],
    found: dict[str, int],
    missing: dict[str, str],
    state: dict[tuple[str, int], dict],
    data_root: Path,
) -> dict:
    """Counts and size statistics over the requested IDs, from the latest version records."""
    status, kinds = Counter(), Counter()
    complete, sizes_before, sizes_after = 0, [], []
    for arxiv_id in ids:
        n = found.get(arxiv_id)
        if not n:
            continue
        recs = [state.get((arxiv_id, v)) for v in range(1, n + 1)]
        status.update(r["status"] if r else "pending" for r in recs)
        kinds.update(r["kind"] for r in recs if r and r.get("kind"))
        if all(is_done(r, data_root) for r in recs):
            complete += 1
            sizes_before.append(sum(r.get("bytes_before", 0) for r in recs))
            sizes_after.append(sum(r.get("bytes_after", 0) for r in recs))
    versions = [r for r in state.values() if r["arxiv_id"] in found]
    return {
        "ids_requested": len(ids),
        "papers_found": len(found),
        "papers_missing": len(missing),
        "papers_complete": complete,
        "paper_success_rate": round(complete / len(ids), 4) if ids else None,
        "versions_total": sum(found.values()),
        "version_status": dict(status),
        "version_kinds": dict(kinds),
        "avg_paper_bytes_before": round(sum(sizes_before) / complete) if complete else None,
        "avg_paper_bytes_after": round(sum(sizes_after) / complete) if complete else None,
        "avg_versions_per_paper": round(sum(found.values()) / len(found), 3) if found else None,
        "versions_recorded": len(versions),
    }


def write_failed_csv(
    path: Path, ids: list[str], found: dict[str, int], missing: dict[str, str], state: dict
) -> int:
    rows = [
        {"arxiv_id": i, "version": "", "stage": "discovery", "reason": r, "http_status": ""}
        for i, r in missing.items()
    ]
    for arxiv_id in ids:
        for v in range(1, found.get(arxiv_id, 0) + 1):
            rec = state.get((arxiv_id, v))
            if rec and rec["status"] == "failed":
                rows.append(
                    {
                        "arxiv_id": arxiv_id,
                        "version": v,
                        "stage": rec.get("stage", ""),
                        "reason": rec.get("reason", ""),
                        "http_status": rec.get("http_status") or "",
                    }
                )
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(
            fh, fieldnames=["arxiv_id", "version", "stage", "reason", "http_status"]
        )
        writer.writeheader()
        writer.writerows(rows)
    return len(rows)


def run(
    ids: list[str],
    *,
    data_root: Path,
    work_dir: Path,
    client: HttpClient | None = None,
    snapshot: Path | None = None,
    keep_extensions: frozenset[str] = config.KEEP_EXTENSIONS,
    keep_archives: bool = False,
    references: bool = True,
    refresh_metadata: bool = False,
    progress: bool = True,
    label: str | None = None,
) -> dict:
    """Discover ``ids``, then download and extract every version not done yet."""
    data_root.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)
    lock = WorkDirLock(work_dir)
    lock.acquire()  # raises WorkDirLockedError if another run uses this work_dir
    tmp_dir = work_dir / "tmp"
    archive_dir = work_dir / "archives" if keep_archives else None
    client = client or HttpClient(get_arxiv_limiter(), config.USER_AGENT)

    started_at, t0 = _now(), time.perf_counter()
    requests_before = client.requests_sent
    found: dict[str, int] = {}
    missing: dict[str, str] = {}
    discovery_s = 0.0
    processed: list[dict] = []
    state: dict[tuple[str, int], dict] = {}
    interrupted = False
    disc_log = JsonlWriter(work_dir / "discovery.jsonl")
    ver_log = JsonlWriter(work_dir / "versions.jsonl")
    try:
        log.info("run %s: %d IDs %s -> %s", label or "", len(ids), ids[0], ids[-1])
        disc = discover(
            ids,
            client,
            data_root,
            snapshot=snapshot,
            refresh=refresh_metadata,
            journal=disc_log.write,
        )
        found, missing, discovery_s = disc.found, disc.missing, disc.elapsed_s
        ref_thread = (
            _start_references([i for i in ids if i in found], data_root) if references else None
        )

        state = load_version_state(work_dir / "versions.jsonl")
        todo = [
            (i, v)
            for i in ids
            for v in range(1, found.get(i, 0) + 1)
            if not is_done(state.get((i, v)), data_root)
        ]
        log.info(
            "%d versions in total, %d already done, %d to crawl",
            sum(found.values()),
            sum(found.values()) - len(todo),
            len(todo),
        )
        bar = tqdm(total=len(todo), unit="ver", disable=not progress, dynamic_ncols=True)
        loop_start = time.perf_counter()
        for k, (arxiv_id, version) in enumerate(todo, 1):
            rec = process_version(
                client,
                arxiv_id,
                version,
                data_root=data_root,
                tmp_dir=tmp_dir,
                keep_extensions=keep_extensions,
                archive_dir=archive_dir,
            )
            rec["finished_at"] = _now()
            ver_log.write(rec)
            state[(arxiv_id, version)] = rec
            processed.append(rec)
            if rec["status"] == "failed":
                log.warning("%sv%d failed: %s", arxiv_id, version, rec.get("reason"))
            bar.update(1)
            if k % 50 == 0 or k == len(todo):
                per = (time.perf_counter() - loop_start) / k
                log.info(
                    "progress %d/%d versions, %s, %.2f s/version, ETA %.1f min",
                    k,
                    len(todo),
                    dict(Counter(r["status"] for r in processed)),
                    per,
                    per * (len(todo) - k) / 60,
                )
        bar.close()
        if ref_thread is not None:
            log.info("waiting for the reference crawl to finish")
            ref_thread.join()
    except BaseException:  # Ctrl+C, BlockedError, crash: the summary must not look complete
        interrupted = True
        raise
    finally:
        disc_log.close()
        ver_log.close()
        shutil.rmtree(tmp_dir, ignore_errors=True)
        wall_s = time.perf_counter() - t0
        summary = {
            "label": label,
            "first_id": ids[0],
            "last_id": ids[-1],
            "started_at": started_at,
            "finished_at": _now(),
            "interrupted": interrupted,
            "wall_s": round(wall_s, 1),
            "discovery_s": round(discovery_s, 1),
            "versions_processed_this_run": len(processed),
            "mean_version_s": round(sum(r["total_s"] for r in processed) / len(processed), 3)
            if processed
            else None,
            "mean_download_s": round(
                sum(r.get("download_s") or 0 for r in processed) / len(processed), 3
            )
            if processed
            else None,
            "avg_paper_s": round(wall_s / len(ids), 3),
            "arxiv_requests": client.requests_sent - requests_before,
            "min_request_gap_s": round(client.limiter.min_gap_s, 3)
            if client.limiter.min_gap_s is not None
            else None,
            "breaker_trips": client.breaker_trips,
            "keep_extensions": sorted(keep_extensions),
            **summarize(ids, found, missing, state, data_root),
        }
        summary["failed_rows"] = write_failed_csv(
            work_dir / "failed.csv", ids, found, missing, state
        )
        (work_dir / "run_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
        with open(work_dir / "runs.jsonl", "a", encoding="utf-8") as fh:
            fh.write(json.dumps(summary) + "\n")
        lock.release()
    return summary


# --------------------------------------------------------------------------- CLI


def parse_month_end(values: list[str]) -> dict[str, int]:
    result = {}
    for item in values:
        yymm, _, num = item.partition("=")
        result[yymm] = int(num)
    return result


def select_ids(args: argparse.Namespace) -> tuple[list[str], str | None]:
    month_end = {**config.MONTH_END, **parse_month_end(args.month_end)}
    if args.ids:
        raw = (
            Path(args.ids[1:]).read_text().split()
            if args.ids.startswith("@")
            else args.ids.split(",")
        )
        ids, label = [i.strip() for i in raw if i.strip()], "ids"
    elif args.range:
        start, end = config.RANGES[args.range]
        ids, label = list(iter_id_range(start, end, month_end)), args.range
    else:
        if not args.end:
            raise SystemExit("--start needs --end")
        ids, label = list(iter_id_range(args.start, args.end, month_end)), None
    ids = ids[args.offset :]
    if args.limit is not None:
        ids = ids[: args.limit]
    if not ids:
        raise SystemExit("no IDs selected")
    return ids, label


def setup_logging(work_dir: Path, console_level: int) -> None:
    work_dir.mkdir(parents=True, exist_ok=True)
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    file_handler = logging.FileHandler(work_dir / "run.log", encoding="utf-8")
    file_handler.setFormatter(
        logging.Formatter(
            "%(asctime)s.%(msecs)03d %(levelname)s %(name)s: %(message)s", "%Y-%m-%d %H:%M:%S"
        )
    )
    console = logging.StreamHandler()
    console.setLevel(console_level)
    console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%H:%M:%S"))
    root.addHandler(file_handler)
    root.addHandler(console)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    which = parser.add_mutually_exclusive_group(required=True)
    which.add_argument("--range", choices=sorted(config.RANGES), help="assigned range by owner")
    which.add_argument("--start", help="first arXiv ID, e.g. 2606.30734 (with --end)")
    which.add_argument("--ids", help="comma-separated IDs, or @file with one ID per line")
    parser.add_argument("--end", help="last arXiv ID (inclusive)")
    parser.add_argument(
        "--month-end",
        action="append",
        default=[],
        metavar="YYMM=N",
        help="last sequence number of a month crossed by the range (overrides config)",
    )
    parser.add_argument("--offset", type=int, default=0, help="skip the first N IDs")
    parser.add_argument("--limit", type=int, help="process at most N IDs")
    parser.add_argument("--data-root", type=Path, default=Path(config.DEFAULT_DATA_ROOT))
    parser.add_argument("--work-dir", type=Path, default=Path(config.DEFAULT_WORK_DIR))
    parser.add_argument("--snapshot", type=Path, help="Kaggle arxiv-metadata-oai-snapshot.json")
    parser.add_argument(
        "--keep-ext",
        help="comma-separated extensions to keep (default: %(default)s)",
        default=",".join(sorted(config.KEEP_EXTENSIONS)),
    )
    parser.add_argument("--keep-archives", action="store_true", help="keep raw /src/ responses")
    parser.add_argument("--no-references", action="store_true", help="skip Semantic Scholar")
    parser.add_argument("--refresh-metadata", action="store_true", help="re-run discovery")
    parser.add_argument("--no-progress", action="store_true", help="log lines instead of a bar")
    parser.add_argument("--dry-run", action="store_true", help="only list the IDs to crawl")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    ids, label = select_ids(args)
    print(f"{len(ids)} IDs: {ids[0]} -> {ids[-1]}")
    if args.dry_run:
        return 0

    setup_logging(args.work_dir, logging.INFO if args.no_progress else logging.WARNING)
    keep = frozenset(
        e.strip().lower() if e.strip().startswith(".") else f".{e.strip().lower()}"
        for e in args.keep_ext.split(",")
        if e.strip()
    )
    try:
        summary = run(
            ids,
            data_root=args.data_root,
            work_dir=args.work_dir,
            snapshot=args.snapshot,
            keep_extensions=keep,
            keep_archives=args.keep_archives,
            references=not args.no_references,
            refresh_metadata=args.refresh_metadata,
            progress=not args.no_progress,
            label=label,
        )
    except KeyboardInterrupt:
        print("interrupted: re-run the same command to resume", file=sys.stderr)
        return 130
    except WorkDirLockedError as exc:
        print(f"not started: {exc}", file=sys.stderr)
        return 2
    print(
        json.dumps(
            {
                k: summary[k]
                for k in (
                    "papers_complete",
                    "ids_requested",
                    "paper_success_rate",
                    "version_status",
                    "wall_s",
                    "discovery_s",
                    "arxiv_requests",
                    "min_request_gap_s",
                    "failed_rows",
                )
            },
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
