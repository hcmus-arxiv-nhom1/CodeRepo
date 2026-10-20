"""GĐ1 spike tools.

    python -m src.spike month-end 2606 --guess 32040   # last ID of a month, via the API
    python -m src.spike run --per-range 10              # crawl a sample, measure, estimate
    python -m src.spike snapshot-coverage --snapshot arxiv-metadata-oai-snapshot.json

``run`` crawls ``--per-range`` IDs of every assigned range (Thiên's sample always holds
the last IDs of June and the first of July), keeps the raw /src/ responses, copies one
of each kind to ``data/fixtures/<kind>/`` for inspection, and writes
``<work-dir>/spike_report.json`` with the time-per-version measurements and the
estimated crawl time for a full 5,000-ID range:

    T ~= ceil(N / 100) * interval + sum over versions of max(interval, t_v)

where ``interval`` is the mean pacing (3 s + jitter/2) and ``t_v`` the measured
request + transfer time of each version.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import random
import shutil
import statistics
from collections import Counter
from collections.abc import Callable
from pathlib import Path

from src import config
from src.crawler.discovery import api_query, load_snapshot
from src.crawler.downloader import source_url
from src.crawler.http_client import HttpClient
from src.crawler.ratelimit import get_arxiv_limiter
from src.run_pipeline import load_version_state, run, setup_logging
from src.utils.ids import iter_id_range

log = logging.getLogger("src.spike")


def find_last_id(
    yymm: str, exists: Callable[[list[str]], set[str]], guess: int, window: int = 100
) -> int:
    """Largest sequence number of month ``yymm`` that exists, probing ``window`` IDs per call."""

    def probe(lo: int, hi: int) -> list[int]:
        found = exists([f"{yymm}.{n:05d}" for n in range(lo, hi + 1)])
        return sorted(int(i.split(".")[1]) for i in found)

    lo = max(1, guess - window // 2)
    for _ in range(200):
        hi = lo + window - 1
        present = probe(lo, hi)
        if not present:
            if lo == 1:
                raise ValueError(f"no IDs found for month {yymm}")
            lo = max(1, lo - window)
            continue
        top = present[-1]
        if top == hi:
            lo = hi + 1
            continue
        after = probe(top + 1, top + window)  # make sure this is not a gap
        if not after:
            return top
        lo = after[-1] + 1
    raise RuntimeError("month-end search did not converge")


def sample_ids(per_range: int, seed: int) -> dict[str, list[str]]:
    rng = random.Random(seed)
    samples = {}
    for name, (start, end) in config.RANGES.items():
        ids = list(iter_id_range(start, end, config.MONTH_END))
        picks: list[str] = []
        if name == "thien":  # the month border: last IDs of June, first IDs of July
            june = [i for i in ids if i.startswith("2606.")]
            july = [i for i in ids if i.startswith("2607.")]
            picks = june[-3:] + july[:2]
        rest = [i for i in ids if i not in picks]
        picks += rng.sample(rest, max(0, per_range - len(picks)))
        samples[name] = sorted(picks)
    return samples


def _pct(values: list[float], q: float) -> float:
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(q * len(ordered)))]


def analyse(records: list[dict], papers: int, range_size: int = 5000) -> dict:
    ok = [r for r in records if r.get("download_s") is not None and r["status"] != "failed"]
    times = [r["download_s"] for r in ok]
    interval = config.ARXIV_MIN_INTERVAL_S + config.ARXIV_JITTER_S / 2
    versions_per_paper = len(records) / papers if papers else 0.0
    per_version = statistics.fmean(max(interval, t) for t in times) if times else interval
    api_calls = math.ceil(range_size / config.ARXIV_API_BATCH)
    est_s = api_calls * interval + range_size * versions_per_paper * per_version
    return {
        "papers": papers,
        "versions": len(records),
        "versions_per_paper": round(versions_per_paper, 3),
        "status": dict(Counter(r["status"] for r in records)),
        "kinds": dict(Counter(r.get("kind") for r in records if r.get("kind"))),
        "download_s_mean": round(statistics.fmean(times), 3) if times else None,
        "download_s_median": round(statistics.median(times), 3) if times else None,
        "download_s_p90": round(_pct(times, 0.9), 3) if times else None,
        "download_s_max": round(max(times), 3) if times else None,
        "archive_bytes_mean": round(
            statistics.fmean(r["archive_bytes"] for r in ok if "archive_bytes" in r)
        )
        if any("archive_bytes" in r for r in ok)
        else None,
        "bytes_before_mean": round(statistics.fmean(r.get("bytes_before", 0) for r in ok))
        if ok
        else None,
        "bytes_after_mean": round(statistics.fmean(r.get("bytes_after", 0) for r in ok))
        if ok
        else None,
        "pacing_interval_s": interval,
        "estimate_range_size": range_size,
        "estimate_s_per_version": round(per_version, 3),
        "estimate_hours_per_range": round(est_s / 3600, 2),
    }


def save_fixtures(records: list[dict], archive_dir: Path, fixtures_dir: Path) -> dict[str, str]:
    saved = {}
    for rec in records:
        kind, name = rec.get("kind"), rec.get("archive_name")
        if not kind or kind in saved or not name or not (archive_dir / name).exists():
            continue
        target = fixtures_dir / kind / name
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(archive_dir / name, target)
        saved[kind] = str(target)
    return saved


def probe_error(client: HttpClient, arxiv_id: str, version: int, fixtures_dir: Path) -> dict:
    """Request a version that does not exist and keep what arXiv answers (status, type, body)."""
    res = client.get(source_url(arxiv_id, version), headers={"Accept-Encoding": "identity"})
    info = {
        "url": res.url,
        "status": res.status,
        "content_type": res.headers.get("Content-Type"),
        "body_head": (res.content or b"")[:300].decode("utf-8", "replace"),
    }
    out = fixtures_dir / f"error_{res.status}"
    out.mkdir(parents=True, exist_ok=True)
    (out / "response.json").write_text(json.dumps(info, indent=2), encoding="utf-8")
    return info


def cmd_month_end(args: argparse.Namespace) -> None:
    client = HttpClient(get_arxiv_limiter(), config.USER_AGENT)

    def exists(ids: list[str]) -> set[str]:
        return {e.arxiv_id for e in api_query(client, ids)}

    last = find_last_id(args.yymm, exists, args.guess)
    print(f"last ID of {args.yymm}: {args.yymm}.{last:05d}  ({client.requests_sent} API requests)")


def cmd_run(args: argparse.Namespace) -> None:
    samples = sample_ids(args.per_range, args.seed)
    ids = [i for name in samples for i in samples[name]]
    setup_logging(args.work_dir, logging.INFO)
    client = HttpClient(get_arxiv_limiter(), config.USER_AGENT)
    summary = run(
        ids,
        data_root=args.data_root,
        work_dir=args.work_dir,
        client=client,
        keep_archives=True,
        references=False,
        progress=False,
        label="spike",
    )
    state = load_version_state(args.work_dir / "versions.jsonl")
    report = {"samples": samples, "run": summary, "ranges": {}}
    all_records = []
    for name, picked in samples.items():
        recs = [r for (i, _v), r in state.items() if i in set(picked)]
        all_records += recs
        report["ranges"][name] = analyse(recs, len(picked))
    report["overall"] = analyse(all_records, len(ids))
    report["fixtures"] = save_fixtures(all_records, args.work_dir / "archives", args.fixtures_dir)
    multi = max(ids, key=lambda i: summary_versions(state, i))
    report["error_probe"] = probe_error(
        client, multi, summary_versions(state, multi) + 1, args.fixtures_dir
    )
    (args.work_dir / "spike_report.json").write_text(json.dumps(report, indent=2), encoding="utf-8")

    print(f"{'range':<8}{'papers':>7}{'vers':>6}{'V/N':>6}{'t_mean':>8}{'t_p90':>7}{'est_h':>7}")
    for name, r in [*report["ranges"].items(), ("overall", report["overall"])]:
        print(
            f"{name:<8}{r['papers']:>7}{r['versions']:>6}{r['versions_per_paper']:>6.2f}"
            f"{r['download_s_mean'] or 0:>8.2f}{r['download_s_p90'] or 0:>7.2f}"
            f"{r['estimate_hours_per_range']:>7.2f}"
        )
    print("kinds:", report["overall"]["kinds"], "| status:", report["overall"]["status"])
    print("min gap between arXiv requests:", summary["min_request_gap_s"], "s")
    print("error probe:", report["error_probe"]["status"], report["error_probe"]["content_type"])


def summary_versions(state: dict, arxiv_id: str) -> int:
    return max((v for (i, v) in state if i == arxiv_id), default=0)


def cmd_snapshot_coverage(args: argparse.Namespace) -> None:
    ranges = {
        name: list(iter_id_range(start, end, config.MONTH_END))
        for name, (start, end) in config.RANGES.items()
    }
    wanted = {i for ids in ranges.values() for i in ids}
    found = load_snapshot(args.snapshot, wanted)
    print(f"{'range':<8}{'in snapshot':>12}{'missing':>9}")
    for name, ids in ranges.items():
        have = sum(i in found for i in ids)
        print(f"{name:<8}{have:>12}{len(ids) - have:>9}")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="GĐ1 spike tools")
    sub = parser.add_subparsers(dest="cmd", required=True)
    p = sub.add_parser("month-end", help="find the last arXiv ID of a month")
    p.add_argument("yymm")
    p.add_argument("--guess", type=int, default=30000)
    p.set_defaults(func=cmd_month_end)
    p = sub.add_parser("run", help="crawl a sample of every range and estimate crawl time")
    p.add_argument("--per-range", type=int, default=10)
    p.add_argument("--seed", type=int, default=2026)
    p.add_argument("--data-root", type=Path, default=Path("data/spike"))
    p.add_argument("--work-dir", type=Path, default=Path("work/spike"))
    p.add_argument("--fixtures-dir", type=Path, default=Path("data/fixtures"))
    p.set_defaults(func=cmd_run)
    p = sub.add_parser("snapshot-coverage", help="count range IDs present in the Kaggle snapshot")
    p.add_argument("--snapshot", type=Path, required=True)
    p.set_defaults(func=cmd_snapshot_coverage)
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()
