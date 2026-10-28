# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""Producer: discover the metadata of a range of IDs, queue one download job per missing version.

The workers do the downloading; this process only talks to arXiv for discovery (through the
same coordinator) and then waits for the queue to drain so it can write ``run_summary.json``
and ``failed.csv`` in the same shape as ``run_pipeline``. It takes the work-dir lock so two
producers never publish for the same work directory.
"""

from __future__ import annotations

import json
import logging
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

from src import config
from src.crawler.coordinator import RemoteRateLimiter
from src.crawler.discovery import discover, read_metadata
from src.crawler.http_client import HttpClient
from src.crawler.ladder import DiscoveryLadder
from src.crawler.queue import DownloadJob, RedisJobQueue
from src.crawler.worklock import WorkDirLock
from src.run_pipeline import (
    JsonlWriter,
    _now,
    _start_references,
    build_parser,
    is_done,
    load_version_state,
    select_ids,
    setup_logging,
    summarize,
    write_failed_csv,
)

log = logging.getLogger(__name__)


@dataclass
class Produced:
    found: dict[str, int]
    missing: dict[str, str]
    published: int
    discovery_s: float
    tier_history: list[int] = field(default_factory=list)
    api_failed_batches: int = 0
    oai_fallbacks: int = 0
    discovery_requests: int = 0  # every arXiv request made while discovering
    time_to_first_job_s: float | None = None


def _requests(client: HttpClient | None) -> int:
    return client.requests_sent if client is not None else 0


def _take_chunk(remaining: deque[str], size: int, data_root: Path, refresh: bool) -> list[str]:
    """Next IDs to discover: cached IDs are free, up to ``size`` uncached ones are charged."""
    chunk, fresh = [], 0
    while remaining and fresh < size:
        arxiv_id = remaining.popleft()
        if refresh or read_metadata(data_root, arxiv_id) is None:
            fresh += 1
        chunk.append(arxiv_id)
    return chunk


def produce(
    ids: list[str],
    *,
    client: HttpClient | None,
    job_queue: RedisJobQueue,
    data_root: Path,
    work_dir: Path,
    backlog: Callable[[], int] = lambda: 0,
    ladder: DiscoveryLadder | None = None,
    snapshot: Path | None = None,
    refresh_metadata: bool = False,
) -> Produced:
    ladder = ladder or DiscoveryLadder.from_env()
    data_root.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)
    lock = WorkDirLock(work_dir)
    lock.acquire()  # raises WorkDirLockedError if another run uses this work_dir
    disc_log = JsonlWriter(work_dir / "discovery.jsonl")
    out = Produced({}, {}, 0, 0.0)
    remaining = deque(ids)
    started = time.perf_counter()
    try:
        while remaining:
            size = ladder.next_size(backlog())
            chunk = _take_chunk(remaining, size, data_root, refresh_metadata)
            out.tier_history.append(size)
            requests_before = _requests(client)
            disc = discover(
                chunk,
                client,
                data_root,
                snapshot=snapshot,
                refresh=refresh_metadata,
                journal=disc_log.write,
            )
            out.discovery_requests += _requests(client) - requests_before
            out.found.update(disc.found)
            out.missing.update(disc.missing)
            out.discovery_s += disc.elapsed_s
            out.api_failed_batches += disc.api_failed_batches
            out.oai_fallbacks += disc.oai_fallbacks
            state = load_version_state(work_dir / "versions.jsonl")
            for arxiv_id in chunk:
                for version in range(1, disc.found.get(arxiv_id, 0) + 1):
                    if is_done(state.get((arxiv_id, version)), data_root):
                        continue
                    job_queue.publish(DownloadJob.create(arxiv_id, version))
                    out.published += 1
                    if out.time_to_first_job_s is None:
                        out.time_to_first_job_s = round(time.perf_counter() - started, 3)
            if disc.api_failed_batches:
                ladder.on_rate_limited()
            log.info("chunk of %d IDs (tier %d): %d jobs so far", len(chunk), size, out.published)
        return out
    finally:
        disc_log.close()
        lock.release()


def wait_until_drained(
    queue: RedisJobQueue, poll_s: float, sleep: Callable[[float], None] = time.sleep
) -> None:
    """Pending plus not-yet-delivered jobs reach 0 when all are acked or dead-lettered."""
    while queue.depth() > 0:
        sleep(poll_s)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    parser.add_argument("--coordinator-url", default="http://coordinator:8780")
    parser.add_argument("--download-stream", default="download-jobs")
    parser.add_argument("--download-group", default="crawler-workers")
    parser.add_argument("--poll-s", type=float, default=5.0)
    parser.add_argument("--no-wait", action="store_true", help="publish and exit")
    args = parser.parse_args(argv)
    if not args.redis_url:
        raise SystemExit("--redis-url is required")
    ids, label = select_ids(args)
    print(f"{len(ids)} IDs: {ids[0]} -> {ids[-1]}")
    if args.dry_run:
        return 0

    setup_logging(args.work_dir, logging.INFO)
    started_at, t0 = _now(), time.perf_counter()
    queue = RedisJobQueue(
        args.redis_url,
        args.download_stream,
        args.download_group,
        "producer",
        job_cls=DownloadJob,
    )
    limiter = RemoteRateLimiter(args.coordinator_url, worker="producer")
    client = HttpClient(limiter, config.USER_AGENT)
    parser_queue = RedisJobQueue(args.redis_url, args.queue_stream, args.queue_group, "producer")
    out = produce(
        ids,
        client=client,
        job_queue=queue,
        data_root=args.data_root,
        work_dir=args.work_dir,
        backlog=lambda: queue.depth() + parser_queue.depth(),
        snapshot=args.snapshot,
        refresh_metadata=args.refresh_metadata,
    )
    ref_thread = None if args.no_references else _start_references(list(out.found), args.data_root)
    if args.no_wait:
        return 0
    wait_until_drained(queue, args.poll_s)
    if ref_thread is not None:
        ref_thread.join()

    state = load_version_state(args.work_dir / "versions.jsonl")
    summary = {
        "label": label,
        "first_id": ids[0],
        "last_id": ids[-1],
        "started_at": started_at,
        "finished_at": _now(),
        "wall_s": round(time.perf_counter() - t0, 1),
        "discovery_s": round(out.discovery_s, 1),
        "time_to_first_job_s": out.time_to_first_job_s,
        "discovery_api_requests": out.discovery_requests - out.oai_fallbacks,
        "discovery_oai_fallbacks": out.oai_fallbacks,
        "discovery_api_failed_batches": out.api_failed_batches,
        "discovery_tier_history": out.tier_history,
        "jobs_published": out.published,
        "dead_letter": int(queue.client.xlen(queue.dead_letter_stream)),
        "coordinator": limiter.stats(),
        **summarize(ids, out.found, out.missing, state, args.data_root),
    }
    summary["failed_rows"] = write_failed_csv(
        args.work_dir / "failed.csv", ids, out.found, out.missing, state
    )
    (args.work_dir / "run_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
