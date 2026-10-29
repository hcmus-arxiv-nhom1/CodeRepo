# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""Crawler worker: take download jobs from Redis and fetch each version through the cluster.

Every arXiv request of this process is paced by the coordinator (``RemoteRateLimiter``), so
running several workers never raises the request rate. A version is acknowledged only after
its record is in ``versions.jsonl``; ``ok`` versions are then handed to the parser stream.
A job that fails stays pending and comes back after the visibility timeout; after
``max_attempts`` it moves to ``<stream>:dead``.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import time
from pathlib import Path

from src import config
from src.crawler.coordinator import RemoteRateLimiter
from src.crawler.http_client import HttpClient
from src.crawler.queue import DownloadJob, RedisJobQueue
from src.run_pipeline import FINAL_STATUSES, _now, _publish_parser_job, process_version

VISIBILITY_TIMEOUT_S = 300
MAX_ATTEMPTS = 3


def append_record(path: Path, rec: dict) -> None:
    """One ``write`` on an ``O_APPEND`` descriptor: lines from several workers never interleave."""
    data = (json.dumps(rec, ensure_ascii=False) + "\n").encode()
    flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT | getattr(os, "O_BINARY", 0)
    fd = os.open(path, flags, 0o644)
    try:
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)


def process_job(
    job: DownloadJob,
    *,
    client: HttpClient,
    data_root: Path,
    work_dir: Path,
    keep_extensions: frozenset[str],
    parser_queue: RedisJobQueue | None,
) -> str:
    rec = process_version(
        client, job.arxiv_id, job.version, data_root=data_root, keep_extensions=keep_extensions
    )
    rec["finished_at"] = _now()
    append_record(work_dir / "versions.jsonl", rec)
    if rec["status"] == "ok" and parser_queue is not None:
        _publish_parser_job(parser_queue, data_root, job.arxiv_id, job.version)
    return rec["status"]


def process_one(
    queue: RedisJobQueue,
    *,
    client: HttpClient,
    data_root: Path,
    work_dir: Path,
    keep_extensions: frozenset[str],
    parser_queue: RedisJobQueue | None,
    visibility_timeout_ms: int = VISIBILITY_TIMEOUT_S * 1000,
    max_attempts: int = MAX_ATTEMPTS,
) -> bool:
    queue.reclaim_idle(visibility_timeout_ms)
    job = queue.claim(block_ms=1000)
    if job is None:
        return False
    try:
        status = process_job(
            job,
            client=client,
            data_root=data_root,
            work_dir=work_dir,
            keep_extensions=keep_extensions,
            parser_queue=parser_queue,
        )
    except Exception as exc:  # noqa: BLE001 - one bad job must not stop the worker
        queue.retry_or_dead_letter(job.job_id, str(exc), max_attempts)
        return True
    if status in FINAL_STATUSES:
        queue.ack(job.job_id)
    else:
        queue.retry_or_dead_letter(job.job_id, f"version status {status}", max_attempts)
    return True


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download arXiv sources from the Redis queue.")
    parser.add_argument("--redis-url", default="redis://redis:6379/0")
    parser.add_argument("--coordinator-url", default="http://coordinator:8780")
    parser.add_argument("--download-stream", default="download-jobs")
    parser.add_argument("--download-group", default="crawler-workers")
    parser.add_argument("--parser-stream", default="parser-jobs")
    parser.add_argument("--parser-group", default="parser-workers")
    parser.add_argument("--consumer", default=f"{socket.gethostname()}-{os.getpid()}")
    parser.add_argument("--data-root", type=Path, default=Path(config.DEFAULT_DATA_ROOT))
    parser.add_argument("--work-dir", type=Path, default=Path(config.DEFAULT_WORK_DIR))
    parser.add_argument("--visibility-timeout-s", type=int, default=VISIBILITY_TIMEOUT_S)
    parser.add_argument("--max-attempts", type=int, default=MAX_ATTEMPTS)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    args.work_dir.mkdir(parents=True, exist_ok=True)
    queue = RedisJobQueue(
        args.redis_url,
        args.download_stream,
        args.download_group,
        args.consumer,
        job_cls=DownloadJob,
    )
    parser_queue = RedisJobQueue(
        args.redis_url, args.parser_stream, args.parser_group, args.consumer
    )
    limiter = RemoteRateLimiter(args.coordinator_url, worker=args.consumer)
    client = HttpClient(limiter, config.USER_AGENT)
    print(f"crawler worker {args.consumer} -> {args.coordinator_url}")
    while True:
        handled = process_one(
            queue,
            client=client,
            data_root=args.data_root,
            work_dir=args.work_dir,
            keep_extensions=config.KEEP_EXTENSIONS,
            parser_queue=parser_queue,
            visibility_timeout_ms=args.visibility_timeout_s * 1000,
            max_attempts=args.max_attempts,
        )
        if not handled:
            time.sleep(0.5)


if __name__ == "__main__":
    raise SystemExit(main())
