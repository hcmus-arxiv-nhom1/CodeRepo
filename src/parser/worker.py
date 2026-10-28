from __future__ import annotations

import argparse
import json
import os
import socket
import time
from pathlib import Path

from src.crawler.queue import ParserJob, RedisJobQueue
from src.parser.gather import gather

MAX_ATTEMPTS = 3
RECLAIM_IDLE_MS = 60_000


def parse_job(job: ParserJob, corpus_root: Path, output_root: Path) -> dict:
    corpus_root = Path(corpus_root).resolve()
    version_dir = (corpus_root / job.corpus_relpath).resolve()
    if not version_dir.is_relative_to(corpus_root):
        raise ValueError("job path is outside corpus root")
    if not version_dir.is_dir():
        raise FileNotFoundError(version_dir)

    result = gather(version_dir)
    payload = {
        "job_id": job.job_id,
        "arxiv_id": job.arxiv_id,
        "version": job.version,
        "schema_version": job.schema_version,
        "root": result.root,
        "files": result.files,
        "unused": result.unused,
        "missing": result.missing,
        "other_roots": result.other_roots,
    }
    output_root = Path(output_root)
    output_root.mkdir(parents=True, exist_ok=True)
    output_path = output_root / f"{job.job_id}.json"
    temp_path = output_path.with_suffix(f".{os.getpid()}.tmp")
    temp_path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    temp_path.replace(output_path)
    return payload


def process_one(queue: RedisJobQueue, corpus_root: Path, output_root: Path) -> bool:
    queue.reclaim_idle(RECLAIM_IDLE_MS)
    job = queue.claim(block_ms=0)
    if job is None:
        return False
    try:
        parse_job(job, corpus_root, output_root)
    except Exception as exc:
        queue.retry_or_dead_letter(job.job_id, str(exc), MAX_ATTEMPTS)
        return False
    queue.ack(job.job_id)
    return True


def run_worker(queue: RedisJobQueue, corpus_root: Path, output_root: Path) -> None:
    while True:
        if not process_one(queue, corpus_root, output_root):
            time.sleep(0.5)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Consume parser jobs from Redis Streams.")
    parser.add_argument("--redis-url", default="redis://redis:6379/0")
    parser.add_argument("--queue-stream", default="parser-jobs")
    parser.add_argument("--queue-group", default="parser-workers")
    parser.add_argument("--consumer", default=f"{socket.gethostname()}-{os.getpid()}")
    parser.add_argument("--corpus-root", type=Path, default=Path("data/corpus"))
    parser.add_argument("--output-root", type=Path, default=Path("data/parsed"))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    queue = RedisJobQueue(args.redis_url, args.queue_stream, args.queue_group, args.consumer)
    run_worker(queue, args.corpus_root, args.output_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
