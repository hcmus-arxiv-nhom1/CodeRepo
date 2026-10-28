from __future__ import annotations

import json

from src.crawler.queue import ParserJob
from src.parser.worker import process_one


class LocalQueue:
    def __init__(self, jobs: list[ParserJob]) -> None:
        self.jobs = jobs
        self.acked: list[str] = []
        self.retried: list[str] = []

    def reclaim_idle(self, min_idle_ms: int, count: int = 10) -> int:
        return 0

    def claim(self, *, block_ms: int = 5000) -> ParserJob | None:
        return self.jobs.pop(0) if self.jobs else None

    def ack(self, job_id: str) -> None:
        self.acked.append(job_id)

    def retry_or_dead_letter(self, job_id: str, error: str, max_attempts: int) -> None:
        self.retried.append(job_id)


def test_multiple_workers_partition_jobs_without_duplicate_outputs(tmp_path):
    corpus = tmp_path / "corpus"
    jobs = []
    for number in range(4):
        paper_id = f"2606.{number + 1:05d}"
        relpath = f"2606-{number + 1:05d}/v1"
        version = corpus / relpath
        version.mkdir(parents=True)
        (version / "main.tex").write_text(
            "\\documentclass{article}\\begin{document}x\\end{document}", encoding="utf-8"
        )
        jobs.append(ParserJob.create(paper_id, 1, relpath, {}))

    queues = [LocalQueue(jobs[:2]), LocalQueue(jobs[2:])]
    for queue in queues:
        while process_one(queue, corpus, tmp_path / "parsed"):
            pass

    outputs = list((tmp_path / "parsed").glob("*.json"))
    job_ids = [json.loads(path.read_text(encoding="utf-8"))["job_id"] for path in outputs]
    assert len(outputs) == 4
    assert len(set(job_ids)) == 4
    assert sum(len(queue.acked) for queue in queues) == 4
