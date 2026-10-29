from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.crawler.queue import ParserJob
from src.parser.worker import build_parser, parse_job, process_one


class OneJobQueue:
    def __init__(self, job: ParserJob) -> None:
        self.job = job
        self.acked: list[str] = []
        self.retried: list[tuple[str, str, int]] = []

    def reclaim_idle(self, min_idle_ms: int, count: int = 10) -> int:
        return 0

    def claim(self, *, block_ms: int = 5000) -> ParserJob | None:
        job, self.job = self.job, None
        return job

    def ack(self, job_id: str) -> None:
        self.acked.append(job_id)

    def retry_or_dead_letter(self, job_id: str, error: str, max_attempts: int) -> None:
        self.retried.append((job_id, error, max_attempts))


def make_job(tmp_path: Path) -> ParserJob:
    version = tmp_path / "corpus" / "2606-00001" / "v1"
    version.mkdir(parents=True)
    (version / "main.tex").write_text(
        "\\documentclass{article}\n\\begin{document}Hello\\end{document}\n", encoding="utf-8"
    )
    return ParserJob.create("2606.00001", 1, "2606-00001/v1", {"title": "Hello"})


def test_parse_job_persists_idempotent_result(tmp_path):
    job = make_job(tmp_path)
    result = parse_job(job, tmp_path / "corpus", tmp_path / "parsed")
    output = tmp_path / "parsed" / f"{job.job_id}.json"

    assert result["root"] == "main.tex"
    assert json.loads(output.read_text(encoding="utf-8"))["job_id"] == job.job_id
    assert parse_job(job, tmp_path / "corpus", tmp_path / "parsed") == result


def test_process_one_acks_only_after_result_is_written(tmp_path):
    job = make_job(tmp_path)
    queue = OneJobQueue(job)

    assert process_one(queue, tmp_path / "corpus", tmp_path / "parsed") is True
    assert queue.acked == [job.job_id]
    assert not queue.retried


def test_process_one_does_not_ack_missing_version(tmp_path):
    job = ParserJob.create("2606.00001", 1, "2606-00001/v1", {})
    queue = OneJobQueue(job)

    assert process_one(queue, tmp_path / "corpus", tmp_path / "parsed") is False
    assert queue.acked == []
    assert queue.retried[0][0] == job.job_id


def test_worker_cli_generates_a_unique_default_consumer(monkeypatch):
    monkeypatch.delenv("WORKER_CONSUMER", raising=False)
    args = build_parser().parse_args([])

    assert args.consumer


def test_parse_job_rejects_path_escape(tmp_path):
    job = ParserJob.create("2606.00001", 1, "../outside", {})

    with pytest.raises(ValueError, match="outside corpus root"):
        parse_job(job, tmp_path / "corpus", tmp_path / "parsed")
