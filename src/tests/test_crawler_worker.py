# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
from __future__ import annotations

import json

from src.crawler import worker
from src.crawler.queue import DownloadJob


class FakeQueue:
    def __init__(self, jobs):
        self.jobs = list(jobs)
        self.acked: list[str] = []
        self.retried: list[tuple[str, str, int]] = []
        self.reclaimed_with: list[int] = []

    def reclaim_idle(self, min_idle_ms, count=10):
        self.reclaimed_with.append(min_idle_ms)
        return 0

    def claim(self, *, block_ms=5000):
        return self.jobs.pop(0) if self.jobs else None

    def ack(self, job_id):
        self.acked.append(job_id)

    def retry_or_dead_letter(self, job_id, error, max_attempts):
        self.retried.append((job_id, error, max_attempts))


def _ctx(tmp_path, parser_queue=None):
    return dict(
        client=None,
        data_root=tmp_path / "corpus",
        work_dir=tmp_path / "work",
        keep_extensions=frozenset({".tex", ".bib"}),
        parser_queue=parser_queue,
    )


def _patch(monkeypatch, status, published):
    monkeypatch.setattr(
        worker,
        "process_version",
        lambda client, arxiv_id, version, **kw: {
            "arxiv_id": arxiv_id,
            "version": version,
            "status": status,
        },
    )
    monkeypatch.setattr(
        worker, "_publish_parser_job", lambda q, root, i, v: published.append((i, v))
    )


def test_ok_version_is_recorded_published_to_the_parser_and_acked(tmp_path, monkeypatch):
    published: list = []
    _patch(monkeypatch, "ok", published)
    job = DownloadJob.create("2606.00001", 1)
    queue = FakeQueue([job])
    (tmp_path / "work").mkdir()

    assert worker.process_one(queue, **_ctx(tmp_path, parser_queue=object())) is True

    lines = (tmp_path / "work" / "versions.jsonl").read_text(encoding="utf-8").splitlines()
    assert [json.loads(x)["status"] for x in lines] == ["ok"]
    assert published == [("2606.00001", 1)]
    assert queue.acked == [job.job_id] and queue.retried == []


def test_no_tex_is_final_but_not_sent_to_the_parser(tmp_path, monkeypatch):
    published: list = []
    _patch(monkeypatch, "no_tex", published)
    job = DownloadJob.create("2606.00002", 1)
    queue = FakeQueue([job])
    (tmp_path / "work").mkdir()

    worker.process_one(queue, **_ctx(tmp_path, parser_queue=object()))

    assert published == []
    assert queue.acked == [job.job_id]


def test_failed_version_is_retried_not_acked(tmp_path, monkeypatch):
    _patch(monkeypatch, "failed", [])
    job = DownloadJob.create("2606.00003", 1)
    queue = FakeQueue([job])
    (tmp_path / "work").mkdir()

    worker.process_one(queue, **_ctx(tmp_path), max_attempts=3)

    assert queue.acked == []
    assert queue.retried[0][0] == job.job_id and queue.retried[0][2] == 3


def test_exception_is_retried_and_the_worker_keeps_going(tmp_path, monkeypatch):
    def boom(*a, **k):
        raise RuntimeError("disk full")

    monkeypatch.setattr(worker, "process_version", boom)
    job = DownloadJob.create("2606.00004", 1)
    queue = FakeQueue([job])
    (tmp_path / "work").mkdir()

    assert worker.process_one(queue, **_ctx(tmp_path)) is True
    assert queue.acked == [] and "disk full" in queue.retried[0][1]


def test_idle_queue_returns_false_and_reclaims_with_the_visibility_timeout(tmp_path):
    queue = FakeQueue([])
    assert worker.process_one(queue, **_ctx(tmp_path), visibility_timeout_ms=1234) is False
    assert queue.reclaimed_with == [1234]
