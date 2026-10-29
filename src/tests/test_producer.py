# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
from __future__ import annotations

import json

import pytest

from src.crawler import producer
from src.crawler.discovery import DiscoveryResult
from src.crawler.ladder import DiscoveryLadder
from src.crawler.worklock import WorkDirLock
from src.run_pipeline import version_dir


class FakeQueue:
    def __init__(self, depths=()):
        self.published = []
        self._depths = list(depths)

    def publish(self, job):
        self.published.append((job.arxiv_id, job.version))
        return "1"

    def depth(self):
        return self._depths.pop(0) if self._depths else 0


def test_produce_publishes_only_versions_that_are_not_done(tmp_path, monkeypatch):
    data_root, work_dir = tmp_path / "corpus", tmp_path / "work"
    work_dir.mkdir()
    version_dir(data_root, "2606.00001", 1).mkdir(parents=True)
    (work_dir / "versions.jsonl").write_text(
        json.dumps({"arxiv_id": "2606.00001", "version": 1, "status": "ok"}) + "\n",
        encoding="utf-8",
    )

    def fake_discover(ids, client, root, **kwargs):
        result = DiscoveryResult()
        result.found = {"2606.00001": 2, "2606.00002": 1}
        return result

    monkeypatch.setattr(producer, "discover", fake_discover)
    queue = FakeQueue()

    out = producer.produce(
        ["2606.00001", "2606.00002"],
        client=None,
        job_queue=queue,
        data_root=data_root,
        work_dir=work_dir,
    )

    assert sorted(queue.published) == [("2606.00001", 2), ("2606.00002", 1)]
    assert out.published == 2 and out.found == {"2606.00001": 2, "2606.00002": 1}
    lock = WorkDirLock(work_dir)
    lock.acquire()  # produce() released the work-dir lock
    lock.release()


def test_wait_until_drained_polls_until_the_queue_is_empty():
    sleeps: list[float] = []
    queue = FakeQueue(depths=[3, 1, 0])
    producer.wait_until_drained(queue, 2.0, sleep=sleeps.append)
    assert sleeps == [2.0, 2.0]


def test_produce_refuses_a_second_run_on_the_same_work_dir(tmp_path, monkeypatch):
    monkeypatch.setattr(producer, "discover", lambda *a, **k: DiscoveryResult())
    work_dir = tmp_path / "work"
    held = WorkDirLock(work_dir)
    held.acquire()
    try:
        with pytest.raises(Exception, match="already used"):
            producer.produce(
                ["2606.00001"],
                client=None,
                job_queue=FakeQueue(),
                data_root=tmp_path / "corpus",
                work_dir=work_dir,
            )
    finally:
        held.release()


def _ids(n):
    return [f"2606.{k:05d}" for k in range(1, n + 1)]


def test_chunks_follow_the_ladder_and_jobs_are_published_per_chunk(tmp_path, monkeypatch):
    queue = FakeQueue()
    seen = []  # (chunk length, jobs already published when discover ran)

    def fake_discover(chunk, client, root, **kwargs):
        seen.append((len(chunk), len(queue.published)))
        result = DiscoveryResult()
        result.found = {i: 1 for i in chunk}
        return result

    monkeypatch.setattr(producer, "discover", fake_discover)
    backlogs = iter([0, 0, 12, 40])

    out = producer.produce(
        _ids(30),
        client=None,
        job_queue=queue,
        data_root=tmp_path / "corpus",
        work_dir=tmp_path / "work",
        backlog=lambda: next(backlogs),
        ladder=DiscoveryLadder(),
    )

    assert seen == [(5, 0), (5, 5), (10, 10), (10, 20)]
    assert out.tier_history == [5, 5, 10, 20]
    assert out.published == 30 and len(queue.published) == 30
    assert out.time_to_first_job_s is not None


def test_cached_ids_do_not_count_toward_the_batch_size(tmp_path, monkeypatch):
    queue = FakeQueue()
    seen = []
    cached = set(_ids(3))

    def fake_discover(chunk, client, root, **kwargs):
        seen.append(len(chunk))
        result = DiscoveryResult()
        result.found = {i: 1 for i in chunk}
        return result

    monkeypatch.setattr(producer, "discover", fake_discover)
    monkeypatch.setattr(
        producer, "read_metadata", lambda root, i: {"num_versions": 1} if i in cached else None
    )

    producer.produce(
        _ids(8),
        client=None,
        job_queue=queue,
        data_root=tmp_path / "corpus",
        work_dir=tmp_path / "work",
        ladder=DiscoveryLadder(),
    )

    assert seen == [8]  # 3 cached IDs ride along for free, 5 fresh IDs fill the first tier


def test_a_rejected_chunk_lowers_the_next_batch_size(tmp_path, monkeypatch):
    calls = []

    def fake_discover(chunk, client, root, **kwargs):
        calls.append(len(chunk))
        result = DiscoveryResult()
        result.found = {i: 1 for i in chunk}
        result.api_failed_batches = 1 if len(calls) == 1 else 0
        return result

    monkeypatch.setattr(producer, "discover", fake_discover)

    out = producer.produce(
        _ids(160),
        client=None,
        job_queue=FakeQueue(),
        data_root=tmp_path / "corpus",
        work_dir=tmp_path / "work",
        backlog=lambda: 1000,
        ladder=DiscoveryLadder(),
    )

    assert out.tier_history == [100, 50, 50]
    assert out.api_failed_batches == 1
