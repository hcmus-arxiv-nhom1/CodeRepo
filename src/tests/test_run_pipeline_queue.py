from __future__ import annotations

import pytest

from src.crawler.queue import ParserJob
from src.crawler.scheduler import AdaptiveBatchScheduler
from src.run_pipeline import build_parser, run
from src.tests.test_run_pipeline import FakeArxiv, client_for


class RecordingQueue:
    def __init__(self, fail: bool = False) -> None:
        self.jobs: list[ParserJob] = []
        self.fail = fail
        self.completed = 0

    def publish(self, job: ParserJob) -> str:
        if self.fail:
            raise ConnectionError("redis down")
        self.jobs.append(job)
        self.completed += 1
        return f"{len(self.jobs)}-0"

    def depth(self) -> int:
        return max(0, len(self.jobs) - self.completed)

    def completed_count(self) -> int:
        return self.completed

    def published_count(self) -> int:
        return len(self.jobs)


def test_run_publishes_jobs_after_completed_tex_versions(tmp_path):
    queue = RecordingQueue()
    run(
        ["2606.00001", "2606.00003"],
        data_root=tmp_path / "corpus",
        work_dir=tmp_path / "work",
        client=client_for(FakeArxiv()),
        references=False,
        progress=False,
        job_queue=queue,
    )

    assert len(queue.jobs) == 1
    job = queue.jobs[0]
    assert job.arxiv_id == "2606.00001"
    assert (tmp_path / "corpus" / job.corpus_relpath / "main.tex").is_file()


def test_run_retries_publication_after_queue_failure(tmp_path):
    with pytest.raises(ConnectionError, match="redis down"):
        run(
            ["2606.00001"],
            data_root=tmp_path / "corpus",
            work_dir=tmp_path / "work",
            client=client_for(FakeArxiv()),
            references=False,
            progress=False,
            job_queue=RecordingQueue(fail=True),
        )

    successful_queue = RecordingQueue()
    run(
        ["2606.00001"],
        data_root=tmp_path / "corpus",
        work_dir=tmp_path / "work",
        client=client_for(FakeArxiv()),
        references=False,
        progress=False,
        job_queue=successful_queue,
    )
    assert len(successful_queue.jobs) == 1


def test_cli_accepts_redis_queue_and_adaptive_scheduler_options():
    args = build_parser().parse_args(
        ["--range", "thien", "--redis-url", "redis://redis:6379/0", "--queue-seed", "20"]
    )
    assert args.redis_url == "redis://redis:6379/0"
    assert args.queue_seed == 20


def test_adaptive_scheduler_is_used_in_queue_mode(tmp_path):
    queue = RecordingQueue()
    scheduler = AdaptiveBatchScheduler(seed=1, max_batch=1, step=1, max_prefetch=1)
    run(
        ["2606.00001"],
        data_root=tmp_path / "corpus",
        work_dir=tmp_path / "work",
        client=client_for(FakeArxiv()),
        references=False,
        progress=False,
        job_queue=queue,
        scheduler=scheduler,
    )
    assert len(queue.jobs) == 1


def test_queue_mode_finishes_when_mix_has_no_tex_and_tex(tmp_path):
    queue = RecordingQueue()
    scheduler = AdaptiveBatchScheduler(seed=1, max_batch=1, step=1, max_prefetch=1)
    summary = run(
        ["2606.00003", "2606.00001"],
        data_root=tmp_path / "corpus",
        work_dir=tmp_path / "work",
        client=client_for(FakeArxiv()),
        references=False,
        progress=False,
        job_queue=queue,
        scheduler=scheduler,
    )
    assert summary["versions_processed_this_run"] == 2
    assert len(queue.jobs) == 1
    assert queue.jobs[0].arxiv_id == "2606.00001"


def test_queue_mode_discovers_in_seed_batches_before_more_ids(tmp_path, monkeypatch):
    import src.run_pipeline as pipeline
    from src.crawler.discovery import DiscoveryResult

    calls: list[list[str]] = []
    fake = FakeArxiv()

    def discover_batch(ids, client, data_root, **kwargs):
        calls.append(list(ids))
        return DiscoveryResult(found={arxiv_id: 1 for arxiv_id in ids})

    monkeypatch.setattr(pipeline, "discover", discover_batch)
    monkeypatch.setattr(
        pipeline,
        "process_version",
        lambda client, arxiv_id, version, **kwargs: {
            "status": "no_tex",
            "arxiv_id": arxiv_id,
            "version": version,
            "total_s": 0.0,
        },
    )
    queue = RecordingQueue()
    scheduler = AdaptiveBatchScheduler(seed=2, max_batch=2, step=2, max_prefetch=4)

    pipeline.run(
        [f"2606.{number:05d}" for number in range(1, 5)],
        data_root=tmp_path / "corpus",
        work_dir=tmp_path / "work",
        client=client_for(fake),
        references=False,
        progress=False,
        job_queue=queue,
        scheduler=scheduler,
    )

    assert calls == [["2606.00001", "2606.00002"], ["2606.00003", "2606.00004"]]


def test_trial_data_paths_are_distinct(tmp_path):
    assert not (tmp_path / "data" / "trial-100" / "corpus").exists()
    assert not (tmp_path / "work" / "trial-100").exists()
