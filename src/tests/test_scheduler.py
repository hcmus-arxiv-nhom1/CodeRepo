from __future__ import annotations

from src.crawler.scheduler import AdaptiveBatchScheduler


def test_scheduler_starts_with_configured_seed():
    scheduler = AdaptiveBatchScheduler(seed=10)

    assert scheduler.next_batch(queue_depth=0, worker_progress=True) == 10


def test_scheduler_grows_in_bounded_steps_when_backlog_is_low():
    scheduler = AdaptiveBatchScheduler(seed=10, step=10, low_watermark=20, max_batch=40)

    assert scheduler.next_batch(queue_depth=0, worker_progress=True) == 10
    assert scheduler.next_batch(queue_depth=0, worker_progress=True) == 20
    assert scheduler.next_batch(queue_depth=0, worker_progress=True) == 30


def test_scheduler_holds_when_backlog_is_high():
    scheduler = AdaptiveBatchScheduler(seed=10, step=10, high_watermark=100)

    assert scheduler.next_batch(queue_depth=100, worker_progress=True) == 0


def test_scheduler_pauses_without_worker_progress():
    scheduler = AdaptiveBatchScheduler(seed=10)

    assert scheduler.next_batch(queue_depth=0, worker_progress=False) == 0


def test_scheduler_caps_each_batch_and_available_prefetch():
    scheduler = AdaptiveBatchScheduler(
        seed=20, step=20, low_watermark=20, max_batch=30, max_prefetch=40
    )

    assert scheduler.next_batch(queue_depth=0, worker_progress=True) == 20
    assert scheduler.next_batch(queue_depth=0, worker_progress=True) == 30
    assert scheduler.next_batch(queue_depth=35, worker_progress=True) == 5
