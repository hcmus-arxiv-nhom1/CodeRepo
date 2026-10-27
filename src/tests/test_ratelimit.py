# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
import threading
import time

from src.crawler.ratelimit import RateLimiter, semantic_scholar_limiter
from src.tests.helpers import FakeClock


def make(interval=3.0, jitter=0.0, rng=lambda: 0.0):
    clock = FakeClock()
    return clock, RateLimiter(interval, jitter, clock=clock, sleep=clock.sleep, rng=rng)


def test_call_starts_are_spaced_by_interval_plus_jitter():
    clock, limiter = make(3.0, 0.5, rng=lambda: 1.0)
    starts = []
    for _ in range(3):
        with limiter:
            starts.append(clock.t)
            clock.t += 0.2  # the request itself
    assert starts == [0.0, 3.5, 7.0]
    assert limiter.calls == 3
    assert limiter.min_gap_s == 3.5


def test_slow_request_does_not_add_extra_wait():
    clock, limiter = make(3.0)
    with limiter:
        clock.t += 10.0  # a large download
    with limiter:
        assert clock.t == 10.0


def test_pause_and_slower_pacing():
    clock, limiter = make(3.0)
    with limiter:
        pass
    limiter.pause_for(60.0)
    with limiter:
        assert clock.t == 60.0
    limiter.set_min_interval(5.0)  # applies from the next scheduled call on
    with limiter:
        assert clock.t == 63.0
    with limiter:
        assert clock.t == 68.0


def test_only_one_call_in_flight():
    limiter = RateLimiter(0.0)
    active, peak = 0, 0
    lock = threading.Lock()

    def worker():
        nonlocal active, peak
        for _ in range(20):
            with limiter:
                with lock:
                    active += 1
                    peak = max(peak, active)
                time.sleep(0.001)
                with lock:
                    active -= 1

    threads = [threading.Thread(target=worker) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert peak == 1


def test_semantic_scholar_pacing_depends_on_api_key():
    assert semantic_scholar_limiter(has_api_key=True).min_interval_s < 2
    assert semantic_scholar_limiter(has_api_key=False).min_interval_s >= 3


def test_semantic_scholar_limiter_is_shared():
    """Two callers (pipeline thread, notebook re-run) must not get two separate budgets."""
    assert semantic_scholar_limiter(has_api_key=True) is semantic_scholar_limiter(True)
    assert semantic_scholar_limiter(has_api_key=False) is semantic_scholar_limiter(False)
