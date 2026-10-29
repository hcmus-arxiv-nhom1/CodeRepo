# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
import json
import threading
import time
import urllib.request

import pytest

from src.crawler.coordinator import RemoteRateLimiter, check_min_interval, make_server
from src.crawler.ratelimit import RateLimiter

INTERVAL = 0.05


@pytest.fixture
def coordinator():
    limiter = RateLimiter(INTERVAL, 0.0, name="cluster")
    server = make_server(limiter)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield limiter, f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()


def _run_workers(url: str, workers: int, jobs: int):
    starts: list[float] = []
    in_flight = []
    guard = threading.Lock()
    active = 0

    def work(k: int) -> None:
        nonlocal active
        limiter = RemoteRateLimiter(url, worker=f"w{k}")
        for _ in range(jobs):
            with limiter:
                with guard:
                    active += 1
                    in_flight.append(active)
                    starts.append(time.monotonic())
                time.sleep(0.01)
                with guard:
                    active -= 1

    threads = [threading.Thread(target=work, args=(k,)) for k in range(workers)]
    [t.start() for t in threads]
    [t.join() for t in threads]
    return sorted(starts), in_flight


@pytest.mark.parametrize("workers", [2, 4])
def test_cluster_keeps_pace_and_single_connection(coordinator, workers):
    limiter, url = coordinator
    starts, in_flight = _run_workers(url, workers, jobs=6)
    assert len(starts) == workers * 6
    assert max(in_flight) == 1  # never two requests in flight across the cluster
    gaps = [b - a for a, b in zip(starts, starts[1:], strict=False)]
    assert min(gaps) >= INTERVAL * 0.8  # Windows timers undershoot sleeps by a few ms
    assert limiter.min_gap_s >= INTERVAL * 0.8


def test_more_workers_do_not_speed_up_the_arxiv_lane(coordinator):
    _, url = coordinator
    t0 = time.monotonic()
    _run_workers(url, 1, jobs=12)
    one = time.monotonic() - t0
    t0 = time.monotonic()
    _run_workers(url, 4, jobs=3)
    four = time.monotonic() - t0
    assert four > one * 0.8


def test_hold_is_reentrant_and_blocks_other_workers(coordinator):
    _, url = coordinator
    a, b = RemoteRateLimiter(url, "a"), RemoteRateLimiter(url, "b")
    got_turn = threading.Event()
    with a.hold():
        with a:  # a Range resume inside a held body
            pass

        def other() -> None:
            with b:
                got_turn.set()

        t = threading.Thread(target=other)
        t.start()
        assert not got_turn.wait(0.2)  # b waits while a holds the body
    assert got_turn.wait(2)
    t.join()


def test_pause_and_interval_reach_the_cluster(coordinator):
    limiter, url = coordinator
    remote = RemoteRateLimiter(url, "a")
    remote.set_min_interval(0.2)
    assert limiter.min_interval_s == 0.2
    with remote:
        pass
    remote.pause_for(0.15)
    t0 = time.monotonic()
    with remote:
        pass
    assert time.monotonic() - t0 >= 0.1


def _raw(url: str, path: str, owner: str) -> dict:
    data = json.dumps({"owner": owner}).encode()
    req = urllib.request.Request(url + path, data=data, method="POST")
    with urllib.request.urlopen(req, timeout=5) as resp:
        return json.loads(resp.read())


@pytest.fixture
def short_lease_coordinator():
    limiter = RateLimiter(0.0, 0.0, name="cluster")
    server = make_server(limiter, lease_ttl_s=0.3)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    yield f"http://127.0.0.1:{server.server_address[1]}"
    server.shutdown()
    server.server_close()


def test_lease_of_a_dead_worker_expires(short_lease_coordinator):
    url = short_lease_coordinator
    _raw(url, "/turn", "dead:1")  # takes the lease, never renews, never releases
    b = RemoteRateLimiter(url, "b", heartbeat_s=0.05)
    t0 = time.monotonic()
    with b:
        waited = time.monotonic() - t0
    assert 0.2 <= waited < 2.0


def test_heartbeat_keeps_the_lease_of_a_slow_request(short_lease_coordinator):
    url = short_lease_coordinator
    a = RemoteRateLimiter(url, "a", heartbeat_s=0.05)
    b = RemoteRateLimiter(url, "b", heartbeat_s=0.05)
    got = threading.Event()

    def other() -> None:
        with b:
            got.set()

    with a:
        t = threading.Thread(target=other)
        t.start()
        assert not got.wait(1.0)  # a holds for 1 s, three times the lease TTL
    assert got.wait(2)
    t.join()


def test_late_release_of_an_expired_lease_is_ignored(short_lease_coordinator):
    url = short_lease_coordinator
    _raw(url, "/turn", "dead:1")
    c_got = threading.Event()

    def other() -> None:
        with RemoteRateLimiter(url, "c", heartbeat_s=0.05):
            c_got.set()

    with RemoteRateLimiter(url, "b", heartbeat_s=0.05):
        _raw(url, "/release", "dead:1")  # the dead worker wakes up late
        t = threading.Thread(target=other)
        t.start()
        assert not c_got.wait(0.5)  # b still owns the lease
    assert c_got.wait(2)
    t.join()


def test_remote_limiter_reports_wait_like_the_local_one(coordinator):
    _, url = coordinator
    a = RemoteRateLimiter(url, "a")
    with a:
        pass
    with a:
        pass
    assert a.wait_s > 0
    assert a.last_wait_s >= INTERVAL * 0.5


def test_stats_report_calls_and_the_smallest_gap(coordinator):
    _, url = coordinator
    a = RemoteRateLimiter(url, "a")
    for _ in range(3):
        with a:
            pass
    stats = a.stats()
    assert stats["calls"] == 3
    assert stats["min_gap_s"] >= INTERVAL * 0.8


def test_interval_never_goes_below_the_startup_pace(coordinator):
    limiter, url = coordinator
    RemoteRateLimiter(url, "a").set_min_interval(INTERVAL / 10)
    assert limiter.min_interval_s == INTERVAL


def test_coordinator_refuses_a_pace_below_the_terms_of_use():
    with pytest.raises(ValueError):
        check_min_interval(0.5)
    assert check_min_interval(3.0) == 3.0
