# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""Rate limiters. One shared instance for every arXiv host; a separate one for Semantic Scholar.

A limiter is used as a context manager around the *whole* HTTP call, body included::

    with limiter:
        response = session.get(url)

which guarantees both arXiv rules at once: at most one request in flight (the lock is
held for the duration of the call) and successive request starts at least
``min_interval_s`` (+ random jitter) apart.
"""

from __future__ import annotations

import random
import threading
import time
from collections.abc import Callable

from src import config


class RateLimiter:
    """Serialises calls and spaces their start times by ``min_interval_s`` + U(0, jitter_s)."""

    def __init__(
        self,
        min_interval_s: float,
        jitter_s: float = 0.0,
        *,
        name: str = "",
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
        rng: Callable[[], float] = random.random,
    ) -> None:
        self.min_interval_s = min_interval_s
        self.jitter_s = jitter_s
        self.name = name
        self._clock = clock
        self._sleep = sleep
        self._rng = rng
        self._lock = threading.RLock()
        self._next_start = float("-inf")
        self._last_start: float | None = None
        self.calls = 0
        self.last_gap_s: float | None = None  # gap between the starts of the last two calls
        self.min_gap_s: float | None = None  # smallest observed gap between call starts
        self.wait_s = 0.0  # cumulative time spent waiting for the limiter
        self.last_wait_s = 0.0

    def __enter__(self) -> RateLimiter:
        self._lock.acquire()
        try:
            self._wait_turn()
        except BaseException:
            self._lock.release()
            raise
        return self

    def __exit__(self, *exc_info: object) -> None:
        self._lock.release()

    def _wait_turn(self) -> None:
        wait_started = self._clock()
        while (delay := self._next_start - self._clock()) > 0:
            self._sleep(delay)
        now = self._clock()
        self.last_wait_s = now - wait_started
        self.wait_s += self.last_wait_s
        if self._last_start is not None:
            gap = now - self._last_start
            self.last_gap_s = gap
            self.min_gap_s = gap if self.min_gap_s is None else min(self.min_gap_s, gap)
        self._last_start = now
        self.calls += 1
        self._next_start = now + self.min_interval_s + self._rng() * self.jitter_s

    def hold(self) -> threading.RLock:
        """The lock alone, without taking a turn: keeps one connection across a body that
        needs several requests (Range resumes), each of which still waits its turn."""
        return self._lock

    def pause_for(self, seconds: float) -> None:
        """Delay the next call by at least ``seconds`` from now (backoff, circuit breaker)."""
        with self._lock:
            self._next_start = max(self._next_start, self._clock() + seconds)

    def set_min_interval(self, seconds: float) -> None:
        with self._lock:
            self.min_interval_s = seconds


_arxiv_limiter: RateLimiter | None = None
_arxiv_limiter_lock = threading.Lock()


def get_arxiv_limiter() -> RateLimiter:
    """The process-wide limiter shared by every arXiv host (API, OAI-PMH, /src/)."""
    global _arxiv_limiter
    with _arxiv_limiter_lock:
        if _arxiv_limiter is None:
            _arxiv_limiter = RateLimiter(
                config.ARXIV_MIN_INTERVAL_S, config.ARXIV_JITTER_S, name="arxiv"
            )
        return _arxiv_limiter


_s2_limiters: dict[bool, RateLimiter] = {}


def semantic_scholar_limiter(has_api_key: bool | None = None) -> RateLimiter:
    """The process-wide Semantic Scholar limiter, separate from arXiv so both run in parallel.

    With an API key S2 allows 1 request/s. Without one the shared pool allows
    100 requests per 5 minutes, i.e. one request every 3 s on average. Every caller
    gets the same instance (spec 0002), so two S2 crawls never double the rate.
    """
    if has_api_key is None:
        has_api_key = config.SEMANTIC_SCHOLAR_API_KEY is not None
    with _arxiv_limiter_lock:
        if has_api_key not in _s2_limiters:
            _s2_limiters[has_api_key] = RateLimiter(
                1.1 if has_api_key else 3.0, 0.2, name="semantic_scholar"
            )
        return _s2_limiters[has_api_key]
