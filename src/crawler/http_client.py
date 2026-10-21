# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""Polite HTTP client shared by every arXiv call (API, OAI-PMH, /src/).

- every request runs inside the shared ``RateLimiter`` (one connection, >= 3 s apart);
- retries connection errors, timeouts, 429 and 5xx with ``2**n`` s backoff + jitter,
  honouring ``Retry-After``; at most ``max_attempts`` attempts per URL;
- 3 consecutive 403s trip a circuit breaker: pause 15-30 min and slow pacing to 5 s.
  Too many trips in one run raise ``BlockedError`` so the run stops instead of hammering.
- downloads stream to ``<dest>.part`` and are renamed only once the body is complete.
"""

from __future__ import annotations

import logging
import os
import random
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from requests.structures import CaseInsensitiveDict

from src.crawler.ratelimit import RateLimiter

log = logging.getLogger(__name__)

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
CHUNK_SIZE = 1 << 16


class FetchError(Exception):
    """A URL could not be fetched after all retries."""

    def __init__(self, url: str, reason: str, status: int | None = None, attempts: int = 0):
        super().__init__(f"{url}: {reason} (status={status}, attempts={attempts})")
        self.url = url
        self.reason = reason
        self.status = status
        self.attempts = attempts


class BlockedError(FetchError):
    """arXiv keeps answering 403 even after circuit-breaker pauses: stop the run."""


class _IncompleteBody(Exception):
    pass


# Network failures worth retrying. ChunkedEncodingError (connection dropped mid-body) is
# not a ConnectionError subclass in requests, so it is listed explicitly.
_TRANSIENT_ERRORS = (
    requests.ConnectionError,
    requests.Timeout,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ContentDecodingError,
    _IncompleteBody,
)


@dataclass
class FetchResult:
    url: str
    status: int
    headers: Mapping[str, str] = field(default_factory=dict)
    content: bytes | None = None  # body when not streamed (first 4 KiB for error statuses)
    path: Path | None = None  # file holding the body, when streamed
    nbytes: int = 0
    attempts: int = 1
    elapsed_s: float = 0.0  # request + transfer time of the successful attempt


class HttpClient:
    def __init__(
        self,
        limiter: RateLimiter,
        user_agent: str,
        *,
        session: requests.Session | None = None,
        timeout: tuple[float, float] = (15.0, 120.0),
        max_attempts: int = 5,
        backoff_base_s: float = 2.0,
        max_backoff_s: float = 600.0,
        forbidden_threshold: int = 3,
        breaker_pause_s: tuple[float, float] = (900.0, 1800.0),
        breaker_interval_s: float = 5.0,
        max_breaker_trips: int = 3,
        rng: Callable[[], float] = random.random,
    ) -> None:
        self.limiter = limiter
        if session is None:
            session = requests.Session()
            adapter = HTTPAdapter(pool_connections=1, pool_maxsize=1, max_retries=0)
            session.mount("https://", adapter)
            session.mount("http://", adapter)
        session.headers["User-Agent"] = user_agent
        self.session = session
        self.timeout = timeout
        self.max_attempts = max_attempts
        self.backoff_base_s = backoff_base_s
        self.max_backoff_s = max_backoff_s
        self.forbidden_threshold = forbidden_threshold
        self.breaker_pause_s = breaker_pause_s
        self.breaker_interval_s = breaker_interval_s
        self.max_breaker_trips = max_breaker_trips
        self._rng = rng
        self._consecutive_403 = 0
        self.breaker_trips = 0
        self.requests_sent = 0

    def get(
        self,
        url: str,
        *,
        dest: Path | None = None,
        headers: Mapping[str, str] | None = None,
    ) -> FetchResult:
        """GET ``url``. With ``dest`` the body is streamed to that file.

        Returns the result for 200 and for non-retryable statuses (e.g. 404) so the caller
        can decide; raises ``FetchError`` once retries are exhausted.
        """
        last_reason, last_status = "no attempt made", None
        for attempt in range(1, self.max_attempts + 1):
            retry_after: str | None = None
            try:
                with self.limiter:
                    self.requests_sent += 1
                    gap = self.limiter.last_gap_s if self.limiter.calls > 1 else None
                    start = time.perf_counter()
                    resp = self.session.get(
                        url, headers=headers, timeout=self.timeout, stream=dest is not None
                    )
                    try:
                        status = resp.status_code
                        retry_after = resp.headers.get("Retry-After")
                        content, nbytes = None, 0
                        if status == 200:
                            if dest is not None:
                                nbytes = self._stream_to(resp, dest)
                            else:
                                content = resp.content
                                nbytes = len(content)
                        else:
                            content = resp.content[:4096]  # error page snippet, for diagnosis
                        elapsed = time.perf_counter() - start
                    finally:
                        resp.close()
            except _TRANSIENT_ERRORS as exc:
                last_reason, last_status = f"{type(exc).__name__}: {exc}", None
                log.warning("GET %s failed (%s), attempt %d", url, last_reason, attempt)
                self._backoff(attempt, None)
                continue

            # "gap" = time since the previous request *started*: the auditable pacing figure.
            log.info(
                "GET %s -> %d (%d B, %.2f s, attempt %d, gap %s)",
                url,
                status,
                nbytes,
                elapsed,
                attempt,
                f"{gap:.2f} s" if gap is not None else "-",
            )
            last_status = status
            if status == 403:
                last_reason = "HTTP 403"
                self._consecutive_403 += 1
                if self._consecutive_403 >= self.forbidden_threshold:
                    self._trip_breaker(url, attempt)
                else:
                    self._backoff(attempt, retry_after)
                continue
            self._consecutive_403 = 0
            if status in RETRYABLE_STATUS:
                last_reason = f"HTTP {status}"
                self._backoff(attempt, retry_after)
                continue
            return FetchResult(
                url=url,
                status=status,
                headers=CaseInsensitiveDict(resp.headers),
                content=content,
                path=dest if status == 200 else None,
                nbytes=nbytes,
                attempts=attempt,
                elapsed_s=elapsed,
            )
        raise FetchError(url, last_reason, last_status, self.max_attempts)

    def _stream_to(self, resp: requests.Response, dest: Path) -> int:
        dest.parent.mkdir(parents=True, exist_ok=True)
        part = dest.with_name(dest.name + ".part")
        nbytes = 0
        try:
            with open(part, "wb") as fh:
                for chunk in resp.iter_content(CHUNK_SIZE):
                    fh.write(chunk)
                    nbytes += len(chunk)
            expected = resp.headers.get("Content-Length")
            encoded = resp.headers.get("Content-Encoding", "identity") != "identity"
            if expected is not None and not encoded and int(expected) != nbytes:
                raise _IncompleteBody(f"got {nbytes} of {expected} bytes")
            os.replace(part, dest)
        except BaseException:
            part.unlink(missing_ok=True)
            raise
        return nbytes

    def _backoff(self, attempt: int, retry_after: str | None) -> None:
        delay = None
        if retry_after:
            try:
                delay = float(retry_after)
            except ValueError:
                delay = None  # HTTP-date form: fall back to exponential backoff
        if delay is None:
            delay = self.backoff_base_s**attempt + self._rng()
        delay = min(delay, self.max_backoff_s)
        log.info("backing off %.1f s before next arXiv request", delay)
        self.limiter.pause_for(delay)

    def _trip_breaker(self, url: str, attempt: int) -> None:
        self._consecutive_403 = 0
        self.breaker_trips += 1
        if self.breaker_trips > self.max_breaker_trips:
            raise BlockedError(url, "repeated HTTP 403 after circuit-breaker pauses", 403, attempt)
        low, high = self.breaker_pause_s
        pause = low + self._rng() * (high - low)
        self.limiter.set_min_interval(max(self.limiter.min_interval_s, self.breaker_interval_s))
        log.error(
            "circuit breaker #%d: %d consecutive 403s, pausing %.0f s, pacing now %.1f s",
            self.breaker_trips,
            self.forbidden_threshold,
            pause,
            self.limiter.min_interval_s,
        )
        self.limiter.pause_for(pause)
