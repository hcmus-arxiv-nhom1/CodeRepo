# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""Polite HTTP client shared by every arXiv call (API, OAI-PMH, /src/).

- every request runs inside the shared ``RateLimiter`` (one connection, >= 3 s apart);
- retries connection errors, timeouts, 429 and 5xx with ``2**n`` s backoff + jitter,
  honouring ``Retry-After``; at most ``max_attempts`` attempts per URL;
- 3 consecutive 403s trip a circuit breaker: pause 15-30 min and slow pacing to 5 s.
  Too many trips in one run raise ``BlockedError`` so the run stops instead of hammering.
- ``get`` reads small answers (API, OAI-PMH) into memory; ``stream`` hands a download
  body to a consumer as a file-like object, so a source archive is unpacked while it
  arrives and never written to disk. A body cut mid-transfer is resumed with a Range
  request from the byte it stopped at (no backoff, only the 3 s pace): arXiv /src/
  answers Range with ``206 Partial Content`` (checked on 27/10/2026).
"""

from __future__ import annotations

import io
import logging
import random
import re
import time
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from requests.structures import CaseInsensitiveDict

from src.crawler.ratelimit import RateLimiter

log = logging.getLogger(__name__)

RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})
CHUNK_SIZE = 1 << 16
MAX_RESUMES = 5  # Range resumes of one body before restarting it from byte 0
MAX_RESUME_REJECTIONS = 3  # 429/5xx answers to one Range request before restarting from byte 0


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


class _CutBody(Exception):
    """A body broke mid-transfer and could not be resumed; restart it from byte 0."""


# Network failures worth retrying. ChunkedEncodingError (connection dropped mid-body) is
# not a ConnectionError subclass in requests, so it is listed explicitly.
_TRANSIENT_ERRORS = (
    requests.ConnectionError,
    requests.Timeout,
    requests.exceptions.ChunkedEncodingError,
    requests.exceptions.ContentDecodingError,
)
_CONTENT_RANGE_RE = re.compile(r"bytes (\d+)-(\d+)/(\d+)")


@dataclass
class FetchResult:
    url: str
    status: int
    headers: Mapping[str, str] = field(default_factory=dict)
    content: bytes | None = None  # body of get() (first 4 KiB for error statuses)
    value: Any = None  # what the consumer of stream() returned
    nbytes: int = 0  # body bytes received
    attempts: int = 1
    resumes: int = 0  # Range resumes of the successful attempt
    elapsed_s: float = 0.0  # full fetch across attempts, excluding measured limiter waits
    wait_s: float = 0.0  # limiter/backoff waits across all attempts and Range resumes


class ResumableBody(io.RawIOBase):
    """Response body as a readable stream that survives connections cut mid-transfer.

    On a broken transfer it asks the server for the rest (``Range: bytes=<pos>-``)
    through the shared limiter, checks ``Content-Range`` and carries on, so whatever
    is reading (gzip, tarfile) never notices.
    """

    def __init__(self, client: HttpClient, url: str, resp, headers: Mapping[str, str]):
        self._client, self._url, self._headers = client, url, dict(headers)
        self._resp = resp
        self._chunks = resp.iter_content(CHUNK_SIZE)
        self._buf = b""
        self.position = 0
        self.resumes = 0
        length = resp.headers.get("Content-Length")
        encoded = resp.headers.get("Content-Encoding", "identity") != "identity"
        self.total = int(length) if length is not None and not encoded else None

    def readable(self) -> bool:
        return True

    def readinto(self, b) -> int:
        while not self._buf:
            try:
                self._buf = next(self._chunks)
            except StopIteration:
                if self.total is None or self.position >= self.total:
                    return 0  # end of body
                self._resume(f"body ended at {self.position} of {self.total} bytes")
            except _TRANSIENT_ERRORS as exc:
                self._resume(f"{type(exc).__name__} at {self.position} bytes")
        n = min(len(b), len(self._buf))
        b[:n] = self._buf[:n]
        self._buf = self._buf[n:]
        self.position += n
        return n

    def _resume(self, why: str) -> None:
        if self.resumes >= MAX_RESUMES:
            raise _CutBody(f"{why}; {MAX_RESUMES} resumes already")
        self.resumes += 1
        self._resp.close()
        log.warning("GET %s cut mid-body (%s), resuming with Range", self._url, why)
        range_headers = {**self._headers, "Range": f"bytes={self.position}-"}
        for rejected in range(1, MAX_RESUME_REJECTIONS + 1):
            try:
                resp = self._client._send(self._url, range_headers, stream=True)
            except _TRANSIENT_ERRORS as exc:
                raise _CutBody(f"{why}; resume request failed: {exc}") from exc
            if resp.status_code not in RETRYABLE_STATUS:
                break
            # Rate limited or overloaded: keep the bytes already read, wait, ask again.
            retry_after = resp.headers.get("Retry-After")
            resp.close()
            if rejected == MAX_RESUME_REJECTIONS:
                raise _CutBody(f"{why}; resume answered {resp.status_code} {rejected} times")
            if resp.status_code == 429:
                self._client._pace_up()
            self._client._backoff(rejected, retry_after)
        m = _CONTENT_RANGE_RE.match(resp.headers.get("Content-Range", ""))
        if resp.status_code != 206 or m is None or int(m.group(1)) != self.position:
            resp.close()
            raise _CutBody(f"{why}; resume answered {resp.status_code}")
        self._resp, self._chunks = resp, resp.iter_content(CHUNK_SIZE)

    def drain(self) -> None:
        """Read whatever is left (used before closing a fully consumed body)."""
        while self.read(CHUNK_SIZE):
            pass

    def close(self) -> None:
        self._resp.close()
        super().close()


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
        adaptive_pacing: bool = False,
        pacing_max_s: float = 10.0,
        pacing_up: float = 1.5,
        pacing_down_s: float = 0.5,
        pacing_streak: int = 20,
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
        # Adaptive pacing (spec 0004): a 429 slows the pace by x pacing_up (never above
        # pacing_max_s); pacing_streak successes in a row win back pacing_down_s, never
        # below the pace the client started with. Off by default: the pace only ever grows.
        self.adaptive_pacing = adaptive_pacing
        self.pacing_max_s = pacing_max_s
        self.pacing_up = pacing_up
        self.pacing_down_s = pacing_down_s
        self.pacing_streak = pacing_streak
        self._base_interval_s = limiter.min_interval_s
        self._ok_streak = 0

    # ----------------------------------------------------------------- public

    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str] | None = None,
        max_attempts: int | None = None,
    ) -> FetchResult:
        """GET a small answer into memory (``FetchResult.content``).

        Returns the result for 200 and for non-retryable statuses (e.g. 404) so the caller
        can decide; raises ``FetchError`` once retries are exhausted. ``max_attempts``
        lowers the client default for a caller that has a cheaper fallback (discovery).
        """
        return self._fetch(url, headers, None, max_attempts)

    def stream(
        self,
        url: str,
        consume: Callable[[ResumableBody, Mapping[str, str]], Any],
        *,
        headers: Mapping[str, str] | None = None,
    ) -> FetchResult:
        """GET ``url`` and pass the 200 body to ``consume(body, headers)`` while it arrives.

        ``FetchResult.value`` is what ``consume`` returned. Exceptions raised by
        ``consume`` (e.g. a corrupt archive) propagate to the caller.
        """
        return self._fetch(url, headers, consume)

    # ----------------------------------------------------------------- internals

    def _send(self, url: str, headers: Mapping[str, str] | None, *, stream: bool):
        """One request, after waiting its turn (also used for Range resumes)."""
        with self.limiter:  # re-entrant: _fetch already holds the lock
            self.requests_sent += 1
            gap = self.limiter.last_gap_s if self.limiter.calls > 1 else None
            resp = self.session.get(
                url, headers=headers or None, timeout=self.timeout, stream=stream
            )
            # "gap" = time since the previous request *started*: the auditable pacing figure.
            log.info(
                "GET %s -> %d (%s, gap %s)",
                url,
                resp.status_code,
                headers.get("Range", "full body") if headers else "full body",
                f"{gap:.2f} s" if gap is not None else "-",
            )
            return resp

    def _fetch(self, url: str, headers, consume, max_attempts: int | None = None) -> FetchResult:
        last_reason, last_status = "no attempt made", None
        wait_before = self.limiter.wait_s
        fetch_started = time.perf_counter()
        attempts = max_attempts or self.max_attempts
        for attempt in range(1, attempts + 1):
            retry_after: str | None = None
            try:
                with self.limiter.hold():  # whole body, resumes included: one connection
                    start = time.perf_counter()
                    resp = self._send(url, headers, stream=consume is not None)
                    try:
                        status = resp.status_code
                        retry_after = resp.headers.get("Retry-After")
                        content, value, nbytes, resumes = None, None, 0, 0
                        if status == 200 and consume is not None:
                            body = ResumableBody(self, url, resp, headers or {})
                            try:
                                value = consume(body, CaseInsensitiveDict(resp.headers))
                            finally:
                                nbytes, resumes = body.position, body.resumes
                                body.close()
                        elif status == 200:
                            content = resp.content
                            nbytes = len(content)
                        else:
                            content = resp.content[:4096]  # error page snippet, for diagnosis
                        elapsed = time.perf_counter() - start
                    finally:
                        resp.close()
            except _CutBody as exc:
                # The server answered, then the transfer broke beyond repair: not a rate
                # limit, so no backoff; the limiter's 3 s pace is the only wait.
                last_reason, last_status = f"cut mid-body: {exc}", None
                log.warning("GET %s restarts from byte 0 (%s), attempt %d", url, exc, attempt)
                continue
            except _TRANSIENT_ERRORS as exc:
                last_reason, last_status = f"{type(exc).__name__}: {exc}", None
                log.warning("GET %s failed (%s), attempt %d", url, last_reason, attempt)
                if attempt < attempts:  # nothing left to wait for after the last attempt
                    self._backoff(attempt, None)
                continue

            log.info(
                "GET %s done: %d (%d B, %.2f s, attempt %d, %d resume(s))",
                url,
                status,
                nbytes,
                elapsed,
                attempt,
                resumes,
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
            if status == 429:
                self._pace_up()
            if status in RETRYABLE_STATUS:
                last_reason = f"HTTP {status}"
                if attempt < attempts:
                    self._backoff(attempt, retry_after)
                continue
            if 200 <= status < 300:
                self._pace_ok()
            return FetchResult(
                url=url,
                status=status,
                headers=CaseInsensitiveDict(resp.headers),
                content=content,
                value=value,
                nbytes=nbytes,
                attempts=attempt,
                resumes=resumes,
                elapsed_s=max(
                    0.0, time.perf_counter() - fetch_started - (self.limiter.wait_s - wait_before)
                ),
                wait_s=self.limiter.wait_s - wait_before,
            )
        raise FetchError(url, last_reason, last_status, attempts)

    def _pace_floor(self) -> float:
        # after a circuit-breaker trip the slower breaker pace is the floor
        return max(self._base_interval_s, self.breaker_interval_s if self.breaker_trips else 0.0)

    def _pace_up(self) -> None:
        if not self.adaptive_pacing:
            return
        self._ok_streak = 0
        current = self.limiter.min_interval_s
        new = min(self.pacing_max_s, round(current * self.pacing_up, 2))
        if new > current:
            self.limiter.set_min_interval(new)
            log.info("adaptive pacing: %.2f -> %.2f s after HTTP 429", current, new)

    def _pace_ok(self) -> None:
        if not self.adaptive_pacing:
            return
        self._ok_streak += 1
        current, floor = self.limiter.min_interval_s, self._pace_floor()
        if self._ok_streak >= self.pacing_streak and current > floor:
            new = max(floor, round(current - self.pacing_down_s, 2))
            self.limiter.set_min_interval(new)
            self._ok_streak = 0
            log.info(
                "adaptive pacing: %.2f -> %.2f s after %d successes",
                current,
                new,
                self.pacing_streak,
            )

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
