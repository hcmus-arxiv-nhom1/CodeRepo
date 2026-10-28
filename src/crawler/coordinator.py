# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""Cluster-wide arXiv pace: one coordinator hands out turns to every worker machine.

arXiv's Terms of Use apply to *all* machines under our control as a whole: one request in
flight and starts >= 3 s apart across the whole group. Adding workers therefore never
speeds up the arXiv lane; it only lets the other work (extraction, disk, Drive sync) run in
parallel. The coordinator keeps a single ``RateLimiter`` for the cluster and
``RemoteRateLimiter`` is a drop-in for it in ``HttpClient``::

    python -m src.crawler.coordinator --port 8780            # central machine
    limiter = RemoteRateLimiter("http://central:8780")       # in every worker
    client = HttpClient(limiter, config.USER_AGENT)

Protocol (JSON over HTTP, all POST): ``/turn`` waits for the lock and the pace, ``/lock``
waits for the lock only (``hold``), ``/release`` frees one level, ``/pause`` and
``/interval`` forward backoff and breaker decisions to everybody, ``/renew`` extends the
holder's lease and ``/stats`` reports the pace actually achieved.

The lock is a *lease*: the holder must renew it (``RemoteRateLimiter`` does so from a
heartbeat thread). A holder that stops renewing, because its process died, loses the lock
after ``lease_ttl_s`` and the next waiter takes over; a late ``/release`` from the old
holder is ignored because the owner no longer matches.
"""

from __future__ import annotations

import argparse
import json
import os
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from src import config
from src.crawler.ratelimit import RateLimiter

MIN_ALLOWED_INTERVAL_S = 3.0  # arXiv Terms of Use


def check_min_interval(value: float) -> float:
    if value < MIN_ALLOWED_INTERVAL_S:
        raise ValueError(
            f"pace {value} s is below the arXiv Terms of Use minimum of {MIN_ALLOWED_INTERVAL_S} s"
        )
    return value


class _State:
    """Cluster-wide limiter plus a re-entrant lease keyed by owner (worker id + thread)."""

    def __init__(self, limiter: RateLimiter, lease_ttl_s: float = 60.0) -> None:
        self.limiter = limiter
        self.lease_ttl_s = lease_ttl_s
        self._cv = threading.Condition()
        self._owner: str | None = None
        self._depth = 0
        self._expires_at = 0.0

    def _take(self, owner: str) -> None:
        with self._cv:
            while self._owner not in (None, owner):
                remaining = self._expires_at - time.monotonic()
                if remaining <= 0:  # the holder stopped renewing: it died, take the lease over
                    self._owner, self._depth = None, 0
                    break
                self._cv.wait(timeout=remaining)
            self._owner = owner
            self._depth += 1
            self._expires_at = time.monotonic() + self.lease_ttl_s

    def lock(self, owner: str) -> dict:
        self._take(owner)
        return {}

    def turn(self, owner: str) -> dict:
        self._take(owner)
        try:
            self.limiter._wait_turn()  # noqa: SLF001 - we hold the cluster lock ourselves
        except BaseException:
            self.release(owner)
            raise
        return {
            "calls": self.limiter.calls,
            "last_gap_s": self.limiter.last_gap_s,
            "min_gap_s": self.limiter.min_gap_s,
            "min_interval_s": self.limiter.min_interval_s,
        }

    def release(self, owner: str) -> dict:
        with self._cv:
            if self._owner == owner:
                self._depth -= 1
                if self._depth == 0:
                    self._owner = None
                    self._cv.notify_all()
        return {}

    def renew(self, owner: str) -> dict:
        with self._cv:
            held = self._owner == owner
            if held:
                self._expires_at = time.monotonic() + self.lease_ttl_s
        return {"held": held}

    def stats(self) -> dict:
        return {
            "calls": self.limiter.calls,
            "min_gap_s": self.limiter.min_gap_s,
            "min_interval_s": self.limiter.min_interval_s,
        }


def make_server(
    limiter: RateLimiter, host: str = "127.0.0.1", port: int = 0, *, lease_ttl_s: float = 60.0
):
    state = _State(limiter, lease_ttl_s)

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            body = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))) or b"{}")
            owner = body.get("owner", "")
            routes = {
                "/turn": lambda: state.turn(owner),
                "/lock": lambda: state.lock(owner),
                "/release": lambda: state.release(owner),
                "/renew": lambda: state.renew(owner),
                "/stats": state.stats,
                "/pause": lambda: limiter.pause_for(float(body["seconds"])) or {},
                "/interval": lambda: limiter.set_min_interval(float(body["seconds"])) or {},
            }
            handler = routes.get(self.path)
            if handler is None:
                self.send_error(404)
                return
            payload = json.dumps(handler()).encode()
            try:
                self.send_response(200)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                if self.path in ("/turn", "/lock"):
                    state.release(owner)  # caller vanished while waiting

        def log_message(self, *args: object) -> None:
            pass

    return ThreadingHTTPServer((host, port), Handler)


class RemoteRateLimiter:
    """Same interface as ``RateLimiter`` (what ``HttpClient`` uses), paced by the coordinator."""

    def __init__(self, url: str, worker: str = "", *, heartbeat_s: float = 10.0) -> None:
        self._url = url.rstrip("/")
        self._worker = worker
        self._heartbeat_s = heartbeat_s
        self._local = threading.RLock()  # keeps this process's threads from interleaving
        self._depth = 0  # nesting of turn/hold; only touched while holding self._local
        self._beat_stop: threading.Event | None = None
        self.calls = 0
        self.last_gap_s: float | None = None
        self.min_gap_s: float | None = None
        self.min_interval_s = config.ARXIV_MIN_INTERVAL_S
        self.wait_s = 0.0  # cumulative time spent waiting for the coordinator
        self.last_wait_s = 0.0

    def _owner(self) -> str:
        return f"{self._worker}:{threading.get_ident()}"

    def _post(self, path: str, *, owner: str | None = None, timeout: float = 3600, **body: object):
        data = json.dumps({"owner": owner or self._owner(), **body}).encode()
        req = urllib.request.Request(self._url + path, data=data, method="POST")
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read())

    def _heartbeat(self, owner: str, stop: threading.Event) -> None:
        while not stop.wait(self._heartbeat_s):
            try:
                self._post("/renew", owner=owner, timeout=5)
            except Exception:  # noqa: BLE001, S110 - coordinator unreachable: the lease just runs out
                pass

    def _beat_start(self) -> None:
        self._depth += 1
        if self._depth == 1:
            self._beat_stop = threading.Event()
            threading.Thread(
                target=self._heartbeat, args=(self._owner(), self._beat_stop), daemon=True
            ).start()

    def _beat_stop_one(self) -> None:
        self._depth -= 1
        if self._depth == 0 and self._beat_stop is not None:
            self._beat_stop.set()

    def _acquire(self, path: str) -> dict:
        self._local.acquire()
        try:
            self._beat_start()  # before the POST: renewals also cover the pace wait inside /turn
            try:
                started = time.monotonic()
                info = self._post(path)
                self.last_wait_s = time.monotonic() - started
                self.wait_s += self.last_wait_s
                return info
            except BaseException:
                self._beat_stop_one()
                raise
        except BaseException:
            self._local.release()
            raise

    def _release(self) -> None:
        try:
            self._post("/release")
        finally:
            self._beat_stop_one()
            self._local.release()

    def __enter__(self) -> RemoteRateLimiter:
        info = self._acquire("/turn")
        self.calls = info["calls"]
        self.last_gap_s = info["last_gap_s"]
        self.min_gap_s = info["min_gap_s"]
        self.min_interval_s = info["min_interval_s"]
        return self

    def __exit__(self, *exc_info: object) -> None:
        self._release()

    def hold(self) -> _Hold:
        return _Hold(self)

    def pause_for(self, seconds: float) -> None:
        self._post("/pause", seconds=seconds)

    def set_min_interval(self, seconds: float) -> None:
        self.min_interval_s = seconds
        self._post("/interval", seconds=seconds)

    def stats(self) -> dict:
        return self._post("/stats")


class _Hold:
    """``with limiter.hold():`` keeps the cluster lock without taking a turn."""

    def __init__(self, limiter: RemoteRateLimiter) -> None:
        self._limiter = limiter

    def __enter__(self) -> None:
        self._limiter._acquire("/lock")  # noqa: SLF001

    def __exit__(self, *exc_info: object) -> None:
        self._limiter._release()  # noqa: SLF001


def main() -> int:
    parser = argparse.ArgumentParser(description="Central arXiv pace coordinator")
    parser.add_argument("--host", default="0.0.0.0")  # noqa: S104 - workers are on other machines
    parser.add_argument("--port", type=int, default=8780)
    parser.add_argument(
        "--min-interval", type=float, default=float(os.environ.get("ARXIV_MIN_GAP_S", "3.0"))
    )
    parser.add_argument("--lease-ttl", type=float, default=60.0)
    args = parser.parse_args()
    interval = check_min_interval(args.min_interval)
    limiter = RateLimiter(interval, config.ARXIV_JITTER_S, name="cluster")
    limiter.pause_for(interval)  # a restart forgets the last request: wait one full pace first
    print(f"coordinator on {args.host}:{args.port}, pace {interval}s, lease {args.lease_ttl}s")
    make_server(limiter, args.host, args.port, lease_ttl_s=args.lease_ttl).serve_forever()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
