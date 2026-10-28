# Crawler Workers with a Global Rate Limiter Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Run N `crawler-worker` containers (default 3, set by `CRAWLER_WORKERS` in Docker Compose) that download arXiv sources from a Redis job queue while every arXiv request goes through one shared coordinator limiter.

**Architecture:** A producer discovers metadata and publishes one `DownloadJob` per missing version to a Redis stream. Workers claim jobs, download with an `HttpClient` whose limiter is a `RemoteRateLimiter`, append the result to `versions.jsonl`, and hand `ok` versions to the existing parser stream. The coordinator (exactly one instance) already serialises turns; this plan adds lease expiry so a dead worker cannot hold the lock forever.

**Tech Stack:** Python 3.12, Redis Streams (`redis` 7.2.1), stdlib `http.server`, pytest, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-10-28-crawler-workers-global-limiter-design.md`

## Global Constraints

- arXiv Terms of Use: one request in flight and request starts at least **3 s** apart, for all workers together. The coordinator refuses a pace below `3.0` s.
- The coordinator runs as **exactly one instance**. Never scale it.
- A worker never sends an arXiv request except through `RemoteRateLimiter`. If the coordinator is unreachable the worker waits; it does not fall back to a local limiter.
- Lease TTL default `60` s; heartbeat period default `10` s (must stay well under the TTL).
- Job visibility timeout default `300` s; `MAX_ATTEMPTS` default `3`; failed jobs go to `<stream>:dead`.
- Python 3.12. New source files start with the line `# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.` and pass `ruff check`.
- Reuse existing code: `RedisJobQueue`, `process_version`, `discover`, `WorkDirLock`, `summarize`, `write_failed_csv`.

## Notes before starting

- **`config.ARXIV_MIN_INTERVAL_S` is currently `0.5` in the working tree** (uncommitted edit of `src/config.py`; the committed value is `3.0`). The coordinator in this plan takes its pace from `--min-interval` / `ARXIV_MIN_GAP_S` (default `3.0`) and rejects anything below `3.0`, so the cluster does not depend on that value. The single-process `run_pipeline` path still reads `config.ARXIV_MIN_INTERVAL_S`; restore it to `3.0` before any real crawl.
- **`RemoteRateLimiter` has no `wait_s`/`last_wait_s`**, but `HttpClient._fetch` reads `limiter.wait_s`. Using it with `HttpClient` raises `AttributeError` today; Task 1 fixes that.
- Many files in the repo are untracked or modified from earlier uncommitted work (`coordinator.py`, `queue.py`, `test_coordinator.py`, ...). Commit steps below add only the files each task touches, which for those files includes the earlier uncommitted work. Confirm with the user before the first commit.
- Run tests from `D:\introds\CodeRepo` with `python -m pytest src/tests/<file> -v`.

## File Structure

- Modify `src/crawler/coordinator.py` — lease expiry, `/renew`, `/stats`, heartbeat, `wait_s`, pace floor, startup cooldown.
- Modify `src/tests/test_coordinator.py` — tests for the above.
- Modify `src/crawler/queue.py` — `DownloadJob`, shared payload mixin, `job_cls` parameter.
- Modify `src/tests/test_queue.py` — `DownloadJob` round trip.
- Create `src/crawler/worker.py` — download worker (`process_one`, `run_worker`, CLI).
- Create `src/tests/test_crawler_worker.py`.
- Create `src/crawler/producer.py` — discovery + publish + wait + summary (CLI).
- Create `src/tests/test_producer.py`.
- Modify `compose.yaml`, `.env.example`, `README.md` — services, variables, usage.

---

### Task 1: Coordinator lease expiry, heartbeat and stats

**Files:**
- Modify: `src/crawler/coordinator.py` (replace the whole file with the code in Step 3)
- Test: `src/tests/test_coordinator.py` (append)

**Interfaces:**
- Produces: `make_server(limiter, host="127.0.0.1", port=0, *, lease_ttl_s=60.0)`; `RemoteRateLimiter(url, worker="", *, heartbeat_s=10.0)` with attributes `calls`, `last_gap_s`, `min_gap_s`, `min_interval_s`, `wait_s`, `last_wait_s` and methods `hold()`, `pause_for(seconds)`, `set_min_interval(seconds)`, `stats() -> dict` (`calls`, `min_gap_s`, `min_interval_s`); `check_min_interval(value: float) -> float` (raises `ValueError` below `3.0`); HTTP routes `/renew` and `/stats`.

- [ ] **Step 1: Write the failing tests**

Append to `src/tests/test_coordinator.py` (add `import json`, `import urllib.request` and `from src.crawler.coordinator import check_min_interval` to the imports at the top):

```python
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


def test_coordinator_refuses_a_pace_below_the_terms_of_use():
    with pytest.raises(ValueError):
        check_min_interval(0.5)
    assert check_min_interval(3.0) == 3.0
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `python -m pytest src/tests/test_coordinator.py -v`
Expected: collection error `ImportError: cannot import name 'check_min_interval'`.

- [ ] **Step 3: Replace `src/crawler/coordinator.py`**

```python
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
                    state.release(owner)  # the caller vanished while waiting: do not hold for nobody

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
            except Exception:  # noqa: BLE001 - coordinator unreachable: the lease just runs out
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
```

- [ ] **Step 4: Run all coordinator tests**

Run: `python -m pytest src/tests/test_coordinator.py -v`
Expected: all PASS, including the original four tests and the six new ones.

- [ ] **Step 5: Lint and run the wider suite**

Run: `ruff check src/crawler/coordinator.py src/tests/test_coordinator.py ; python -m pytest src/tests -q`
Expected: ruff clean (fix any finding in the new code), tests pass.

- [ ] **Step 6: Commit**

```bash
git add src/crawler/coordinator.py src/tests/test_coordinator.py
git commit -m "feat: coordinator lease expiry, heartbeat, stats and pace floor"
```

---

### Task 2: `DownloadJob` and a generic job queue

**Files:**
- Modify: `src/crawler/queue.py`
- Test: `src/tests/test_queue.py` (append)

**Interfaces:**
- Produces: `DownloadJob(job_id, arxiv_id, version, schema_version=1)` with `DownloadJob.create(arxiv_id: str, version: int)`, `to_fields()`, `from_fields(fields)`; `RedisJobQueue(..., client=None, job_cls=ParserJob)`.

- [ ] **Step 1: Write the failing test**

Append to `src/tests/test_queue.py` (change its import to `from src.crawler.queue import DownloadJob, ParserJob, RedisJobQueue`):

```python
def test_download_job_id_is_stable_and_the_queue_roundtrips_it():
    a = DownloadJob.create("2606.30734", 2)
    b = DownloadJob.create("2606.30734", 2)
    assert a.job_id == b.job_id
    assert a.job_id != DownloadJob.create("2606.30734", 1).job_id

    queue = RedisJobQueue(
        "redis://unused", "download-jobs", "crawler-workers", "c1", client=FakeRedis(),
        job_cls=DownloadJob,
    )
    queue.publish(a)
    queue.publish(b)  # same version: ignored
    job = queue.claim(block_ms=0)
    assert job == a and isinstance(job, DownloadJob)
    assert queue.claim(block_ms=0) is None
```

- [ ] **Step 2: Run it to verify it fails**

Run: `python -m pytest src/tests/test_queue.py -v`
Expected: ImportError for `DownloadJob`.

- [ ] **Step 3: Implement**

In `src/crawler/queue.py`, add a mixin above `ParserJob`, make `ParserJob` use it, add `DownloadJob`, and add `job_cls`:

```python
class _Payload:
    """Stream payload shared by every job type: one JSON field."""

    def to_fields(self) -> dict[str, str]:
        return {"payload": json.dumps(asdict(self), ensure_ascii=False)}

    @classmethod
    def from_fields(cls, fields: dict[bytes | str, bytes | str]):
        raw = fields.get(b"payload", fields.get("payload", "{}"))
        if isinstance(raw, bytes):
            raw = raw.decode()
        return cls(**json.loads(raw))


@dataclass(frozen=True)
class DownloadJob(_Payload):
    job_id: str
    arxiv_id: str
    version: int
    schema_version: int = 1

    @classmethod
    def create(cls, arxiv_id: str, version: int, schema_version: int = 1) -> DownloadJob:
        key = f"{arxiv_id}:v{version}:download-v{schema_version}"
        return cls(hashlib.sha256(key.encode()).hexdigest(), arxiv_id, version, schema_version)
```

Change `class ParserJob:` to `class ParserJob(_Payload):` and delete its own `to_fields` and `from_fields` methods (the mixin provides identical ones). In `RedisJobQueue.__init__` add the keyword `job_cls: type = ParserJob` after `client`, store `self.job_cls = job_cls`, and replace both `ParserJob.from_fields(fields)` calls (in `claim` and `claim_message`) with `self.job_cls.from_fields(fields)`.

- [ ] **Step 4: Run the queue and parser tests**

Run: `python -m pytest src/tests/test_queue.py src/tests/test_queue_integration.py src/tests/test_parser_worker.py -v`
Expected: all PASS (parser jobs behave exactly as before).

- [ ] **Step 5: Commit**

```bash
git add src/crawler/queue.py src/tests/test_queue.py
git commit -m "feat: DownloadJob and job_cls on the Redis job queue"
```

---

### Task 3: Crawler download worker

**Files:**
- Create: `src/crawler/worker.py`
- Test: `src/tests/test_crawler_worker.py`

**Interfaces:**
- Consumes: `RedisJobQueue` (Task 2), `RemoteRateLimiter` (Task 1), `process_version`, `_publish_parser_job`, `_now`, `FINAL_STATUSES` from `src.run_pipeline`.
- Produces: `append_record(path: Path, rec: dict) -> None`; `process_job(job, *, client, data_root, work_dir, keep_extensions, parser_queue) -> str` (returns the version status); `process_one(queue, *, client, data_root, work_dir, keep_extensions, parser_queue, visibility_timeout_ms=300_000, max_attempts=3) -> bool` (True if a job was handled); `main(argv=None) -> int`.

- [ ] **Step 1: Write the failing tests**

`src/tests/test_crawler_worker.py`:

```python
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
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest src/tests/test_crawler_worker.py -v`
Expected: `ModuleNotFoundError: No module named 'src.crawler.worker'`.

- [ ] **Step 3: Implement `src/crawler/worker.py`**

```python
# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""Crawler worker: take download jobs from Redis and fetch each version through the cluster.

Every arXiv request of this process is paced by the coordinator (``RemoteRateLimiter``), so
running several workers never raises the request rate. A version is acknowledged only after
its record is in ``versions.jsonl``; ``ok`` versions are then handed to the parser stream.
A job that fails stays pending and comes back after the visibility timeout; after
``max_attempts`` it moves to ``<stream>:dead``.
"""

from __future__ import annotations

import argparse
import json
import os
import socket
import time
from pathlib import Path

from src import config
from src.crawler.coordinator import RemoteRateLimiter
from src.crawler.http_client import HttpClient
from src.crawler.queue import DownloadJob, RedisJobQueue
from src.run_pipeline import FINAL_STATUSES, _now, _publish_parser_job, process_version

VISIBILITY_TIMEOUT_S = 300
MAX_ATTEMPTS = 3


def append_record(path: Path, rec: dict) -> None:
    """One ``write`` on an ``O_APPEND`` descriptor: lines from several workers never interleave."""
    data = (json.dumps(rec, ensure_ascii=False) + "\n").encode()
    flags = os.O_WRONLY | os.O_APPEND | os.O_CREAT | getattr(os, "O_BINARY", 0)
    fd = os.open(path, flags, 0o644)
    try:
        os.write(fd, data)
        os.fsync(fd)
    finally:
        os.close(fd)


def process_job(
    job: DownloadJob,
    *,
    client: HttpClient,
    data_root: Path,
    work_dir: Path,
    keep_extensions: frozenset[str],
    parser_queue: RedisJobQueue | None,
) -> str:
    rec = process_version(
        client, job.arxiv_id, job.version, data_root=data_root, keep_extensions=keep_extensions
    )
    rec["finished_at"] = _now()
    append_record(work_dir / "versions.jsonl", rec)
    if rec["status"] == "ok" and parser_queue is not None:
        _publish_parser_job(parser_queue, data_root, job.arxiv_id, job.version)
    return rec["status"]


def process_one(
    queue: RedisJobQueue,
    *,
    client: HttpClient,
    data_root: Path,
    work_dir: Path,
    keep_extensions: frozenset[str],
    parser_queue: RedisJobQueue | None,
    visibility_timeout_ms: int = VISIBILITY_TIMEOUT_S * 1000,
    max_attempts: int = MAX_ATTEMPTS,
) -> bool:
    queue.reclaim_idle(visibility_timeout_ms)
    job = queue.claim(block_ms=1000)
    if job is None:
        return False
    try:
        status = process_job(
            job,
            client=client,
            data_root=data_root,
            work_dir=work_dir,
            keep_extensions=keep_extensions,
            parser_queue=parser_queue,
        )
    except Exception as exc:  # noqa: BLE001 - one bad job must not stop the worker
        queue.retry_or_dead_letter(job.job_id, str(exc), max_attempts)
        return True
    if status in FINAL_STATUSES:
        queue.ack(job.job_id)
    else:
        queue.retry_or_dead_letter(job.job_id, f"version status {status}", max_attempts)
    return True


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Download arXiv sources from the Redis queue.")
    parser.add_argument("--redis-url", default="redis://redis:6379/0")
    parser.add_argument("--coordinator-url", default="http://coordinator:8780")
    parser.add_argument("--download-stream", default="download-jobs")
    parser.add_argument("--download-group", default="crawler-workers")
    parser.add_argument("--parser-stream", default="parser-jobs")
    parser.add_argument("--parser-group", default="parser-workers")
    parser.add_argument("--consumer", default=f"{socket.gethostname()}-{os.getpid()}")
    parser.add_argument("--data-root", type=Path, default=Path(config.DEFAULT_DATA_ROOT))
    parser.add_argument("--work-dir", type=Path, default=Path(config.DEFAULT_WORK_DIR))
    parser.add_argument("--visibility-timeout-s", type=int, default=VISIBILITY_TIMEOUT_S)
    parser.add_argument("--max-attempts", type=int, default=MAX_ATTEMPTS)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    args.work_dir.mkdir(parents=True, exist_ok=True)
    queue = RedisJobQueue(
        args.redis_url,
        args.download_stream,
        args.download_group,
        args.consumer,
        job_cls=DownloadJob,
    )
    parser_queue = RedisJobQueue(
        args.redis_url, args.parser_stream, args.parser_group, args.consumer
    )
    limiter = RemoteRateLimiter(args.coordinator_url, worker=args.consumer)
    client = HttpClient(limiter, config.USER_AGENT)
    print(f"crawler worker {args.consumer} -> {args.coordinator_url}")
    while True:
        handled = process_one(
            queue,
            client=client,
            data_root=args.data_root,
            work_dir=args.work_dir,
            keep_extensions=config.KEEP_EXTENSIONS,
            parser_queue=parser_queue,
            visibility_timeout_ms=args.visibility_timeout_s * 1000,
            max_attempts=args.max_attempts,
        )
        if not handled:
            time.sleep(0.5)


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run tests and lint**

Run: `python -m pytest src/tests/test_crawler_worker.py -v ; ruff check src/crawler/worker.py src/tests/test_crawler_worker.py`
Expected: 5 PASS, ruff clean.

- [ ] **Step 5: Commit**

```bash
git add src/crawler/worker.py src/tests/test_crawler_worker.py
git commit -m "feat: crawler worker consuming download jobs through the coordinator"
```

---

### Task 4: Producer (discovery, publish, wait, summary)

**Files:**
- Create: `src/crawler/producer.py`
- Test: `src/tests/test_producer.py`

**Interfaces:**
- Consumes: `DownloadJob`, `RedisJobQueue` (Task 2), `RemoteRateLimiter.stats()` (Task 1), `discover`, `WorkDirLock`, and from `src.run_pipeline`: `JsonlWriter`, `load_version_state`, `is_done`, `summarize`, `write_failed_csv`, `build_parser`, `select_ids`, `setup_logging`, `_start_references`.
- Produces: `produce(ids, *, client, job_queue, data_root, work_dir, snapshot=None, refresh_metadata=False) -> Produced` where `Produced` has `found: dict[str,int]`, `missing: dict[str,str]`, `published: int`, `discovery_s: float`; `wait_until_drained(queue, poll_s, sleep=time.sleep) -> None`; `main(argv=None) -> int`.

- [ ] **Step 1: Write the failing tests**

`src/tests/test_producer.py`:

```python
# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
from __future__ import annotations

import json

import pytest

from src.crawler import producer
from src.crawler.discovery import DiscoveryResult
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
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest src/tests/test_producer.py -v`
Expected: `ModuleNotFoundError: No module named 'src.crawler.producer'`.

- [ ] **Step 3: Implement `src/crawler/producer.py`**

```python
# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""Producer: discover the metadata of a range of IDs, queue one download job per missing version.

The workers do the downloading; this process only talks to arXiv for discovery (through the
same coordinator) and then waits for the queue to drain so it can write ``run_summary.json``
and ``failed.csv`` in the same shape as ``run_pipeline``. It takes the work-dir lock so two
producers never publish for the same work directory.
"""

from __future__ import annotations

import json
import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

from src import config
from src.crawler.coordinator import RemoteRateLimiter
from src.crawler.discovery import discover
from src.crawler.http_client import HttpClient
from src.crawler.queue import DownloadJob, RedisJobQueue
from src.crawler.worklock import WorkDirLock
from src.run_pipeline import (
    JsonlWriter,
    _now,
    _start_references,
    build_parser,
    is_done,
    load_version_state,
    select_ids,
    setup_logging,
    summarize,
    write_failed_csv,
)

log = logging.getLogger(__name__)


@dataclass
class Produced:
    found: dict[str, int]
    missing: dict[str, str]
    published: int
    discovery_s: float


def produce(
    ids: list[str],
    *,
    client: HttpClient,
    job_queue: RedisJobQueue,
    data_root: Path,
    work_dir: Path,
    snapshot: Path | None = None,
    refresh_metadata: bool = False,
) -> Produced:
    data_root.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)
    lock = WorkDirLock(work_dir)
    lock.acquire()  # raises WorkDirLockedError if another run uses this work_dir
    disc_log = JsonlWriter(work_dir / "discovery.jsonl")
    try:
        disc = discover(
            ids,
            client,
            data_root,
            snapshot=snapshot,
            refresh=refresh_metadata,
            journal=disc_log.write,
        )
        state = load_version_state(work_dir / "versions.jsonl")
        todo = [
            (arxiv_id, version)
            for arxiv_id in ids
            for version in range(1, disc.found.get(arxiv_id, 0) + 1)
            if not is_done(state.get((arxiv_id, version)), data_root)
        ]
        for arxiv_id, version in todo:
            job_queue.publish(DownloadJob.create(arxiv_id, version))
        log.info("%d download jobs published", len(todo))
        return Produced(disc.found, disc.missing, len(todo), disc.elapsed_s)
    finally:
        disc_log.close()
        lock.release()


def wait_until_drained(
    queue: RedisJobQueue, poll_s: float, sleep: Callable[[float], None] = time.sleep
) -> None:
    """Pending plus not-yet-delivered jobs reach 0 when all are acked or dead-lettered."""
    while queue.depth() > 0:
        sleep(poll_s)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    parser.add_argument("--coordinator-url", default="http://coordinator:8780")
    parser.add_argument("--download-stream", default="download-jobs")
    parser.add_argument("--download-group", default="crawler-workers")
    parser.add_argument("--poll-s", type=float, default=5.0)
    parser.add_argument("--no-wait", action="store_true", help="publish and exit")
    args = parser.parse_args(argv)
    if not args.redis_url:
        raise SystemExit("--redis-url is required")
    ids, label = select_ids(args)
    print(f"{len(ids)} IDs: {ids[0]} -> {ids[-1]}")
    if args.dry_run:
        return 0

    setup_logging(args.work_dir, logging.INFO)
    started_at, t0 = _now(), time.perf_counter()
    queue = RedisJobQueue(
        args.redis_url,
        args.download_stream,
        args.download_group,
        "producer",
        job_cls=DownloadJob,
    )
    limiter = RemoteRateLimiter(args.coordinator_url, worker="producer")
    client = HttpClient(limiter, config.USER_AGENT)
    out = produce(
        ids,
        client=client,
        job_queue=queue,
        data_root=args.data_root,
        work_dir=args.work_dir,
        snapshot=args.snapshot,
        refresh_metadata=args.refresh_metadata,
    )
    ref_thread = (
        None if args.no_references else _start_references(list(out.found), args.data_root)
    )
    if args.no_wait:
        return 0
    wait_until_drained(queue, args.poll_s)
    if ref_thread is not None:
        ref_thread.join()

    state = load_version_state(args.work_dir / "versions.jsonl")
    summary = {
        "label": label,
        "first_id": ids[0],
        "last_id": ids[-1],
        "started_at": started_at,
        "finished_at": _now(),
        "wall_s": round(time.perf_counter() - t0, 1),
        "discovery_s": round(out.discovery_s, 1),
        "jobs_published": out.published,
        "dead_letter": int(queue.client.xlen(queue.dead_letter_stream)),
        "coordinator": limiter.stats(),
        **summarize(ids, out.found, out.missing, state, args.data_root),
    }
    summary["failed_rows"] = write_failed_csv(
        args.work_dir / "failed.csv", ids, out.found, out.missing, state
    )
    (args.work_dir / "run_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

- [ ] **Step 4: Run tests and lint**

Run: `python -m pytest src/tests/test_producer.py -v ; ruff check src/crawler/producer.py src/tests/test_producer.py`
Expected: 3 PASS, ruff clean. If `WorkDirLockedError`'s message does not contain "already used", the third test's `match` already fits the existing text: `"... is already used by another run (...)"`.

- [ ] **Step 5: Commit**

```bash
git add src/crawler/producer.py src/tests/test_producer.py
git commit -m "feat: producer publishes download jobs and summarises the run"
```

---

### Task 5: Docker Compose, environment and README

**Files:**
- Modify: `compose.yaml`
- Modify: `.env.example`
- Modify: `README.md`

**Interfaces:**
- Consumes: CLIs from Tasks 1, 3, 4.
- Produces: services `coordinator`, `crawler-worker` (replicas `${CRAWLER_WORKERS:-3}`), `crawler-producer`; variables `CRAWLER_WORKERS`, `ARXIV_MIN_GAP_S`, `LEASE_TTL_S`, `JOB_VISIBILITY_TIMEOUT_S`, `MAX_ATTEMPTS`.

- [ ] **Step 1: Edit `compose.yaml`**

Add these services after `redis`:

```yaml
  coordinator:
    build: .
    command:
      - python
      - -m
      - src.crawler.coordinator
      - --host
      - 0.0.0.0
      - --port
      - "8780"
      - --min-interval
      - "${ARXIV_MIN_GAP_S:-3.0}"
      - --lease-ttl
      - "${LEASE_TTL_S:-60}"
    healthcheck:
      test:
        - CMD
        - python
        - -c
        - "import urllib.request as u; u.urlopen(u.Request('http://localhost:8780/stats', data=b'{}'), timeout=2)"
      interval: 5s
      timeout: 3s
      retries: 10
    restart: unless-stopped

  crawler-worker:
    build: .
    command:
      - python
      - -m
      - src.crawler.worker
      - --redis-url
      - redis://redis:6379/0
      - --coordinator-url
      - http://coordinator:8780
      - --data-root
      - /data/corpus
      - --work-dir
      - /data/work
      - --visibility-timeout-s
      - "${JOB_VISIBILITY_TIMEOUT_S:-300}"
      - --max-attempts
      - "${MAX_ATTEMPTS:-3}"
    environment:
      ARXIV_CONTACT_EMAIL: ${ARXIV_CONTACT_EMAIL:-}
    volumes:
      - ${CORPUS_HOST_PATH:-./data/corpus}:/data/corpus
      - ${WORK_HOST_PATH:-./work}:/data/work
    depends_on:
      redis:
        condition: service_healthy
      coordinator:
        condition: service_healthy
    restart: unless-stopped
    deploy:
      replicas: ${CRAWLER_WORKERS:-3}

  crawler-producer:
    build: .
    command:
      - python
      - -m
      - src.crawler.producer
      - --range
      - ${CRAWL_RANGE:-thien}
      - --offset
      - ${CRAWL_OFFSET:-0}
      - --limit
      - ${CRAWL_LIMIT:-100}
      - --redis-url
      - redis://redis:6379/0
      - --coordinator-url
      - http://coordinator:8780
      - --data-root
      - /data/corpus
      - --work-dir
      - /data/work
      - --no-references
    environment:
      ARXIV_CONTACT_EMAIL: ${ARXIV_CONTACT_EMAIL:-}
    volumes:
      - ${CORPUS_HOST_PATH:-./data/corpus}:/data/corpus
      - ${WORK_HOST_PATH:-./work}:/data/work
    depends_on:
      redis:
        condition: service_healthy
      coordinator:
        condition: service_healthy
    restart: "no"
```

In the existing `crawler` service add `profiles: ["single"]` (the old single-process path, now opt-in so it cannot run a second arXiv client beside the cluster). In `parser-worker`, delete the `crawler: condition: service_started` entry under `depends_on` (the parser needs only Redis; jobs arrive from `crawler-worker`).

- [ ] **Step 2: Edit `.env.example`**

Append:

```
# Crawler cluster
CRAWLER_WORKERS=3
ARXIV_MIN_GAP_S=3.0
LEASE_TTL_S=60
JOB_VISIBILITY_TIMEOUT_S=300
MAX_ATTEMPTS=3
```

- [ ] **Step 3: Validate the Compose file**

Run: `docker compose config -q`
Expected: no output, exit 0. Then `docker compose config | grep -A1 replicas` should show `replicas: 3` for `crawler-worker`; with `CRAWLER_WORKERS=5` in the environment it should show `5`.

- [ ] **Step 4: Document in `README.md`**

Add a section "Running the crawler cluster" with: the three services and what each does; `docker compose up -d redis coordinator crawler-worker parser-worker` then `docker compose run --rm crawler-producer`; `CRAWLER_WORKERS` sets the worker count (or `--scale crawler-worker=N`); the warning that more workers never raise the arXiv rate; the single-instance rule for `coordinator`; where results land (`work/run_summary.json`, `work/failed.csv`, dead-letter stream `download-jobs:dead`); and that rerunning a dead-lettered version needs `DEL download-jobs:job:<job_id>` first because jobs are deduplicated by id.

- [ ] **Step 5: Commit**

```bash
git add compose.yaml .env.example README.md
git commit -m "feat: compose services for coordinator, crawler workers and producer"
```

---

### Task 6: Live verification with 100 real IDs

This task sends real requests to arXiv (about 100–140, at least 3 s apart, 5–7 minutes). Get the user's go-ahead before running it.

**Files:** none (verification only).

- [ ] **Step 1: Restore the pace setting for any non-cluster use**

Check `src/config.py` has `ARXIV_MIN_INTERVAL_S = 3.0`; if it still says `0.5`, tell the user (the cluster ignores it, the single-process path does not).

- [ ] **Step 2: Start the cluster and run the producer**

```bash
docker compose build
docker compose up -d redis coordinator crawler-worker parser-worker
docker compose ps          # expect 3 crawler-worker containers, coordinator healthy
docker compose run --rm crawler-producer --limit 100
```

Expected: the producer prints the final summary JSON.

- [ ] **Step 3: Check the summary against the spec**

Read `work/run_summary.json` and confirm:
- `coordinator.min_gap_s` ≥ 3.0 (the achieved minimum gap between request starts)
- `papers_complete` = 100 (equal to `ids_requested`), `failed_rows` = 0, `dead_letter` = 0
- `wall_s` roughly 300–420; a much shorter time means the limiter is wrong
- `version_status` may include `no_tex` (PDF-only), which is valid.

- [ ] **Step 4: Kill a worker mid-run**

Repeat with a fresh range (`CRAWL_OFFSET=100`), and while workers are busy run `docker kill <one crawler-worker container>`. Expected: the lease expires within about 60 s, the other workers continue, the killed container restarts (`restart: unless-stopped`), its job returns after the visibility timeout (set `JOB_VISIBILITY_TIMEOUT_S=60` for this run), and `papers_complete` still reaches the full count.

- [ ] **Step 5: Report**

Tell the user the measured numbers (min gap, wall time, completed, dead-letter, worker-kill outcome) and any deviation. Do not claim success without these values.

---

## Self-Review (done while writing)

- **Spec coverage:** producer + queue (Tasks 2–4); `CRAWLER_WORKERS` replicas and env vars (Task 5); lease/TTL/renew/late release ignored (Task 1); global pace and one in flight, `429` pause shared (existing `/pause` route, `HttpClient._backoff`; unchanged); job reliability, retry, dead-letter (Tasks 2–3, existing `RedisJobQueue`); coordinator-unreachable behaviour (workers raise from `_post`, job stays pending; no local fallback); 100-ID test and kill test (Task 6).
- **Deviation from the spec:** the spec says the coordinator keeps its state in Redis to survive a restart. The plan instead makes the coordinator wait one full pace on startup (`limiter.pause_for(interval)`), which gives the same guarantee (no request closer than 3 s to the last one before a crash) without a new Redis dependency in the coordinator. The spec was updated to match.
- **Known limits, not in scope:** a hung but alive worker keeps renewing its lease; jobs dead-lettered are not re-published until their dedupe key is deleted; the Semantic Scholar references crawl runs only when the producer is started without `--no-references`.
- **Type consistency:** `RemoteRateLimiter.stats()`, `check_min_interval`, `DownloadJob.create`, `process_one` keyword names and `Produced` fields match across tasks.
