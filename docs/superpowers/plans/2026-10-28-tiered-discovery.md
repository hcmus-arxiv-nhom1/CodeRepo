# Tiered Discovery Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the producer discover metadata in small batches first (5–10 IDs) and grow to 20–100 IDs only when enough jobs are already queued, stepping down after an arXiv `429`.

**Architecture:** A pure `DiscoveryLadder` maps "queued work" (download-jobs backlog + parser-jobs backlog) to a batch size and steps down for a few rounds after a rate-limited chunk. `produce()` loops over chunks of the ID list instead of discovering everything first, publishing download jobs after each chunk. `discover()` only gains two counters.

**Tech Stack:** Python 3.12, Redis Streams (existing `RedisJobQueue.depth()`), pytest, Docker Compose.

**Spec:** `docs/superpowers/specs/2026-10-28-tiered-discovery-design.md`

## Global Constraints

- All arXiv requests still go through the coordinator; this plan adds no request path and no local limiter.
- Defaults: `DISCOVERY_TIERS=5,10,20,50,100`, `DISCOVERY_BACKLOG_STEPS=10,30,60,100`, `DISCOVERY_COOLDOWN_ROUNDS=3`. The last tier must not exceed `config.ARXIV_API_BATCH` (100).
- Backlog = `download-jobs` pending+undelivered plus `parser-jobs` pending+undelivered (`RedisJobQueue.depth()` of each).
- IDs that already have `metadata.json` cost no request: they do not count toward the batch size and are published immediately.
- `discover()` behaviour is unchanged apart from the two new counters on `DiscoveryResult`.
- No back-pressure on the producer (YAGNI); worker, coordinator, job format and `run_pipeline` are untouched.
- Python 3.12; new source files start with `# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.`; `ruff check` passes. Run tools with `D:\introds\.venv-crawler\Scripts\python -m ...` from `D:\introds\CodeRepo`.

## Notes before starting

- The files this plan edits (`producer.py`, `test_producer.py`, `compose.yaml`, ...) are untracked or modified from earlier uncommitted work. Commit steps add only the files each task touches, which includes that earlier work. Confirm with the user before the first commit.
- Redis keeps `download-jobs:job:<job_id>` dedupe keys in the `redis-data` volume across `docker compose down`. A live rerun of IDs that were already published is silently deduplicated (no jobs, no downloads). Task 4 therefore uses an ID range not crawled before (`CRAWL_OFFSET=200`).

## File Structure

- Create `src/crawler/ladder.py` — `DiscoveryLadder` (pure, env-configurable).
- Create `src/tests/test_ladder.py`.
- Modify `src/crawler/discovery.py` — `api_failed_batches`, `oai_fallbacks` on `DiscoveryResult`.
- Modify `src/tests/test_discovery.py` — counter tests.
- Modify `src/crawler/producer.py` — chunked loop, new summary fields.
- Modify `src/tests/test_producer.py` — ladder-driven loop tests.
- Modify `compose.yaml`, `.env.example`, `README.md` — the three variables and a short note.

---

### Task 1: `DiscoveryLadder`

**Files:**
- Create: `src/crawler/ladder.py`
- Test: `src/tests/test_ladder.py`

**Interfaces:**
- Produces: `DiscoveryLadder(tiers=(5,10,20,50,100), backlog_steps=(10,30,60,100), cooldown_rounds=3)`; `.next_size(backlog: int) -> int`; `.on_rate_limited() -> None`; `DiscoveryLadder.from_env(environ: Mapping[str, str] | None = None) -> DiscoveryLadder`. Invalid configuration raises `ValueError`.

- [ ] **Step 1: Write the failing tests**

`src/tests/test_ladder.py`:

```python
# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
from __future__ import annotations

import pytest

from src.crawler.ladder import DiscoveryLadder


@pytest.mark.parametrize(
    ("backlog", "size"),
    [(0, 5), (9, 5), (10, 10), (29, 10), (30, 20), (59, 20), (60, 50), (99, 50), (100, 100), (5000, 100)],
)
def test_backlog_picks_the_tier(backlog, size):
    assert DiscoveryLadder().next_size(backlog) == size


def test_rate_limit_caps_the_tier_for_the_cooldown_then_lets_it_grow_again():
    ladder = DiscoveryLadder()
    assert ladder.next_size(1000) == 100
    ladder.on_rate_limited()
    assert [ladder.next_size(1000) for _ in range(3)] == [50, 50, 50]
    assert ladder.next_size(1000) == 100


def test_consecutive_rate_limits_step_down_further():
    ladder = DiscoveryLadder()
    assert ladder.next_size(1000) == 100
    ladder.on_rate_limited()
    assert ladder.next_size(1000) == 50
    ladder.on_rate_limited()
    assert ladder.next_size(1000) == 20


def test_rate_limit_at_the_first_tier_stays_there():
    ladder = DiscoveryLadder()
    assert ladder.next_size(0) == 5
    ladder.on_rate_limited()
    assert ladder.next_size(1000) == 5


def test_negative_backlog_is_rejected():
    with pytest.raises(ValueError):
        DiscoveryLadder().next_size(-1)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"tiers": (10, 5), "backlog_steps": (10,)},  # not increasing
        {"tiers": (0, 5), "backlog_steps": (10,)},  # not positive
        {"tiers": (5, 10, 20), "backlog_steps": (10,)},  # one step missing
        {"tiers": (5, 10, 20), "backlog_steps": (30, 10)},  # steps not increasing
        {"cooldown_rounds": -1},
    ],
)
def test_invalid_configuration_is_rejected(kwargs):
    with pytest.raises(ValueError):
        DiscoveryLadder(**kwargs)


def test_from_env_reads_the_three_variables():
    ladder = DiscoveryLadder.from_env(
        {
            "DISCOVERY_TIERS": "2, 4,8",
            "DISCOVERY_BACKLOG_STEPS": "5,9",
            "DISCOVERY_COOLDOWN_ROUNDS": "1",
        }
    )
    assert ladder.tiers == (2, 4, 8)
    assert ladder.next_size(9) == 8
    ladder.on_rate_limited()
    assert ladder.next_size(9) == 4
    assert ladder.next_size(9) == 8  # cooldown of one round is over


def test_from_env_defaults_when_unset():
    assert DiscoveryLadder.from_env({}).tiers == (5, 10, 20, 50, 100)
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest src/tests/test_ladder.py -v`
Expected: `ModuleNotFoundError: No module named 'src.crawler.ladder'`.

- [ ] **Step 3: Implement `src/crawler/ladder.py`**

```python
# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""Batch size for metadata discovery, chosen from how much work is already queued.

Early on nothing is queued, so discovery asks the API for only a few IDs: the first download
jobs appear within seconds and a small request is unlikely to be rejected. Once the queues
hold enough jobs to keep the workers busy during one more request, batches grow so fewer
requests are spent on metadata. After an arXiv ``429`` the ladder steps down one tier and
holds it for a few rounds before it may grow again.
"""

from __future__ import annotations

import os
from bisect import bisect_right
from collections.abc import Mapping
from dataclasses import dataclass, field

DEFAULT_TIERS = (5, 10, 20, 50, 100)
DEFAULT_BACKLOG_STEPS = (10, 30, 60, 100)
DEFAULT_COOLDOWN_ROUNDS = 3


def _ints(raw: str) -> tuple[int, ...]:
    return tuple(int(part) for part in raw.split(",") if part.strip())


@dataclass
class DiscoveryLadder:
    tiers: tuple[int, ...] = DEFAULT_TIERS
    backlog_steps: tuple[int, ...] = DEFAULT_BACKLOG_STEPS
    cooldown_rounds: int = DEFAULT_COOLDOWN_ROUNDS
    _cap: int = field(default=0, init=False, repr=False)  # highest tier index while cooling down
    _cooldown: int = field(default=0, init=False, repr=False)
    _last: int = field(default=0, init=False, repr=False)  # tier index used by the last round

    def __post_init__(self) -> None:
        tiers, steps = self.tiers, self.backlog_steps
        if not tiers or tiers[0] <= 0 or any(b <= a for a, b in zip(tiers, tiers[1:], strict=False)):
            raise ValueError("tiers must be positive and strictly increasing")
        if len(steps) != len(tiers) - 1:
            raise ValueError("need exactly one backlog step for every tier after the first")
        if (steps and steps[0] <= 0) or any(b <= a for a, b in zip(steps, steps[1:], strict=False)):
            raise ValueError("backlog steps must be positive and strictly increasing")
        if self.cooldown_rounds < 0:
            raise ValueError("cooldown_rounds cannot be negative")

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> DiscoveryLadder:
        env = os.environ if environ is None else environ
        return cls(
            tiers=_ints(env["DISCOVERY_TIERS"]) if env.get("DISCOVERY_TIERS") else DEFAULT_TIERS,
            backlog_steps=_ints(env["DISCOVERY_BACKLOG_STEPS"])
            if env.get("DISCOVERY_BACKLOG_STEPS")
            else DEFAULT_BACKLOG_STEPS,
            cooldown_rounds=int(env.get("DISCOVERY_COOLDOWN_ROUNDS") or DEFAULT_COOLDOWN_ROUNDS),
        )

    def next_size(self, backlog: int) -> int:
        if backlog < 0:
            raise ValueError("backlog cannot be negative")
        index = bisect_right(self.backlog_steps, backlog)
        if self._cooldown > 0:
            index = min(index, self._cap)
            self._cooldown -= 1
        self._last = index
        return self.tiers[index]

    def on_rate_limited(self) -> None:
        """The chunk just discovered was rejected: use a lower tier for the next few rounds."""
        self._cap = max(self._last - 1, 0)
        self._cooldown = self.cooldown_rounds
```

- [ ] **Step 4: Run tests and lint**

Run: `python -m pytest src/tests/test_ladder.py -v ; python -m ruff check src/crawler/ladder.py src/tests/test_ladder.py`
Expected: all PASS, ruff clean (wrap long lines if ruff reports E501).

- [ ] **Step 5: Commit**

```bash
git add src/crawler/ladder.py src/tests/test_ladder.py
git commit -m "feat: DiscoveryLadder chooses the discovery batch size from queued work"
```

---

### Task 2: Failure and fallback counters in `discover()`

**Files:**
- Modify: `src/crawler/discovery.py` (`DiscoveryResult`, the two API `except` blocks, the OAI loop)
- Test: `src/tests/test_discovery.py`

**Interfaces:**
- Produces: `DiscoveryResult.api_failed_batches: int = 0` and `DiscoveryResult.oai_fallbacks: int = 0`.

- [ ] **Step 1: Write the failing tests**

In `src/tests/test_discovery.py`, add to the end of `test_discover_resolves_versions_and_writes_metadata`:

```python
    assert res.oai_fallbacks == 1  # 2606.00003 was not returned by the API
    assert res.api_failed_batches == 0
```

and append a new test:

```python
def test_discover_counts_a_failed_api_batch(tmp_path):
    calls = []
    good = arxiv_handler(calls)

    def handler(url):
        if not calls:  # first batch fails: feed cut mid-body
            calls.append(url)
            return FakeResponse(200, b'<?xml version="1.0"?><feed><entry><id>http://arx')
        return good(url)

    res = discover(["2606.00001"], make_client(handler), tmp_path)
    assert res.api_failed_batches == 1
    assert res.oai_fallbacks == 0
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest src/tests/test_discovery.py -v`
Expected: `AttributeError: 'DiscoveryResult' object has no attribute 'oai_fallbacks'`.

- [ ] **Step 3: Implement**

In `src/crawler/discovery.py`:

1. Add two fields to `DiscoveryResult` after `elapsed_s`:

```python
    api_failed_batches: int = 0  # API batches that raised (429 after retries, cut feed, ...)
    oai_fallbacks: int = 0  # OAI-PMH GetRecord lookups made for IDs the API did not resolve
```

2. In both `except (FetchError, etree.XMLSyntaxError) as exc:` blocks (the base batch loop and the versioned batch loop) add `result.api_failed_batches += 1` as the first line of the block, before the `log.error(...)` call.

3. In the OAI loop, add `result.oai_fallbacks += 1` as the first line of the loop body, before `try:`:

```python
    for arxiv_id in [i for i in todo if i not in entries] + incomplete:
        result.oai_fallbacks += 1
        try:
```

- [ ] **Step 4: Run the discovery and wider tests**

Run: `python -m pytest src/tests/test_discovery.py src/tests/test_run_pipeline.py -v`
Expected: all PASS.

- [ ] **Step 5: Commit**

```bash
git add src/crawler/discovery.py src/tests/test_discovery.py
git commit -m "feat: count failed API batches and OAI fallbacks in discovery"
```

---

### Task 3: Chunked producer loop, summary fields and configuration

**Files:**
- Modify: `src/crawler/producer.py`
- Modify: `src/tests/test_producer.py`
- Modify: `compose.yaml`, `.env.example`, `README.md`

**Interfaces:**
- Consumes: `DiscoveryLadder` (Task 1); `DiscoveryResult.api_failed_batches`, `.oai_fallbacks` (Task 2); `read_metadata` from `src.crawler.discovery`; `RedisJobQueue.depth()`.
- Produces: `produce(ids, *, client, job_queue, data_root, work_dir, backlog=<callable returning 0>, ladder=None, snapshot=None, refresh_metadata=False) -> Produced`; `Produced` gains `tier_history: list[int]`, `api_failed_batches: int`, `oai_fallbacks: int`, `discovery_requests: int`, `time_to_first_job_s: float | None`.

- [ ] **Step 1: Write the failing tests**

In `src/tests/test_producer.py` add `from src.crawler.ladder import DiscoveryLadder` to the imports and append:

```python
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
```

- [ ] **Step 2: Run to verify failure**

Run: `python -m pytest src/tests/test_producer.py -v`
Expected: the three new tests FAIL with `TypeError: produce() got an unexpected keyword argument 'backlog'`; the three existing tests still pass.

- [ ] **Step 3: Implement in `src/crawler/producer.py`**

Replace the imports from `collections.abc` and `dataclasses`, `Produced`, and `produce` with:

```python
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
```

(add `from src.crawler.ladder import DiscoveryLadder` next to the other `src.crawler` imports and change the discovery import to `from src.crawler.discovery import discover, read_metadata`.)

```python
@dataclass
class Produced:
    found: dict[str, int]
    missing: dict[str, str]
    published: int
    discovery_s: float
    tier_history: list[int] = field(default_factory=list)
    api_failed_batches: int = 0
    oai_fallbacks: int = 0
    discovery_requests: int = 0  # every arXiv request made while discovering
    time_to_first_job_s: float | None = None


def _requests(client: HttpClient | None) -> int:
    return client.requests_sent if client is not None else 0


def _take_chunk(remaining: deque[str], size: int, data_root: Path, refresh: bool) -> list[str]:
    """Next IDs to discover: cached IDs are free, up to ``size`` uncached ones are charged."""
    chunk, fresh = [], 0
    while remaining and fresh < size:
        arxiv_id = remaining.popleft()
        if refresh or read_metadata(data_root, arxiv_id) is None:
            fresh += 1
        chunk.append(arxiv_id)
    return chunk


def produce(
    ids: list[str],
    *,
    client: HttpClient | None,
    job_queue: RedisJobQueue,
    data_root: Path,
    work_dir: Path,
    backlog: Callable[[], int] = lambda: 0,
    ladder: DiscoveryLadder | None = None,
    snapshot: Path | None = None,
    refresh_metadata: bool = False,
) -> Produced:
    ladder = ladder or DiscoveryLadder.from_env()
    data_root.mkdir(parents=True, exist_ok=True)
    work_dir.mkdir(parents=True, exist_ok=True)
    lock = WorkDirLock(work_dir)
    lock.acquire()  # raises WorkDirLockedError if another run uses this work_dir
    disc_log = JsonlWriter(work_dir / "discovery.jsonl")
    out = Produced({}, {}, 0, 0.0)
    remaining = deque(ids)
    started = time.perf_counter()
    try:
        while remaining:
            size = ladder.next_size(backlog())
            chunk = _take_chunk(remaining, size, data_root, refresh_metadata)
            out.tier_history.append(size)
            requests_before = _requests(client)
            disc = discover(
                chunk,
                client,
                data_root,
                snapshot=snapshot,
                refresh=refresh_metadata,
                journal=disc_log.write,
            )
            out.discovery_requests += _requests(client) - requests_before
            out.found.update(disc.found)
            out.missing.update(disc.missing)
            out.discovery_s += disc.elapsed_s
            out.api_failed_batches += disc.api_failed_batches
            out.oai_fallbacks += disc.oai_fallbacks
            state = load_version_state(work_dir / "versions.jsonl")
            for arxiv_id in chunk:
                for version in range(1, disc.found.get(arxiv_id, 0) + 1):
                    if is_done(state.get((arxiv_id, version)), data_root):
                        continue
                    job_queue.publish(DownloadJob.create(arxiv_id, version))
                    out.published += 1
                    if out.time_to_first_job_s is None:
                        out.time_to_first_job_s = round(time.perf_counter() - started, 3)
            if disc.api_failed_batches:
                ladder.on_rate_limited()
            log.info("chunk of %d IDs (tier %d): %d jobs so far", len(chunk), size, out.published)
        return out
    finally:
        disc_log.close()
        lock.release()
```

In `main()`: build the parser-queue handle and the backlog callable, pass them to `produce`, and extend the summary. Replace the `out = produce(...)` call and the `summary = {...}` dict with:

```python
    parser_queue = RedisJobQueue(args.redis_url, args.queue_stream, args.queue_group, "producer")
    out = produce(
        ids,
        client=client,
        job_queue=queue,
        data_root=args.data_root,
        work_dir=args.work_dir,
        backlog=lambda: queue.depth() + parser_queue.depth(),
        snapshot=args.snapshot,
        refresh_metadata=args.refresh_metadata,
    )
```

and in the summary dict, after `"discovery_s"`, add:

```python
        "time_to_first_job_s": out.time_to_first_job_s,
        "discovery_api_requests": out.discovery_requests - out.oai_fallbacks,
        "discovery_oai_fallbacks": out.oai_fallbacks,
        "discovery_api_failed_batches": out.api_failed_batches,
        "discovery_tier_history": out.tier_history,
```

- [ ] **Step 4: Add configuration and docs**

`compose.yaml`, service `crawler-producer`, under `environment:` add:

```yaml
      DISCOVERY_TIERS: ${DISCOVERY_TIERS:-5,10,20,50,100}
      DISCOVERY_BACKLOG_STEPS: ${DISCOVERY_BACKLOG_STEPS:-10,30,60,100}
      DISCOVERY_COOLDOWN_ROUNDS: ${DISCOVERY_COOLDOWN_ROUNDS:-3}
```

`.env.example`, append:

```
# Tiered discovery (producer)
DISCOVERY_TIERS=5,10,20,50,100
DISCOVERY_BACKLOG_STEPS=10,30,60,100
DISCOVERY_COOLDOWN_ROUNDS=3
```

`README.md`, in the "crawler workers with one global rate limiter" section add one paragraph: the producer discovers metadata in chunks whose size follows the queued work (download-jobs plus parser-jobs): 5 IDs while the queues are empty, up to 100 once they hold 100 jobs; a rejected chunk lowers the size for `DISCOVERY_COOLDOWN_ROUNDS` rounds; `run_summary.json` reports `time_to_first_job_s`, `discovery_api_requests`, `discovery_oai_fallbacks`, `discovery_tier_history`.

- [ ] **Step 5: Run everything**

Run:
```
python -m pytest src/tests -q
python -m ruff check src/crawler src/tests
docker compose config -q
```
Expected: all tests PASS (the three original producer tests plus the new ones), ruff clean, Compose config valid.

- [ ] **Step 6: Commit**

```bash
git add src/crawler/producer.py src/tests/test_producer.py compose.yaml .env.example README.md
git commit -m "feat: producer discovers in ladder-sized chunks and publishes per chunk"
```

---

### Task 4: Live comparison on 100 new IDs

This sends real requests to arXiv (a few hundred seconds of traffic at 3 s spacing). Get the user's go-ahead before running it. Previous result to beat: discovery 541 s, 200 OAI requests, total 941 s (`D:\introds\run_summary_100.json`).

**Files:** none (verification only).

- [ ] **Step 1: Start the cluster with the new producer image**

```bash
docker compose build
docker compose up -d redis coordinator crawler-worker parser-worker
docker compose ps    # coordinator healthy, 3 crawler-worker containers
```

- [ ] **Step 2: Run the producer on IDs not published before**

The Redis volume still holds dedupe keys for offsets 0–119 (and the 20 IDs at offset 100), so a rerun of those would produce no jobs. Use offset 200:

```bash
CRAWL_OFFSET=200 CRAWL_LIMIT=100 docker compose run --rm crawler-producer
```

Do not append `--limit` to the command: `docker compose run` replaces the whole command.

- [ ] **Step 3: Compare with the previous run**

Read `work/run_summary.json` and report side by side with `D:\introds\run_summary_100.json`:
- `time_to_first_job_s` (expected: seconds, not minutes)
- `discovery_oai_fallbacks` (expected: near 0 unless arXiv rejects again)
- `discovery_tier_history` (expected: starts `5`, grows when the queue fills; steps down after any `429`)
- `discovery_s` and `wall_s` against 541 s and 941 s
- `coordinator.min_gap_s` ≥ 3.0, `papers_complete` = 100, `failed_rows` = 0, `dead_letter` = 0.

If arXiv again answers `429` to every API call, say so: the ladder then only limits the damage, as the spec's known-risks section states. Do not claim an improvement the numbers do not show.

- [ ] **Step 4: Clean up**

`docker compose down` (keeps volumes) and report the numbers.

---

## Self-Review (done while writing)

- **Spec coverage:** backlog signal as sum of both queues (Task 3 `main`); tiers/steps/cooldown env vars with defaults (Tasks 1, 3); step-down on `429` via `api_failed_batches` (Tasks 2, 3); cached IDs free (Task 3 `_take_chunk`); publish per chunk (Task 3); new summary fields `time_to_first_job_s`, `discovery_api_requests`, `discovery_oai_fallbacks`, `discovery_tier_history` (Task 3); counters on `DiscoveryResult` including `oai_fallbacks` (Task 2); invalid config rejected (Task 1); live comparison (Task 4). The spec's note that `wait_until_drained` and the summary stay as they are is respected: only fields are added.
- **Placeholders:** none; every code step has code.
- **Type consistency:** `DiscoveryLadder.next_size/on_rate_limited/from_env`, `Produced` fields, `DiscoveryResult.api_failed_batches/oai_fallbacks`, and the `produce(...)` keywords (`backlog`, `ladder`) match across tasks and tests. The existing producer tests call `produce` without `backlog`/`ladder`, which default to a zero backlog and `DiscoveryLadder.from_env()`.
- **Known limit:** the spec's mention of `discovery_api_requests` includes versioned-ID API batches in the count, because it is "all requests made by discovery minus OAI lookups".
