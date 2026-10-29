# Crawl and Parser Queue Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Keep arXiv crawling in one paced instance while publishing completed versions to a durable Redis queue consumed by up to ten parser workers, with backlog-based crawl scheduling.

**Architecture:** The crawler remains the sole arXiv client and publishes idempotent version jobs only after a version's files and metadata are durable. Redis Streams consumer groups distribute jobs; workers read the shared corpus mount, run M2 parsing, persist idempotent results, and acknowledge only after success. A bounded adaptive scheduler controls how many IDs are admitted based on queue depth and worker progress; Compose runs one crawler, Redis, and scalable parser workers.

**Tech Stack:** Python 3.12; Redis Streams via `redis` Python client; Docker Compose; pytest; shared filesystem volume.

**Spec:** `docs/superpowers/specs/2026-10-27-crawl-parser-queue-design.md`

## Global Constraints

- Exactly one crawler process makes arXiv requests; existing request limiter and minimum interval remain unchanged.
- Initial adaptive seed is 10 IDs, configurable to 20; the bounded live trial may override seed to 100 IDs.
- Parser workers scale to ten and share the corpus volume; do not put large data payloads in Redis.
- Redis Streams persistence is enabled; queue publication failure pauses crawl scheduling rather than dropping jobs.
- Job key is stable across retries for `(arxiv_id, version, parser operation/schema version)`.
- Persist parser output before ACK; bound retries and dead-letter poison jobs.
- Trial uses first 100 IDs of configured `thien` range and isolated data/work directories; never overwrite existing corpus/work.

## Review Focus

- Redis unavailable during publish: crawler must not advance/discard the unqueued completed-version job; covered in Task 2.
- Worker killed after claim but before ACK: pending work is reclaimed and output is not duplicated; covered in Task 3.
- Same job delivered more than once or parser schema changes: output is idempotent and schema-versioned; covered in Task 3.
- Queue backlog grows while workers stop acknowledging: adaptive scheduler pauses/reduces batches instead of unbounded crawling; covered in Task 4.
- Trial output path already exists or range boundary is wrong: trial refuses unsafe reuse and selects exactly the first 100 `thien` IDs; covered in Task 6.

---

### Task 1: Define parser job contract and Redis Streams queue

**Files:**
- Create: `src/crawler/queue.py`
- Create: `src/tests/test_queue.py`
- Modify: `src/requirements.txt`

**Interfaces:**
- Produces `ParserJob` (dataclass): `job_id: str`, `arxiv_id: str`, `version: int`, `corpus_relpath: str`, `metadata: dict`, `schema_version: int`.
- Produces `RedisJobQueue(redis_url: str, stream: str, group: str, consumer: str)` with `publish(job: ParserJob) -> str`, `claim(*, block_ms: int = 5000) -> ParserJob | None`, `ack(job_id: str) -> None`, `retry_or_dead_letter(job_id: str, error: str, max_attempts: int) -> None`, `depth() -> int`, and `reclaim_idle(min_idle_ms: int, count: int = 10) -> int`.

- [ ] **Step 1: Write failing tests** in `src/tests/test_queue.py` for stable job IDs; duplicate publish dedupe; consumer-group claim; depth; ACK removal from pending; bounded retry to dead-letter; idle pending reclaim. Tests use an injected Redis client or local Redis fixture, not a network service.
- [ ] **Step 2: Run targeted tests**: `python -m pytest src/tests/test_queue.py -q`; expected initial failure because queue module is absent.
- [ ] **Step 3: Add redis client dependency** to `src/requirements.txt` using a compatible pinned/minimum version consistent with project dependency conventions.
- [ ] **Step 4: Implement job serialization and `RedisJobQueue`** in `src/crawler/queue.py` with Redis Streams, consumer groups, stable dedupe key, pending recovery, and a dead-letter stream.
- [ ] **Step 5: Run targeted tests**: `python -m pytest src/tests/test_queue.py -q`; expected PASS.

### Task 2: Publish completed crawl versions safely

**Files:**
- Modify: `src/run_pipeline.py:107-179, 273-407, 463-530`
- Create: `src/tests/test_run_pipeline_queue.py`

**Interfaces:**
- Consumes `ParserJob` and `RedisJobQueue` from Task 1.
- Adds optional `job_queue: RedisJobQueue | None` to `run(...)`; absent queue preserves current behavior.
- Adds CLI options `--redis-url`, `--queue-stream`, `--queue-group`, and queue-specific reporting.

- [ ] **Step 1: Write failing tests** that verify successful extracted version publishes one job after output is present; duplicate/re-run does not publish an extra job; versions with no TeX are not sent to M2 parser; queue publish failure exits/pause behavior without silently recording work as handed off; no queue configured retains current behavior.
- [ ] **Step 2: Run targeted tests**: `python -m pytest src/tests/test_run_pipeline_queue.py -q`; expected failure on missing queue integration.
- [ ] **Step 3: Implement queue injection and publication** after durable version record/output completion. Ensure publication failure prevents admitting additional IDs and is reported distinctly for safe resume.
- [ ] **Step 4: Add CLI configuration** to construct queue only when explicitly configured; keep existing single-crawler `WorkDirLock` and `HttpClient(get_arxiv_limiter(), ...)` path intact.
- [ ] **Step 5: Run targeted and existing pipeline tests**: `python -m pytest src/tests/test_run_pipeline_queue.py src/tests/test_run_pipeline.py -q`; expected PASS.

### Task 3: Implement idempotent parser worker and recovery

**Files:**
- Create: `src/parser/worker.py`
- Create: `src/tests/test_parser_worker.py`
- Modify: `src/parser/gather.py` only if a version-scoped callable entry point is needed.

**Interfaces:**
- Consumes `RedisJobQueue` from Task 1.
- Produces `parse_job(job: ParserJob, corpus_root: Path, output_root: Path) -> dict`.
- Produces CLI `python -m src.parser.worker --redis-url ... --corpus-root ... --output-root ... --consumer ...`.

- [ ] **Step 1: Write failing tests** for valid version parsing; stable idempotent output on duplicate delivery; missing corpus directory; no-root result; successful output persisted before ACK; worker exception leaves/retries pending job; poison job moves to dead-letter after configured attempts; reclaimed job resumes.
- [ ] **Step 2: Run targeted tests**: `python -m pytest src/tests/test_parser_worker.py -q`; expected failure because worker module is absent.
- [ ] **Step 3: Implement `parse_job`** using the existing M2 gather/expand logic, writing atomic result files keyed by job/schema version and recording outcome.
- [ ] **Step 4: Implement consumer loop**: claim, parse, persist, ACK; on failures retry with bounded attempts and reclaim idle pending jobs. Do not ACK on exceptions or incomplete output.
- [ ] **Step 5: Run targeted parser and queue tests**: `python -m pytest src/tests/test_parser_worker.py src/tests/test_queue.py -q`; expected PASS.

### Task 4: Add bounded backlog-adaptive crawler scheduling

**Files:**
- Create: `src/crawler/scheduler.py`
- Create: `src/tests/test_scheduler.py`
- Modify: `src/run_pipeline.py` to use scheduler when queue mode is enabled.

**Interfaces:**
- Produces `AdaptiveBatchScheduler(seed: int = 10, max_batch: int = 100, step: int = 10, low_watermark: int = 20, high_watermark: int = 100, max_prefetch: int = 200)` with `next_batch(queue_depth: int, worker_progress: bool) -> int`.

- [ ] **Step 1: Write failing tests** for seed batch 10 (and configurable 20), gradual bounded growth under low backlog/progress, hold/reduce at high backlog, pause when no worker progress, and caps at max batch/max prefetch.
- [ ] **Step 2: Run scheduler tests**: `python -m pytest src/tests/test_scheduler.py -q`; expected failure because scheduler is absent.
- [ ] **Step 3: Implement deterministic scheduler policy** with configurable values and no time-based assumption; queue mode feeds queue depth and recent ACK progress.
- [ ] **Step 4: Integrate chunk admission** so only a bounded ID batch is crawled before rechecking backlog/progress; default non-queue CLI path remains unchanged.
- [ ] **Step 5: Run scheduler and pipeline regression tests**: `python -m pytest src/tests/test_scheduler.py src/tests/test_run_pipeline.py src/tests/test_run_pipeline_queue.py -q`; expected PASS.

### Task 5: Add Docker Compose deployment and operational docs

**Files:**
- Create: `compose.yaml`
- Create: `.env.example`
- Modify: `README.md`
- Modify: `src/requirements.txt` if worker runtime requirements are distinct (prefer shared base image/dependencies).

**Interfaces:**
- Compose services: `redis`, `crawler`, `parser-worker`.
- Required env: `REDIS_URL`, queue stream/group, `DATA_ROOT`, `WORK_DIR`, `PARSER_OUTPUT_ROOT`; configurable scheduler thresholds.

- [ ] **Step 1: Write deployment checks/documented commands** ensuring compose config validates, crawler service is one instance, and worker service supports `--scale parser-worker=10` without fixed container name.
- [ ] **Step 2: Create Compose services** with Redis persistence volume and healthcheck, shared corpus/output mounts, crawler dependency on healthy Redis, and worker dependency on Redis/corpus availability.
- [ ] **Step 3: Add `.env.example` and README run/stop/resume commands**, explicitly document that only parser workers are scaled and arXiv requests remain serialized.
- [ ] **Step 4: Verify Compose config**: `docker compose config`; expected valid resolved service graph, no `container_name` on parser workers.
- [ ] **Step 5: Run local integration** with Redis and two parser workers on a small fixture corpus; expected all jobs completed once, pending count zero, output files present.

### Task 6: Verify adaptive queue behavior and conduct isolated 100-ID trial

**Files:**
- Create: `src/tests/test_queue_integration.py`
- No production code changes unless earlier task tests reveal defects.

- [ ] **Step 1: Add integration test** using local Redis and temporary corpus to run crawler publication with mocked arXiv responses and multiple workers; verify adaptive batch progression, backpressure, and no job loss/duplicate parser output.
- [ ] **Step 2: Run full test suite**: `python -m pytest -q`; expected PASS.
- [ ] **Step 3: Validate trial paths and selected IDs without network**: `python -m src.run_pipeline --range thien --limit 100 --dry-run --data-root <new-trial-path> --work-dir <new-trial-work>`; expected exactly 100 IDs from the start of `thien` range.
- [ ] **Step 4: Confirm trial directories do not exist or are empty**; if either path has existing data, select a new unique trial path rather than overwrite.
- [ ] **Step 5: Run the live trial** with one crawler, 100 `thien` IDs, the requested parser worker scale, isolated paths, and the existing arXiv limiter; capture crawler summary, queue counts, worker throughput, wall time, retries, and minimum request gap.
- [ ] **Step 6: Verify trial outcome**: `min_request_gap_s >= 3.0`, no more than one crawler process, queue has no unacknowledged jobs after completion (or report outstanding failures), all outputs pass existing validation, and report measured times without extrapolating concurrency to arXiv.
