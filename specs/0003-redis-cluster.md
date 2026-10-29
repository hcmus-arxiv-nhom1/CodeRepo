<!-- Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules. -->
# Spec 0003 — Redis queue, crawler workers and one global arXiv limiter

- **Author:** Hiếu (Member 3) · **Reviewers:** Thiên, Đăng · **Status:** implemented (PR #18), optional setup
- **Date:** 27–28/10/2026 · merges the three design notes formerly in `docs/superpowers/`

## Context

The official crawl and the M1 benchmark run on **one** Colab instance (`colab_crawl.ipynb`). This spec covers an optional local Docker Compose setup that lets M2 parsing and the I/O of several workers run next to the arXiv lane. It never raises the arXiv rate: the Terms of Use apply to *"all of the machines under your control as a whole"* (one request in flight, starts ≥ 3 s apart), so adding workers only keeps the single lane busy.

## Changes

| # | Change | Reason | Verification |
| --- | --- | --- | --- |
| 1 | **Parser queue** on Redis Streams (`src/crawler/queue.py`, `src/parser/worker.py`): each `ok` version publishes one job (stable key = paper, version, parser schema); jobs are acknowledged only after the output is persisted, pending jobs of a dead worker are reclaimed after a visibility timeout, and a job failing `MAX_ATTEMPTS` times moves to `<stream>:dead` | Up to ten parser workers can process finished versions while the crawl goes on; a crash loses no job and never loops forever | `test_queue.py`, `test_parser_worker.py`, `test_queue_integration.py` |
| 2 | **Adaptive batches** (`src/crawler/scheduler.py`): the crawler starts with `--queue-seed` IDs and grows or pauses with the queue backlog (low/high watermarks, max prefetch); no acknowledgement means no growth | The crawl never outruns the parsers, and a stalled worker pool stops new work instead of filling Redis | `test_scheduler.py`, `test_run_pipeline_queue.py` |
| 3 | **Coordinator** (`src/crawler/coordinator.py`): one HTTP service holds the only `RateLimiter` of the cluster; `RemoteRateLimiter` is a drop-in for it in `HttpClient`. The lock is a lease renewed by a heartbeat (`LEASE_TTL_S`, default 60 s), so a killed worker loses its turn; a late release of an expired lease is ignored. On start it waits one full pace; it refuses a pace below 3 s, and `/interval` (circuit breaker) can only slow the cluster down | One connection and ≥ 3 s across every worker; 429 backoff and the breaker apply to the whole cluster | `test_coordinator.py` (2 and 4 workers: never two requests in flight, smallest gap ≥ pace) |
| 4 | **Producer and crawler workers** (`src/crawler/producer.py`, `src/crawler/worker.py`): the producer discovers metadata and publishes one download job per missing version; `crawler-worker` replicas (`CRAWLER_WORKERS`, default 3) download through the coordinator and append to `versions.jsonl` with one `O_APPEND` write per record | Workers can die and restart without losing jobs or interleaving log lines | `test_producer.py`, `test_crawler_worker.py` |
| 5 | **Tiered discovery** (`src/crawler/ladder.py`): the producer discovers in chunks of 5 → 10 → 20 → 50 → 100 IDs as the backlog grows (`DISCOVERY_TIERS`, `DISCOVERY_BACKLOG_STEPS`); a chunk rejected with 429 lowers the size for `DISCOVERY_COOLDOWN_ROUNDS` rounds. Cached `metadata.json` costs no request | On the first 100-ID trial (28/10/2026, 3 workers) discovery took 541 s of 941 s: one 100-ID API call got five 429s and fell back to 200 OAI requests while the workers sat idle. Small first chunks give a first job within seconds and keep the OAI fallback cheap | `test_ladder.py`, `test_producer.py`; `run_summary.json` reports `time_to_first_job_s`, `discovery_oai_fallbacks`, `discovery_tier_history` |

## Limits

- The cluster runs `--no-references`: Semantic Scholar is only crawled by `run_pipeline`, i.e. on Colab.
- Benchmark numbers for the book come from the Colab runs, not from Compose.
- Never run `python -m src.run_pipeline` against arXiv while the cluster is up: it has its own local limiter. The former `single` Compose profile did exactly that and was removed (29/10/2026).

Rejected: **several machines or processes each with their own limiter** — it breaks the one-connection rule as soon as two of them run at once.
