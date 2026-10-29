# Crawl and parser queue design

- **Status:** Draft for review
- **Date:** 2026-10-27

## Goal

Keep arXiv access on one crawler instance while allowing up to ten parser workers to process completed crawl outputs concurrently. The crawler should start conservatively with a small batch and adapt the amount of work it schedules based on parser queue backlog. A 100-ID trial of the first 100 IDs in the `thien` range will be run only after this design is approved and the implementation is ready, writing to isolated trial directories.

## Scope

This change introduces a queue-backed handoff between crawl output and parser work, a worker consumer, and Docker Compose orchestration for a single crawler plus Redis and scalable parser workers. It does not increase arXiv request concurrency or change the existing arXiv limiter. The crawler remains the sole process making arXiv requests.

## Components and data flow

1. **Crawler:** performs discovery, downloads and extracts source versions through the existing serialized arXiv client. For each eligible completed version, it publishes an idempotent parser job.
2. **Redis Streams:** provides a durable stream and consumer group. Redis persistence is enabled with a named volume. Jobs are acknowledged only after parser output and status are safely persisted. Pending jobs can be reclaimed after a worker dies.
3. **Parser workers:** consume jobs independently, read version files from a shared corpus volume, run the existing parser entry point for that version, persist results idempotently, then acknowledge. The Compose service has no fixed `container_name` and supports `docker compose up --scale parser-worker=10`.
4. **Shared storage:** crawler and workers mount the same corpus path. Queue messages contain identifiers, version, relative corpus path, and schema/version metadata; they do not contain source archives or large payloads.

## Adaptive scheduling

The crawler begins with a configurable seed batch (default 10 IDs; user-requested trial may use 100). It grows the next batch in bounded steps while queue backlog remains below a low-water threshold and workers are making progress. It pauses scheduling or reduces the next batch when backlog exceeds a high-water threshold. High/low watermarks, step size, and maximum prefetch are configuration, not fixed timing assumptions. Queue depth alone is not treated as proof of worker health: lack of acknowledgements/progress pauses expansion and surfaces a health warning.

## Reliability and idempotency

Each job has a stable key derived from paper ID, version, and parser operation/schema version. Duplicate publication must not produce duplicate persisted output. Workers use consumer-group acknowledgements only after durable completion; a pending-job recovery policy reclaims jobs from failed workers. Retries are bounded, failures are recorded with reason and attempt count, and poison jobs move to a dead-letter stream for inspection rather than cycling forever. Redis outages stop queue publication and therefore pause further crawl scheduling; the crawler must not silently discard jobs.

## Docker Compose behavior

Compose defines `crawler`, `redis`, and scalable `parser-worker` services. Redis health is checked before dependent services start. Crawler is constrained to one instance by the documented run command and an application-level exclusive lock for the chosen work directory; Compose scaling is documented only for parser workers. A shared bind mount or named volume holds the corpus, with a separate named Redis data volume. Environment variables configure queue name, batch/backlog watermarks, shared paths, and worker concurrency.

## Observability

The crawler records discovered IDs, published jobs, queue depth, batch decisions, crawl durations, and limiter pacing in its existing logs/summary plus queue-specific counters. Workers log job key, claim/reclaim, duration, outcome, and attempts. A trial summary reports total wall time, discovery and crawl time, jobs enqueued/completed/failed, queue backlog over time, and parser throughput.

## Verification

- Unit tests cover stable job keys, duplicate publishing, ACK-after-persist ordering, retry/reclaim, dead-letter handling, adaptive batch growth/pause, and Redis outage behavior.
- Integration tests use a local Redis service and temporary shared storage; they verify that multiple workers partition jobs without duplicate completion and that a crashed worker's pending job is reclaimed.
- Compose validation checks that crawler remains one instance and parser workers scale to ten.
- A 100-ID live trial uses the first 100 IDs in configured `thien` range, a new isolated data directory and work directory, one crawler, existing request pacing, and a queue-backed parser worker pool. Do not overwrite existing corpus/work output.

## Assumptions and decisions

- Initial deployment is one Docker host; shared storage is a local bind mount or Docker volume visible to all containers.
- Redis Streams is selected over a lightweight in-process queue because worker restart recovery and durable pending jobs are required.
- “Parser” means the current M2 TeX gathering/expansion stage, not the crawler's source archive extraction, which remains within the single crawler process.
- The requested initial batch 10–20 is represented by a default of 10 and can be set to 20; the proposed 100-ID runtime trial overrides the adaptive seed to 100 only for that bounded measurement.
- arXiv request pacing and single-connection behavior remain unchanged.
