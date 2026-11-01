<!-- Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules. -->
# Spec 0004 — Surviving arXiv throttling

- **Author:** Thiên (Member 1) · **Reviewers:** Đăng, Hiếu, Tiến · **Status:** awaiting review
- **Date:** 02/11/2026 · **Branch:** `fix/throttle-resilience`

## Context

A Colab run on 01/11/2026 (`m1-dang-1000-01`, 1,000 IDs, chunks of 100) stopped after 4 chunks with 399 papers complete. Evidence: `bench_results/` and `rescue_results/`.

- The first 20 minutes ran at the 3 s floor (1.47 GB, no HTTP 429). Then arXiv answered 429 more and more often (11, 26, 33 per 10 minutes) and slowed or cut the transfers: the chunks took 404, 476, 936 and 1,415 s, and the transfer time per version went from 0.10 s to 6.9 s although the later papers were smaller.
- The 429 carries no `Retry-After` (every wait in `run.log` is `2**n` s + jitter). One version (`2604.20926v3`) was refused five times within 33 s, was recorded as `failed`, and the chunk QA raised `RuntimeError`: 600 IDs were never tried.
- Other projects report the same arXiv behaviour in 2026, with throttle windows of minutes and a retry budget "shorter than the window" ([vintage-data #269](https://github.com/cdlethem/vintage-data/issues/269)).

The teaching team accepts several crawl passes and grades how errors are handled ("Discovery and Crawling"), and counts retries and delays in the runtime.

## Changes

| # | Change | Reason | Verification |
| --- | --- | --- | --- |
| 1 | **A chunk that stays incomplete is retried after a cooldown, then recorded and skipped** (`BenchmarkConfig.chunk_attempts = 3`, `chunk_cooldown_s = 300`; wait before try *k+1* is *k* × cooldown). The result lists `incomplete_chunks`; `final_complete` stays false. A child that crashes (exit code ≠ 0, e.g. `BlockedError`) still stops the run | One stubborn version must not stop 20,000 IDs; the pipeline skips finished versions, so a retry costs only the failed ones. Waiting is part of the measured wall time | `test_incomplete_chunk_is_retried_after_a_cooldown_then_recorded_not_fatal` |
| 2 | **Longer retry budget for arXiv** (`config.ARXIV_MAX_ATTEMPTS = 8`, `ARXIV_MAX_BACKOFF_S = 120`): waits 2, 4, 8, 16, 32, 64, 120 s, at most 246 s before giving up | In the 01/11 run 29 of 30 throttled versions recovered after 1–4 attempts; only 5 consecutive 429 failed. The usual case still waits 2–16 s; only a stubborn one waits longer. Discovery's API calls keep their fail-fast 2 attempts (spec 0002 / PR #24) | `test_arxiv_retry_budget_outlasts_a_minutes_long_429_window` |
| 3 | **Adaptive pacing, off by default** (`--adaptive-pacing`, `BenchmarkConfig.adaptive_pacing`): a 429 multiplies the pace by 1.5 (cap 10 s); 20 successes in a row win 0.5 s back, never below the starting pace (or the circuit-breaker pace after a trip). The pace only grows or returns to the starting 3 s, so the Terms of Use minimum always holds | A 429 wastes a request slot and a backoff; slowing down may cost less than being refused. **Not proven**: the trigger of the 01/11 throttle (volume, request count or a shared Colab IP) is unknown, so it is measured on a 200-ID sample against the fixed pace before any use in the official run | `test_adaptive_pacing_*` (4 tests) |
| 4 | **`request_timeline.csv`** in the book CSVs: per minute, arXiv responses by status (200 / 206 / 429 / 5xx), megabytes downloaded, backoff seconds and the pace in force | Tells volume-based from request-based throttling and gives the figure for the book and the poster; `retry_backoff_wait` (PR #24) stays the total | `test_request_timeline_shows_a_throttled_run_minute_by_minute` |

`run_summary.json` also records `adaptive_pacing` and `pace_s_end`.

## Rejected

- **Several connections, threads or IP/VPN switching to get past the rate limit.** The Terms of Use say *"You should not attempt to overcome these limits by increasing the number of machines used to make requests"* and allow arXiv to block access; the statement itself says one connection at a time. The 01/11 run was already throttled with a single connection every 3 s, so more connections or addresses would most likely bring 403s and the circuit breaker (15–30 min pause), for the whole group.
- **Deleting the Colab runtime to obtain another IP.** Same category as above.
- **Raising the rate by asking arXiv** is the sanctioned way (the Terms of Use invite it); that is an e-mail from the group, not a code change.
