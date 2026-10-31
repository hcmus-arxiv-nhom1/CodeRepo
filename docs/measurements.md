# M1 measurement contract

Restart the Colab runtime before setup. The notebook rejects cached `src` modules
to prevent a checkout being recorded alongside Python code from another revision.

The final benchmark uses CPU-only Google Colab and all 20,000 assigned IDs, all
discovered versions and references. A sample validates measurement behavior; it is
not the final benchmark. Use a new run ID for this implementation. Older pinned runs
remain resumable only with their original revision; do not combine their measurements.

## Required measurements and evidence

| Required measurement | Export / definition |
| --- | --- |
| End-to-end wall time | `benchmark_summary.csv/wall_time_e2e`: sum of completed session walls, including restore, child launch, discovery, source, references, retries, checkpoints, QA and final corpus sync. Offline gaps are excluded and sessions are identified. |
| Average time per assigned paper | `avg_paper_time = wall_time_e2e / required IDs`; failed IDs stay in the denominator. Interpret final values only after full coverage. |
| Time for each paper | `paper_attempt_timings.csv/lifecycle_s`: admission into a chunk attempt until discovery, all required versions and reference retrieval reach terminal states. Includes waiting behind other papers; overlapping lifecycle latencies must not be summed. Every attempt is retained. |
| Source work per paper | `paper_attempt_timings.csv/source_work_s`: sum of measured version processing in that attempt. Cached versions have no new source work. `papers_stats.csv/process_sec` remains a legacy sum of latest version records, not end-to-end paper latency. |
| Entry discovery time | `discovery_time`: sum of recorded pipeline attempt discovery durations from `runs.jsonl`. Interrupted discovery without a summary is missing evidence, not zero; check the timing audit. |
| Maximum / average RAM | `ram_max`, `ram_mean`: RSS of runner/notebook kernel and all descendants, including its baseline and monitoring thread. One-second sampling; mean integrates each observation until the next sample, then weights sessions by sampled duration. This is not all Colab machine memory. |
| Maximum disk | `disk_max`: observed logical file size of local run data/work plus measurement staging, sampled every 30 seconds and at finalization. Includes restore output and temporary checkpoint spools when observed. Short spikes can be missed. Environment/packages, exported ZIP outside those paths and remote Drive storage are excluded. |
| Final output size | `output_size`: actual final corpus size, not the sum of intermediate chunk outputs. `disk_free_min` records observed free bytes on the local filesystem. |
| Corpus/reference statistics | `corpus_summary.csv`: completion and TeX counts, mean uncompressed bytes before/after extension filtering, mean references, reference-list success per required ID, resolved S2-ID rate and metadata completeness (S2 ID + title + authors + date/year) per citation. Unresolved/empty references remain in the denominator. |

The sampling boundary starts after run identity/preflight validation and before
corpus restore, and ends after final corpus sync and shift release. Environment
installation, Drive authorization, final telemetry publication/collection, final
evidence re-export/download and offline gaps are outside this boundary. Earlier
report generation inside the runner is included. Monitoring has real overhead,
which remains in measured runtime; no speedup is claimed by this change.

## Durability and interruptions

`runs/<RUN_ID>/measurements/<SESSION_ID>/` on Drive holds a start record, immutable
checksummed sample segments, paper-event segments and a final summary. Samples and
paper events target publication every 30 seconds, independently of the 300-second
corpus checkpoints. This is a target, not a maximum-loss guarantee: Drive can fail
or stall. Staging stays outside the restore destination so restore can use a fresh
directory while RAM/disk/time are already being observed.

The corpus checkpoint itself is a plain mirror, not an archive: completed version
files, metadata/references and the `work/` journal are copied as real files/folders
under `runs/<RUN_ID>/` on Drive (M2 reads the corpus there directly, no unzip step).
Each file is copied through a temp name and read back hash-verified before an atomic
rename, so a kill mid-copy can never corrupt a file already on Drive. `INDEX.json`
(the manifest of every mirrored file's hash, plus the version-directory list) is
replaced last; a kill between file copies just leaves it pointing at the previous,
still-valid, state. Restore re-verifies every file's hash against `INDEX.json` before
copying it back and raises if any mismatch; there is no older generation to fall back
to, so a corrupted mirrored file blocks restore until it is removed from Drive and
re-checkpointed, instead of silently restoring stale data.

Restore overlays independent telemetry after verifying the corpus checkpoint.
Reports deduplicate paper events copied both through checkpoints and telemetry.
Completed versions may need replay after an interruption; their earlier measured
attempt remains evidence. Raw samples contain phase, UTC time, session-relative
monotonic time, RSS, process count, disk bytes and filesystem free bytes.

A hard kill cannot write its final duration. An unfinished/corrupt session therefore
exports `observed_wall_lower_bound`, lists incomplete sessions, and omits exact
`wall_time_e2e` and `avg_paper_time`. `timing_audit.json` also checks missing chunk
profiles. `coverage.json/final_complete` requires complete corpus and timing evidence;
corpus completeness alone is insufficient. Missing timing is never reconstructed
from the time until somebody restarts Colab.

Paper-event segments also validate sequence continuity and expected final counts.
Missing/corrupt segments remain listed in the audit; export continues with valid
evidence and suppresses exact per-paper values. Discovery that raises an exception
still records its observed elapsed time. A hard kill without that summary remains
incomplete evidence.

## Diagnostic measurements

`pipeline_wall_time`, `pipeline_avg_paper_time`, `crawler_ram_max`, `crawler_ram_mean`
and `chunk_disk_max` retain the narrower chunk-profiler scope. `time_v_max_rss`
is the new-schema name for the legacy `ram_max_main_process` diagnostic. `/usr/bin/time -v`
wraps each chunk's profiler; its maximum RSS is a separate process-accounting
cross-check, not the simultaneous sum of every process's RSS.

`download_s` now spans all fetch attempts with limiter/backoff waits counted once.
`limiter_wait_s` and `transfer_extract_s` partition that fetch interval; the latter
includes HTTP/error/retry processing and streamed extraction, not pure network time.
Do not add reference durations to source wall time because they overlap.

Read `book_csv/measurement_audit.json`, `work/timing_audit.json`, coverage and QA
together. `resource_timeseries.csv` concatenates observed session timelines, with
session IDs retained; offline gaps and unknown interrupted tails are not plotted as
zero. Legacy runs without session telemetry keep chunk-only exports, explicitly
marked as legacy in the measurement audit.

## Before optimizing

Run a new 200-ID sample with the same IDs, all versions and references. Also test a
graceful session-budget handoff/resume and an intentional runtime loss on a separate
small sample; verify restore and incomplete-timing reporting. Retain the original
200-ID baseline untouched. The fake-response test suite does not replace a live
Colab/Drive smoke test.
