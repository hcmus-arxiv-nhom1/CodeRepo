<!-- Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules. -->
# Spec 0002 — Crawler tuning for the official crawl

- **Author:** Thiên (Member 1) · **Reviewers:** Đăng, Hiếu · **Status:** awaiting review
- **Date:** 27/10/2026 · **Branch:** `feat/crawler-tuning`

## Context

The lecturer confirmed that what is graded is how efficient the crawling pipeline is, measured on Google Colab in CPU-only mode. Every group is bound by the same arXiv rule, *"wait at least 3 seconds between successive calls and use one connection at a time"*, and that limit applies to *"all of the machines under your control as a whole"* [1]. Only two levers remain: remove idle time, and never call arXiv in parallel by accident.

## Changes

| # | Change | Reason | Verification |
| --- | --- | --- | --- |
| 1 | `ARXIV_JITTER_S` 0.5 → **0.1 s** (`config.py`) | The mean gap between request starts drops from 3.25 s to 3.05 s, about 6 % faster. The minimum gap stays 3.0 s, so the Terms of Use still hold | `min_request_gap_s` in `run_summary.json` ≥ 3.0 s |
| 2 | **Lock on `work/run.lock`** (`src/crawler/worklock.py`), `flock` on Linux and `msvcrt` on Windows | Two `run_pipeline` processes on the same `work/` (a shift started twice) would call arXiv in parallel. The lock belongs to the open file, so the OS releases it when the process dies: no stale lock | `test_worklock.py`; `test_second_run_on_same_work_dir_is_refused`; `test_lock_is_released_when_a_run_stops_on_an_error` |
| 3 | `semantic_scholar_limiter()` becomes a **singleton** | It used to build a new limiter on every call, so two S2 callers could exceed 1 request/s | `test_semantic_scholar_limiter_is_shared` |
| 4 | **Dropped files** recorded in `versions.jsonl`: field `dropped` = `{extension: {"files", "bytes"}}`; a source without TeX is recorded by kind, e.g. `"(pdf)"` | The report explains the size before and after removing figures. The statement keeps only `.tex` and `.bib` [2], so the numbers must show what was removed (figures, `.bbl`, `.sty`…) | `test_targz_keeps_tex_and_bib_only_and_figure_commands`, end-to-end test |
| 5 | **Per-version metadata** in `metadata.json`: field `versions` = `[{"version", "date", "title", "authors"}]`, from versioned API queries (`2606.30762v1`) in batches of 100 | The statement's suggested organisation keeps *"for each version, its paper metadata"*, and the resulted data is graded on how well it follows the suggested conventions. The metadata really changes between versions: on 100 multi-version test papers (27/10/2026) the v1 title differed from the latest for 14 papers, the authors for 12. Cost: about 15 extra requests per range of 5,000 IDs | `test_discover_resolves_versions_and_writes_metadata` |

| 6 | **Resume a body cut mid-transfer** with `Range: bytes=<received>-` (`http_client.py`); `206` appends, `200` restarts from byte 0, `416` or a wrong `Content-Range` discards the partial file | Where the time goes in a 100-ID chunk of the final code (27/10/2026): 57 % is the 3.05 s pace of the requests that are needed, 21 % large files downloading longer than 3 s, **22 % retries** (10 transfers cut mid-body, 13 × 429). A cut file used to be downloaded again from byte 0: one 45 MB source was cut at 27 MiB five times in a row and ended `failed`. `export.arxiv.org/src/` answers Range requests with `206 Partial Content` and `Accept-Ranges: bytes` (checked 27/10/2026) | `test_cut_body_resumes_with_a_range_request_and_no_backoff`, `test_resume_survives_several_cuts`, `test_server_ignoring_range_restarts_from_zero`, `test_wrong_content_range_discards_the_partial` |
| 7 | **No exponential backoff after a cut transfer**: the next attempt waits only for the limiter's 3 s pace. 429, 5xx and connection errors before any byte keep the `2^n` s backoff and `Retry-After` | A connection dropped mid-body is not a rate-limit signal, so waiting longer only adds idle time; the 3 s minimum between request starts is unchanged | Same tests (`starts == [0.0, 3.0]`) |

Rejected on 27/10/2026: **downloading only the `.tex`/`.bib` members.** `/src/` serves one `.tar.gz` per version; gzip is a sequential stream and tar has no index, so the bytes of a given member cannot be requested without decompressing everything before it, and members come in any order (stopping early would miss `.tex`/`.bib` files placed after the figures). The crawler already writes only `.tex`/`.bib` and deletes each archive right after filtering, so the disk peak holds at most one archive.

## Rejected: a separate extraction worker

The plan proposed moving extraction to a second process. Measured on 1,058 test versions (19–20/10): extraction takes **19 ms at the median, 66 ms at p90, 0.57 s at most**, against the 3.05 s pace of the arXiv lane. Extraction is under 1 % of the time per version, so a second process would only add RAM (also a compared metric) and complexity without speeding up the crawl. The parts worth running in parallel are Semantic Scholar (own thread, own limiter) and the M2 parser (`multiprocessing` per paper).

## Impact

- `metadata.json` gains the `versions` list (paper-level fields unchanged, so the `papers` table is unaffected); `versions.jsonl` gains the `dropped` field; `references.json` is unchanged.
- The 3-second minimum pace and the single arXiv connection are unchanged.

## Verified sources

1. arXiv.org. *Terms of Use for arXiv APIs*. https://info.arxiv.org/help/api/tou.html (read 21/10/2026).
2. HCMUS CS TA. *Project Milestone 1 — Data Crawling*. https://hcmus-cs-ta.github.io/lab-book/intro-ds/statement/crawling (read 25/10/2026).
3. Python 3.12 documentation: `fcntl.flock`, `msvcrt.locking`. https://docs.python.org/3.12/library/fcntl.html, https://docs.python.org/3.12/library/msvcrt.html
