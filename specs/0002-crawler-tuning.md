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

## Rejected: a separate extraction worker

The plan proposed moving extraction to a second process. Measured on 1,058 test versions (19–20/10): extraction takes **19 ms at the median, 66 ms at p90, 0.57 s at most**, against the 3.05 s pace of the arXiv lane. Extraction is under 1 % of the time per version, so a second process would only add RAM (also a compared metric) and complexity without speeding up the crawl. The parts worth running in parallel are Semantic Scholar (own thread, own limiter) and the M2 parser (`multiprocessing` per paper).

## Impact

- No change to the `metadata.json` / `references.json` schema; `versions.jsonl` gains the `dropped` field.
- The 3-second minimum pace and the single arXiv connection are unchanged.

## Verified sources

1. arXiv.org. *Terms of Use for arXiv APIs*. https://info.arxiv.org/help/api/tou.html (read 21/10/2026).
2. HCMUS CS TA. *Project Milestone 1 — Data Crawling*. https://hcmus-cs-ta.github.io/lab-book/intro-ds/statement/crawling (read 25/10/2026).
3. Python 3.12 documentation: `fcntl.flock`, `msvcrt.locking`. https://docs.python.org/3.12/library/fcntl.html, https://docs.python.org/3.12/library/msvcrt.html
