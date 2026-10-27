<!-- Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules. -->
# Statistics CSV schema for the book (M1)

> **Status:** proposal by Tiến (Member 2). `src/analysis/stats.py` currently writes the files of [data contract §7](data_contract.md) (`papers_stats.csv`, `references_stats.csv`, `benchmark_summary.csv`, `resource_timeseries.csv`); the two schemas still have to be reconciled.

`src/analysis/stats.py` merges the data of the 4 ranges into the small CSVs below, stored in `data/` of the book repo. The EDA notebook reads only these files (never the corpus), so building the book needs neither Colab nor Drive. Every table has a `range_owner` column ∈ {`thien`, `tien`, `hieu`, `dang`}, the same keys as `config.RANGES`; the whole-corpus row uses `all`.

Conventions: dates in ISO 8601 UTC; sizes in bytes; rates are floats in [0, 1], rounded to 4 digits; an empty cell = no data (never 0 as a stand-in).

## 1. `m1_summary_by_range.csv` — 5 rows (4 ranges + `all`)

| Column | Type | Source / computation | Required statistic (section 1.2) |
| --- | --- | --- | --- |
| `range_owner` | str | range key | — |
| `ids_requested` | int | number of IDs in the range (5,000) | — |
| `papers_found` | int | IDs with a valid `metadata.json` | — |
| `papers_complete` | int | papers whose every version is `ok`/`no_tex`/`unavailable` and has a `vN/` folder | number of papers crawled successfully |
| `paper_success_rate` | float | `papers_complete / ids_requested` | success rate |
| `versions_total` | int | sum of `num_versions` | — |
| `versions_ok`, `versions_no_tex`, `versions_unavailable`, `versions_failed` | int | `versions.jsonl` (latest record of each version) | — |
| `avg_versions_per_paper` | float | `versions_total / papers_found` | — |
| `avg_paper_bytes_before` | float | mean over papers of the summed `bytes_before` of their versions (complete papers only) | average size before removing figures |
| `avg_paper_bytes_after` | float | same with `bytes_after` | average size after removing figures |
| `avg_refs_per_paper` | float | mean number of references (all of them) per paper with a `references.json` | average references per paper |
| `avg_refs_with_arxiv_id` | float | same, only references with a `ref_arxiv_id` | — |
| `ref_success_rate` | float | papers whose references were crawled / `papers_found` | reference metadata success rate |

`all` sums the count columns and recomputes the means / rates over the whole corpus (never a mean of means).

## 2. `m1_papers.csv` — 1 row per paper (at most 20,000 rows)

Used for distributions, box plots and outliers in the EDA.

| Column | Type | Source |
| --- | --- | --- |
| `range_owner`, `arxiv_id` | str | keys |
| `num_versions` | int | `metadata.json` |
| `submission_date` | str | `metadata.json` |
| `has_venue` | bool | `venue` is not null |
| `n_authors` | int | `len(authors)` |
| `versions_ok`, `versions_no_tex`, `versions_unavailable`, `versions_failed` | int | `versions.jsonl` |
| `complete` | bool | as for `papers_complete` |
| `n_tex_files`, `n_bib_files` | int | files kept, all versions |
| `bytes_before`, `bytes_after` | int | sum over the versions |
| `dropped_bytes` | int | `bytes_before - bytes_after` (needs the dropped-files field of spec 0002) |
| `n_refs`, `n_refs_with_arxiv_id` | int | `references.json`; empty if not crawled yet |
| `refs_status` | str | `ok` / `missing` / `failed` |
| `qa_errors` | int | number of `error` rows of the paper in `qa_report.csv` |

## 3. `m1_performance.csv` — 1 row per reported run

Built from `run_summary.json`, `runs.jsonl` and the profiler. Only the official benchmark run (`scope = benchmark`) is used for the performance numbers of the report.

| Column | Type | Source | Required statistic |
| --- | --- | --- | --- |
| `run_id`, `scope` | str | run label; `trial` / `crawl-shift` / `benchmark` | — |
| `range_owner` | str | range or `all` | — |
| `ids` | int | number of IDs run | — |
| `wall_s` | float | `run_summary.wall_s`, end to end | running time |
| `discovery_s` | float | `run_summary.discovery_s` | total entry discovery time |
| `avg_paper_s` | float | `wall_s / ids` | average time per paper |
| `mean_version_s` | float | `run_summary.mean_version_s` | time to process one paper/version |
| `arxiv_requests`, `min_request_gap_s` | int, float | `run_summary` | evidence of politeness compliance |
| `ram_peak_mb`, `ram_mean_mb` | float | profiler: summed RSS of the main process and every child | peak / mean RAM |
| `disk_peak_mb` | float | profiler: `data/` + `work/` | peak disk |
| `output_bytes` | int | `du` of the final corpus | output size |
| `cpu_count`, `ram_total_gb` | int, float | `os.cpu_count()`, `psutil` | testbed configuration |
| `time_v_max_rss_kb` | int | `/usr/bin/time -v`, to cross-check RAM | verifiable |

## 4. `m1_profiler_samples.csv` — time series of the benchmark run

| Column | Type | Note |
| --- | --- | --- |
| `run_id` | str | key into `m1_performance.csv` |
| `t_s` | float | seconds since the start |
| `rss_mb` | float | summed RSS (main process + children) |
| `data_bytes`, `work_bytes` | int | size of the two folders at sampling time |

## 5. Automatic checks

- Each `range_owner` in `m1_papers.csv` has ≤ 5,000 rows; the total equals `papers_found` of `all`.
- `paper_success_rate = papers_complete / ids_requested` agrees between tables 1 and 2.
- `bytes_after <= bytes_before` on every row.
- `avg_paper_bytes_*` in table 1 matches the mean recomputed from table 2.
- `ram_peak_mb >= ram_mean_mb`; `ram_peak_mb` differs from `time_v_max_rss_kb` by no more than a threshold stated in the report.
- Numbers in the report come from these CSVs through code cells, never typed by hand.

## 6. Dependencies on other members

All three were resolved when PRs #11–#13 were merged (27/10/2026):

- `references.json` format (`n_refs`, `ref_success_rate`): spec 0001 (Đăng).
- `dropped_bytes`, `n_tex_files`, `n_bib_files`: dropped-files field in `versions.jsonl` (spec 0002, Thiên).
- `ram_*`, `disk_peak_mb`, `m1_profiler_samples.csv`: `src/utils/profiler.py` (Hiếu).
