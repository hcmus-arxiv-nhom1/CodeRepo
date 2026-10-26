<!-- Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules. -->
# Data contract — Group 1

- **Owner:** Đăng (Member 4, A/R) · Thiên, Tiến (consulted) · Drafted 27/10/2026, needs 4 approvals
- Covers the M1 working data and the CSV files read by the book. Statement: [M1 — Data Crawling](https://hcmus-cs-ta.github.io/lab-book/intro-ds/statement/crawling), [M2](https://hcmus-cs-ta.github.io/lab-book/intro-ds/statement/parsing), [M3](https://hcmus-cs-ta.github.io/lab-book/intro-ds/statement/matching).
- Changing any field name, type or path below requires an approved spec first (Rule 3).

## 1. Keys and folders

- New-style arXiv ID `2606.30734` ↔ folder `2606-30734`; old-style IDs (`hep-th/9901001`) are kept as is. Every module uses `src/utils/ids.py`.
- Versions carry the suffix `v<N>`; a paper with one version is `v1`.

```text
data/corpus/                      # --data-root (DATA_ROOT)
  2606-30734/
    metadata.json                 # section 2
    references.json               # section 4
    v1/  v2/ …                    # section 3
work/                             # --work-dir (WORK_DIR): bookkeeping, synced to Drive with the corpus
  versions.jsonl  discovery.jsonl  failed.csv  run_summary.json  runs.jsonl  run.log  run.lock
  profile/<chunk>/                # profiler output per crawl chunk (section 5)
  qa_report.csv                   # validate.py (section 5)
```

## 2. `metadata.json` (one per paper)

| Field | Type | Notes |
| --- | --- | --- |
| `arxiv_id` | string, required | Equals the folder name with `-` → `.` |
| `title` | string | From the arXiv API |
| `authors` | list[string] | In arXiv order |
| `submission_date` | ISO 8601 UTC string `YYYY-MM-DDThh:mm:ssZ` | Date of `v1` |
| `revised_dates` | list[ISO string] | Dates of `v2`…`vN`, increasing |
| `venue` | string \| null | arXiv `journal_ref` |
| `num_versions` | int | Always `1 + len(revised_dates)` |
| `categories` | list[string] | Extra, not part of the `papers` table |
| `doi` | string \| null | Extra, not part of the `papers` table |
| `versions` | list[object] | **Metadata of each version** (spec 0002, item 5): `{"version": int, "date": ISO string, "title": string \| null, "authors": list[string] \| null}`, one entry per version `1…num_versions`, in order |

The paper-level fields describe the latest version and map to the `papers` table. `versions` follows the statement's suggestion to keep *"for each version, its paper metadata"*; the metadata does change between versions (on 100 multi-version test papers the v1 title differed for 14 and the authors for 12). `versions[k].date` always equals `submission_date` / `revised_dates[k-2]`. Semantic Scholar does not distinguish versions, so reference metadata stays per paper (section 4).

## 3. Version folders `vN/`

- Only `.tex` and `.bib` files, copied byte for byte from the archive with their sub-folders. `.tex` content is never edited.
- A version without TeX (PDF/PS/HTML only, or a 404 from arXiv) still has its `vN/` folder, **empty** or holding only `.bib` files: this is the *empty record*; the paper is never dropped.
- Status and reason of every version are in `work/versions.jsonl`; the last record of each `(arxiv_id, version)` is the valid one:

| Field | Meaning |
| --- | --- |
| `status` | `ok` (at least one `.tex`), `no_tex`, `unavailable` (404/410), `failed` (retried later) |
| `kind` | `tar.gz`, `gz`, `tar`, `zip`, `tex`, `pdf`, `ps`, `html.gz`, … |
| `files` | Kept files, relative to `vN/` |
| `archive_bytes` | Size of the downloaded file |
| `bytes_before` / `bytes_after` | Uncompressed size of every file / of the kept files |
| `dropped` | `{extension: {"files", "bytes"}}` left out by the whitelist (spec 0002) |
| `download_s`, `extract_s`, `total_s`, `attempts`, `finished_at` | Timings and attempts |

## 4. `references.json` (one per paper)

```json
{
  "arxiv_id": "2606.30762",
  "status": "ok",
  "s2_paper_id": "d7a3896bc1a28b40aa13c705ad0a7c72d216f25d",
  "fetched_at": "2026-10-27T01:58:45Z",
  "num_references": 218,
  "num_arxiv_references": 181,
  "references": [
    {
      "s2_id": "53c5d8e56ec75937eecd0aec9c6fe6379e505ab1",
      "title": "Realization of fractional Fermi seas",
      "authors": ["Yi Zeng", "Alvise Bastianello", "Sudipta Dhar", "…"],
      "submission_date": "2026-02-19",
      "year": 2026,
      "venue": null,
      "arxiv_id": "2602.17657",
      "external_ids": {"ArXiv": "2602.17657", "CorpusId": 285787392}
    }
  ]
}
```

- `status`: `ok` (S2 knows the paper, even with 0 references) or `not_found`. A failed batch writes **no** file, so the next run retries it.
- **Every reference** returned by S2 is kept, with or without an arXiv ID (*"Keep all crawled references locally"*).
- `s2_id` may be `null` when S2 could not resolve the citation (13.5 % on 700 test papers); the reference is still kept.
- `submission_date` = `publicationDate`, else the year (`"1999"`), else `null` (9.5 % `null` in the test).
- `arxiv_id` is kept as S2 returns it, old-style IDs included.

## 5. Generated files

| File | Written by | Content |
| --- | --- | --- |
| `work/profile/<chunk>/profile_summary.json` | `src/utils/profiler.py` | `wall_s`, `rss_max_mb`, `rss_mean_mb` (resident memory of the main process **plus all child processes**), `disk_max_mb`, `output_mb`, `cpu_count`, `ram_total_mb`, `exit_code` |
| `work/profile/<chunk>/profile_samples.csv` | profiler | `t_sec, rss_total_mb, n_procs, disk_mb` |
| `work/profile/<chunk>/time_v.log` | `/usr/bin/time -v` | Cross-check of wall time and peak RSS |
| `work/qa_report.csv` | `src/qa/validate.py` | `arxiv_id, version, severity (error/warning), check, detail` |

**Benchmark over several chunks.** The official crawl of all 20,000 IDs is the benchmark and runs in chunks of 500 IDs across Colab sessions. `stats.py` aggregates every `profile_summary.json` under `work/profile/`: wall time = sum of the chunks' wall times, peak RAM / peak disk = maximum, mean RAM = duration-weighted mean, output size = final size of the corpus.

## 6. Mapping to the Hugging Face tables (M3)

| Table | Columns | Source |
| --- | --- | --- |
| `papers` | `arxiv_id`, `title`, `authors`, `submission_date`, `revised_dates`, `venue` | `metadata.json`, same field names |
| `references` | `arxiv_id` (citing paper), `ref_arxiv_id`, `ref_title`, `ref_authors`, `ref_submission_date`, `ref_s2_id` | `references.json`: only references with a non-null `arxiv_id`; `ref_arxiv_id = arxiv_id`, `ref_title = title`, `ref_authors = authors`, `ref_submission_date = submission_date`, `ref_s2_id = s2_id` |

M2 (agreed now): a version without TeX must still be recorded as empty (*"record the version as empty rather than skipping it"*), based on its `status` in `versions.jsonl`.

## 7. CSV files for the book (`arxiv-book/data/m1/`), written by `src/analysis/stats.py`

**`papers_stats.csv`** — one row per paper of the selected IDs:

| Column | Definition |
| --- | --- |
| `arxiv_id`, `range_owner` | ID and owner of its range (`config.RANGES`) |
| `n_versions` | `num_versions` (0 when discovery did not find the paper) |
| `n_versions_ok` | Versions whose final status is `ok`, `no_tex` or `unavailable` **and** whose `vN/` exists |
| `has_source` | 1 when at least one version is `ok` |
| `size_before_bytes`, `size_after_bytes` | Sum of `bytes_before` / `bytes_after` over the versions |
| `n_refs_total`, `n_refs_arxiv` | `num_references`, `num_arxiv_references` (0 before `references.json` exists) |
| `n_refs_meta_ok` | References with a non-null `s2_id`, i.e. resolved by S2 |
| `ref_status` | `ok`, `not_found` or `missing` (not crawled yet) |
| `process_sec` | Sum of the versions' `total_s` |

- **Successful paper:** `n_versions > 0` and `n_versions_ok == n_versions` (same as `papers_complete` in `run_summary.json`).
- **Reference metadata success rate:** share of papers with `ref_status == ok`; the notebook also reports `n_refs_meta_ok / n_refs_total`.

**`references_stats.csv`** — one row per source (`arxiv`, `semantic_scholar`): `source, n_requests, n_hit, n_miss, n_http_429, n_http_5xx`. Requests and HTTP codes are counted in `work/run.log`; `n_hit` / `n_miss` are papers found / not found (`discovery.jsonl` for arXiv, `references.json` for S2).

**`benchmark_summary.csv`** — `metric, value, unit, tool` for `wall_time_e2e`, `discovery_time`, `avg_paper_time`, `ram_max`, `ram_mean`, `disk_max`, `output_size` (plus `/usr/bin/time -v` cross-checks), aggregated as in section 5.

**`resource_timeseries.csv`** — `t_sec, rss_total_mb, disk_mb`, the profiler samples of every chunk placed end to end.
