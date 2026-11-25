# CodeRepo — Group 1, arXiv Project (Introduction to Data Science, HCMUS)

<!-- Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules. -->

> **Do not rename the organization or the repositories before the poster deadline:** the Moodle submission link would break.

- **Book (Moodle submission link):** <https://hcmus-arxiv-nhom1.github.io/arxiv-book/>
- **Code repository:** <https://github.com/hcmus-arxiv-nhom1/CodeRepo>
- **Statement:** <https://hcmus-cs-ta.github.io/lab-book/intro-ds/statement/>
- Data contract: [`docs/data_contract.md`](docs/data_contract.md) · Specs: [`docs/specs/`](docs/specs/)

## Environment

- **Python 3.12 or 3.13**: Google Colab runs 3.13 (checked 30/10/2026). `src/requirements.txt` gives version ranges that match the packages Colab already has, so the notebooks install with `pip --no-deps`; development tools (pytest, ruff, pre-commit) are in `src/requirements-dev.txt`.
- No GPU needed. The official benchmark runs on **Google Colab, CPU-only**.

```bash
# pip + venv
python -m venv .venv
source .venv/bin/activate              # Windows: .venv\Scripts\activate
pip install -r src/requirements.txt -r src/requirements-dev.txt
pre-commit install                     # ruff, nbstripout, blocks files > 500 KB

# or uv
uv venv .venv --python 3.12
uv pip install -r src/requirements.txt -r src/requirements-dev.txt

# checks
ruff check . && ruff format --check . && pytest
```

### Secrets

Never commit secrets. On Colab use **Secrets** (key icon); locally use environment variables.

| Variable | Used for |
| --- | --- |
| `S2_API_KEY` | Semantic Scholar API key: 1.1 s pace instead of 3 s, far fewer 429 answers |
| `ARXIV_CONTACT_EMAIL` | Group e-mail added to the User-Agent sent to arXiv |
| `DATA_ROOT`, `WORK_DIR` | Override the defaults `data/corpus` and `work` (same as `--data-root`, `--work-dir`) |

## Running on Google Colab (CPU-only)

The [crawl and benchmark attempt](src/artifacts/attempts/arxiv_nhom1_crawl_benchmark.ipynb) is retained as an artifact. The five-run workflow uses the [only-v1](src/notebooks/arxiv_nhom1_only_v1_benchmark.ipynb) and [multi-version](src/notebooks/arxiv_nhom1_multi_version_benchmark.ipynb) notebooks; the final benchmark covers all four assigned ranges (20,000 IDs) and their discovered versions. The historical attempt is described below for reference.

1. Upload the prepared `arxiv-assigned-20000.jsonl` to `MyDrive/arxiv-inputs/`, or edit `SNAPSHOT_DRIVE_PATH`. Both backends copy this verified input to local Colab disk.
2. Set `USE_HUGGINGFACE=True` for the private HF Dataset and enable its writable `HF_TOKEN` Colab Secret, or set it to `False` for Drive output. For Drive create the intended team folder, initialize its `TEAM.json` once using `INITIALIZE_TEAM_FOLDER=True`, and set it false afterwards. `S2_API_KEY` and `ARXIV_CONTACT_EMAIL` are optional Colab Secrets.
3. The default sample selects 500 Dang IDs starting at offset 500, with chunk size 100. Set a new run ID. For final mode select all four ranges, set `COUNT=None` and enable references. Verify scope, revision and destination in preflight.
4. Run one crawler at a time. Checkpoints target 300 seconds at completed-version boundaries and at chunk boundaries; long versions, storage and quota delays can extend the target. After runtime loss, confirm the old runtime stopped, then resume with the exact same run ID, backend, settings, snapshot bytes/path and saved code revision. Do not resume the same run concurrently. SHIFT markers are no longer used.
5. Inspect coverage, timing and cleanup diagnostics. Download evidence; if finalization fails, the separate rescue cell packages local raw data and logs before runtime reset. Uncommitted work may need replay.

For the S3 + arXiv hybrid notebook, including the team's prepared Drive bulk,
smoke-test configuration, limitations, checkpoints and resume, see the
[Vietnamese teammate guide](docs/huong_dan_colab_hybrid.md).

Drive output is under `runs/<RUN_ID>/data/corpus/` and `work/`. SHA-addressed previous mutable contents support restore after an interrupted index update; the visible mirror may be ahead of the committed index, so restore and verify before M2. HF stores raw corpus in sharded paths and uses a pinned completed revision; restore produces flat local M2 paths. See [HF setup](docs/huggingface_colab.md) and the [measurement contract](docs/measurements.md).

Session measurements include the runner/notebook kernel and descendants, crawling, reference completion and checkpoints. RSS sampling targets one second, disk 30 seconds; independent telemetry targets 30 seconds on Drive and 120 seconds on HF. Snapshot copy and initial manifest hashing are setup outside the sampler; the filtered snapshot is included in sampled disk. Final telemetry/evidence publication and offline gaps are excluded from sampler wall. Missing session evidence is incomplete, never an invented exact benchmark.

## Docker Compose: crawler workers with one global rate limiter

The official crawl and the M1 benchmark run on Colab (above). Compose is an optional local setup; in it, **every** arXiv request goes through the `coordinator`, so there is never a second arXiv client next to it. Never run `python -m src.run_pipeline` against arXiv while the Compose cluster is up: it has its own local limiter. Parser workers consume M2 jobs from Redis Streams (`--scale parser-worker=10`). Redis AOF persistence is enabled; `docker compose down -v` removes the volumes.

```bash
Copy-Item .env.example .env  # PowerShell; use `cp` in Bash. Set ARXIV_CONTACT_EMAIL first.
```


Three services share one arXiv pace of at least 3 s between request starts, one request in flight:

- `coordinator`: the only place that decides when an arXiv request may start. Run exactly one instance; never scale it. A worker that stops renewing its lease (container killed) loses the turn after `LEASE_TTL_S` seconds.
- `crawler-worker`: takes download jobs from the Redis stream `download-jobs` and hands `ok` versions to the parser stream. Count set by `CRAWLER_WORKERS` (default 3), or `--scale crawler-worker=N`.
- `crawler-producer`: discovers metadata for the ID range, publishes one job per missing version, waits for the queue to drain and writes `work/run_summary.json` and `work/failed.csv`.

```bash
docker compose up -d --build redis coordinator crawler-worker parser-worker
docker compose run --rm crawler-producer  # CRAWL_RANGE, CRAWL_OFFSET, CRAWL_LIMIT in .env pick the IDs
```

More workers never raise the arXiv request rate; they only keep the lane busy while other work (extraction, disk) happens and let the crawl continue when a worker dies. `run_summary.json` reports the achieved `coordinator.min_gap_s`, `dead_letter` and `papers_complete`. Jobs failing `MAX_ATTEMPTS` times land in the stream `download-jobs:dead`; jobs are deduplicated by id, so to retry one delete its key first: `DEL download-jobs:job:<job_id>`.

The producer discovers metadata in chunks whose size follows the queued work (download-jobs plus parser-jobs): 5 IDs while the queues are empty, up to 100 once they hold 100 jobs (`DISCOVERY_TIERS`, `DISCOVERY_BACKLOG_STEPS`). A chunk that arXiv rejects lowers the size for `DISCOVERY_COOLDOWN_ROUNDS` rounds. IDs that already have `metadata.json` cost no request. `run_summary.json` reports `time_to_first_job_s`, `discovery_api_requests`, `discovery_oai_fallbacks` and `discovery_tier_history`.

## Crawler (M1)

Run from the repository root.

```bash
# List the IDs of a range (no network)
python -m src.run_pipeline --range thien --dry-run

# Crawl one member's range (thien | tien | hieu | dang, defined in src/config.py)
python -m src.run_pipeline --range thien --no-progress

# Part of a range: skip the first 1207 IDs, take 200 (across the June/July border)
python -m src.run_pipeline --range thien --offset 1207 --limit 200 --work-dir work/trial-thien

# Any range or list of IDs
python -m src.run_pipeline --start 2606.30734 --end 2607.03693 --month-end 2606=32040
python -m src.run_pipeline --ids 2606.30762,2607.00001
```

- **Resume:** after an interruption (lost Colab session, Ctrl+C, kill) run **the same command** again: finished versions are skipped, failed ones retried. On Colab, sync both `data/corpus/` **and** `work/`, otherwise there is nothing to resume from.
- `--month-end YYMM=N`: last sequence number of a month the range crosses. June 2026 was confirmed with the API as `2606.32040` (stored in `config.MONTH_END`).
- `--keep-ext .tex,.bib,.bbl`: change the kept extensions (default `.tex,.bib`, as required by M1).
- `--snapshot <file>`: optional Kaggle snapshot-first discovery, with API enrichment for historical versions.
- `--adaptive-pacing`: after an HTTP 429 slow the arXiv pace by 1.5x (cap 10 s) and win it back after 20 successes; never below 3 s. Off by default, see [spec 0004](docs/specs/0004-throttle-resilience.md).
- `--no-references`: skip Semantic Scholar; `--refresh-metadata`: run discovery again for IDs that already have `metadata.json`.

### How the crawler calls arXiv

- Only `export.arxiv.org` (API, `/src/`) and `oaipmh.arxiv.org`. Every request goes through **one** shared `RateLimiter`: at most one request in flight, request starts ≥ 3 s apart plus a 0–0.1 s jitter. `run.log` records the `gap` of every request for auditing. Only one run at a time may use a `--work-dir` (`work/run.lock`).
- Discovery: arXiv API `id_list`, 100 IDs per request (about 50 requests for 5,000 IDs). For papers with ≥ 3 versions, the dates of v2..v(N-1) come from versioned IDs (`2606.30762v2`); OAI-PMH `arXivRaw` is the fallback.
- Every version is downloaded from `/src/{id}vN`, its type is detected by magic bytes (tar.gz, single gzip, tar, zip, PDF/PS, HTML) and only whitelisted files are kept.
- Retries on 429/5xx/timeouts/broken transfers, at most 5 attempts, `2^n` s backoff, honouring `Retry-After`. Three consecutive 403s pause for 15–30 minutes and slow the pace to 5 s; more than three pauses stop the run (`BlockedError`) so it can be resumed later.

### Reference metadata (Semantic Scholar)

`run_pipeline` runs `src/crawler/references.py` in a background thread while the arXiv lane waits on its 3-second pace. One `POST /paper/batch` call fetches the complete reference lists of 100 papers, so 20,000 papers need about 200 requests. Every reference is kept, with or without an arXiv ID. To crawl references on their own (e.g. after getting an API key):

```bash
python -m src.crawler.references --data-root data/corpus
```

### Output

```text
data/corpus/                 # --data-root: M1 data only
  2606-30734/                # arXiv ID as yymm-id
    metadata.json            # arxiv_id, title, authors[], submission_date, revised_dates[], venue, num_versions, categories[], doi
    references.json          # every Semantic Scholar reference (status, title, authors, date, arXiv ID, S2 ID)
    v1/                      # .tex/.bib of version 1 only; empty when the version has no TeX
    v2/
work/                        # --work-dir: crawler bookkeeping, not part of the corpus
  versions.jsonl             # one line per version: status, kind, files, bytes before/after, dropped files, timings
  discovery.jsonl            # one line per ID: found/missing, source (api/oai/snapshot)
  failed.csv                 # what still fails after the last run
  run_summary.json           # figures of the last run (runs.jsonl keeps every run)
  run.log                    # every request with its timestamp
  profile/<chunk>/           # profiler output and /usr/bin/time -v log per chunk
```

Dates in `metadata.json` are ISO 8601 UTC (`2026-06-29T18:01:23Z`); the full schema is in [`docs/data_contract.md`](docs/data_contract.md). Version status in `versions.jsonl`: `ok` (has TeX), `no_tex` (PDF/PS/HTML → empty `vN/`), `unavailable` (404 from arXiv → empty `vN/`), `failed` (retried on the next run).

## QA, statistics and performance

```bash
# Integrity check of a range against the data contract -> work/qa_report.csv (exit 1 on errors)
python -m src.qa.validate --range thien --data-root data/corpus --work-dir work

# RAM (all processes) and disk of a whole run
python -m src.utils.profiler --out work/profile/run1 --watch data/corpus --watch work -- \
    python -m src.run_pipeline --range thien --no-progress

# Statistics CSV files for the book (replace the placeholder CSVs of arxiv-book/data/m1);
# sums every work/profile/<chunk>/ of the official crawl
python -m src.analysis.stats --data-root data/corpus --work-dir work --out ../arxiv-book/data/m1
```

## Multi-file gathering (M2)

```bash
# Find the root .tex of every version, count ambiguous roots and missing \input targets
python -m src.parser.gather data/corpus

# Pick ~50 varied papers for M2 development and copy them to a separate folder
python -m src.parser.sample_set --data-root data/corpus --work-dir work --out data/m2-sample
```

In code: `gather(version_dir)` returns the root file and the `.tex` files reachable through `\input`/`\include`; `expand(version_dir)` returns the root text with every `\input`/`\include` replaced by the included file. Text that LaTeX never typesets (`comment` environments, `\iffalse … \fi`) is dropped first, so its includes are not followed.

## Parser (M2): running the whole corpus

One command parses every paper and every version, in shards, and can be stopped and started again (spec [0005](docs/specs/0005-m2-runner.md)):

```bash
# 20 papers first, to check the stages and measure the time per paper
python -m src.parser.run --data-root data/corpus --out-dir data/m2 --work-dir work/m2 --limit 20

# the whole corpus, two processes (the benchmark setting); the crawler's work/ explains empty versions
python -m src.parser.run --data-root data/corpus --out-dir data/m2 --work-dir work/m2 \
    --workers 2 --crawl-work-dir work

# only gather + references (no hierarchy, no tables): where the parser will have trouble
python -m src.parser.run --data-root data/corpus --work-dir work/scan --scan
python -m src.parser.bibitem data/corpus/2606-30762/v1          # the references of one version
```

- **Stop and resume:** press Ctrl+C or lose the session, then run *the same command*. A shard (`--shard-papers 100` papers) is renamed to `<out-dir>/shard-NNNNN/` and then recorded in `<work-dir>/shards.jsonl`; papers recorded there are skipped, a half-written shard is deleted. To parse a corpus that grew since, use a new `--out-dir`.
- **Versions without TeX** are never skipped: `empty` (no `.tex`), `no_root` (no `\documentclass`), `missing` (no `vN/` folder) and `error` are listed in `<work-dir>/m2_versions.csv`, which is the source of the coverage statistics (`m2_papers.csv` has one row per paper).
- **References** come from `\bibitem` and from the `.bib` files the paper names (`\bibliography`, `\addbibresource`, `filecontents`); only the entries the text cites are kept, as BibTeX prints only those.
- Stages are the functions named in `src/parser/run.py` (`STAGES`); the runner refuses to start while one cannot be imported. Exit codes: 0 done, 2 another run uses the work directory, 3 a stage is missing, 4 `--verify-done` found a changed paper, 130 interrupted.
- **The stages** are `src/parser/{normalize,hierarchy,sentences,bibitem,dedup,writer}.py`: cleanup and math normalisation, the tree (sections, sentences, formulas, figures; a table is a figure), sentence splitting, references (`\bibitem`, `.bib`, and the `.bbl` the crawler keeps for a version with no other reference list), deduplication across versions (shared element IDs such as `2310-12345#s7`, united references with renamed `\cite` keys) and the three zstd Parquet tables of the statement. See the Book chapter "Parsing and standardization methodology" for the rules.
- **Checks and measurements:** `python -m src.qa.check_tables --out-dir data/m2` (types, unique IDs, every parent exists, one root per version, every node reachable), `python -m src.qa.audit_structure --data-root data/corpus --out-dir data/m2` (headings, formulas, floats and list items counted in the source versus the tree) and `python -m src.benchmark.m2_benchmark --data-root data/corpus --out-dir data/m2 --work-dir work/m2 --report bench_m2 -- --workers 2` (wall time, RAM and output size, in the CSV format of the Book). The official benchmark runs on a Colab CPU runtime.
- **Separate recovered BBL input:** `--bbl-recovery-dir <run-01>` supplies a recovery journal and files alongside the original corpus roots. They are matched by paper/version during parsing; neither corpus is modified. The parser verifies hashes and pins corpus and settings for resume. This does not add the recovery operation to the M1 benchmark. See [M2 flow](docs/m2_parser_flow.md); Colab smoke and interrupted-resume verification are pending.
- **Smoke and full-run instructions:** [Run M2 on Colab](docs/huong_dan_chay_m2_smoke_va_full.md) describes the 1,000-paper default, progress, input paths and resume.
- **The whole corpus on Colab:** the group crawl is five runs (`only-v1-all` and one `multi-*` run per member), each with `data/corpus` and `work/` on Drive. Open the [M2 notebook](src/notebooks/arxiv_nhom1_m2_parse_benchmark.ipynb) on a CPU runtime: it checks that the five runs are complete (20,000 IDs), restores them to the local disk with SHA-256 verification (setup, not benchmarked), runs `m2_benchmark` over all of them as one corpus (`--data-root` and `--crawl-work-dir` repeated, `--expect-assigned` stops unless the papers are exactly the assigned IDs), copies the state to Drive every 10 minutes and exports the CSV files of the Book. After a lost runtime, open it again with `RESUME = True` and the same `M2_RUN_ID`; every session is logged in `m2_sessions.jsonl` and the report adds them up.
- **Eyeball audit (30 random papers):** `python -m src.qa.audit_sample draw --out-dir data/m2 --work-dir work/m2 --data-root data/corpus --report audit -n 30 --seed 2026` writes one Markdown file per paper (rebuilt tree next to the source path) and a blank score sheet; after reading, `python -m src.qa.audit_sample score audit/audit_sheet.csv --out audit/audit_summary.csv` gives the accuracy per criterion and the error list for the book. `audit/` is git-ignored.

## Reference Matching Pipeline (M3)

Milestone 3 matches each standardized BibTeX entry from Milestone 2 (`bibtex` table, filtered run `m2-full-no-empty-01` with 0 QA issues across 20,000 papers) to its corresponding arXiv-keyed crawled reference from Milestone 1 (`references` table) within the same publication, ranking the top-5 candidate references per entry (evaluated by **MRR@5**). All methods in this repository are classical (no LLMs or pretrained external models).

### Label Matrix Format

Ground-truth labels are stored outside the code repository (staged under `labels/` in the published Hugging Face dataset) as one CSV file per publication: `labels/<method>/<yymm-num>.csv`, where `<method>` is `manual` or `auto`.

```text
citation_key,2602.17657,2401.00001,...   # columns: candidate ref_arxiv_id values of the paper
smith2020,no,yes,...                      # rows: BibTeX entries; cells are strictly yes/no
```

- Candidate columns are the deduplicated `references` rows of that publication carrying a non-null `arxiv_id`, in original order (`src/m3/labels.py`).
- Each labeled row has **at most one** `yes` cell (the true reference), or all `no` cells when no crawled arXiv reference matches (e.g., books or non-arXiv articles). `read_matrix` rejects malformed matrices.

### Pipeline Steps & Commands

All M3 modules operate in bounded memory (shard-by-shard PyArrow reads) on Google Colab CPU runtimes:

```bash
# 1. Select 20 manual papers (5 per range, Q >= 20 BibTeX entries each, stratified by DOI/URL share)
python -m src.m3.select_papers --m2-dir data/m2 --corpus-dir data/corpus --out m3/selection.csv

# 2. Interactive manual labeling (never pre-fills answers; saves atomically after every entry)
python -m src.m3.label_tool --m2-dir data/m2 --corpus-dir data/corpus --labels-dir m3/labels --arxiv-id 2606.30734

# 3. Export M1 metadata.json & references.json to sharded zstd Parquet (papers & references tables)
python -m src.m3.export_m1_tables --corpus-dir data/corpus --out-dir m3/m1_tables_by_range --split-by-range --shard-papers 500

# 4. Build sharded candidate matrices (arxiv_id, citation_key, ref_arxiv_id)
python -m src.m3.candidates --m2-dir data/m2 --refs-dir m3/m1_tables_all/data/references --out-dir m3/candidates

# 5. Classical rule-based auto-labeling (>= 10% of non-manual papers, stratified across 4 ranges)
#    Cascade: explicit arXiv ID in doi/url/journal -> exact cleaned title -> high title + author -> very high title
python -m src.m3.auto_label run --m2-dir data/m2 --refs-dir m3/m1_tables_all/data/references \
    --labels-dir m3/labels --selection-csv m3/selection.csv --total-corpus-papers 20000 --min-papers 2000
python -m src.m3.auto_label evaluate --m2-dir data/m2 --refs-dir m3/m1_tables_all/data/references \
    --labels-dir m3/labels --out-dir m3/auto_eval

# 6. Blind cross-check (manual labels per member, or auto labels split into auto-match / auto-none)
python -m src.m3.crosscheck sample --method auto --m2-dir data/m2 --corpus-dir data/corpus \
    --labels-dir m3/labels --selection-csv m3/selection.csv --sheet m3/auto_sheet.csv --key m3/auto_key.csv
python -m src.m3.crosscheck score --sheet m3/auto_sheet.csv --key m3/auto_key.csv

# 7. Deterministic Train / Valid / Test split (4 manual + 4 auto in Valid and Test; remaining in Train)
python -m src.m3.split_partitions create --labels-dir m3/labels --selection-csv m3/selection.csv --out m3/partitions.csv
python -m src.m3.split_partitions verify --labels-dir m3/labels --selection-csv m3/selection.csv --partitions m3/partitions.csv

# 8. Train-only feature signal EDA (strictly excludes Valid & Test to prevent leakage)
python -m src.m3.analysis --m2-dir data/m2 --corpus-dir data/corpus --labels-dir m3/labels \
    --partitions m3/partitions.csv --out-dir m3/analysis_train

# 9. Run the trained ranker and export schema-validated predictions Parquet
python -m src.m3.predictions --m2-dir data/m2 --refs-dir m3/m1_tables_all/data/references \
    --labels-dir m3/labels --partitions m3/partitions.csv --out-dir m3/predictions_by_range \
    --ranker <module:function> --split-by-range

# 10. Package all 6 Parquet tables under data/<table>/*.parquet and publish to Hugging Face
python -m src.m3.package_dataset --m1-dir m3/m1_tables_by_range --m2-dir data/m2 \
    --predictions-dir m3/predictions_by_range --labels-dir m3/labels --out-dir m3/packages --mode by-range
HF_TOKEN=... python -m src.m3.hf_dataset --package-dir m3/packages/thien \
    --repo-id <hf-user>/arxiv-nhom1-thien --range thien --labels-dir m3/labels --strict-schema
```

Colab notebooks for M3:
- [`src/notebooks/arxiv_nhom1_m3_labeling.ipynb`](src/notebooks/arxiv_nhom1_m3_labeling.ipynb): manual labeling, cross-check, Train EDA, and HF upload.
- [`src/notebooks/arxiv_nhom1_m3_thien_pipeline.ipynb`](src/notebooks/arxiv_nhom1_m3_thien_pipeline.ipynb): M1 Parquet export, sharded candidates, auto-labeling, and `predictions` export.
- [`src/notebooks/arxiv_nhom1_m3_split_and_package.ipynb`](src/notebooks/arxiv_nhom1_m3_split_and_package.ipynb): frozen `partitions.csv` split, Train EDA, 6-table packaging, and HF strict-schema upload.

### Data Cleaning & Feature Engineering

The ranker shares the cleaning helpers in `src/m3/cleaning.py` and preserves Greek symbols and superscript charges when normalizing math in titles. The table shows evidence from manually labeled Train pairs. Means compare labeled matches with labeled nonmatches.

| Feature group | Why it is used | Manual Train evidence |
| --- | --- | --- |
| Title similarity, directional token coverage and length ratio | Recognize equivalent titles and diagnose citations with a truncated title. | Mean title similarity 0.975 vs. 0.320. In `Brekke2024`, coverage was 1.000 from the entry and 0.714 from the candidate. |
| Surname overlap | Distinguish papers with similar titles and recover evidence when titles are malformed. | Mean overlap 0.932 vs. 0.018. |
| Absolute publication-year difference | Allow a small gap between the arXiv posting and citation year. | Mean difference 0.550 vs. 6.010 years with the model's missing-value encoding. |
| Explicit arXiv ID | Use a direct identifier when the entry provides one. | Mean indicator 0.220 vs. 0.000. |
| Missing title, author and year indicators | Distinguish absent metadata from observed disagreement. | The author-missing rate was 0.5% vs. 0.9%. |

Train the classical logistic ranker from the frozen split, filtered M2 shards and the M1 references export:

```bash
python -m src.m3.train_model --m2-dir data/m2 --refs-dir m3/m1_tables_all/data/references \
    --labels-dir m3/labels --partitions m3/partitions.csv --out-dir m3/ranker
```

For a smoke run, add `--train-limit-per-method 2` and use a separate output directory. Training compares title features, title plus surname overlap, and all features, including year and explicit arXiv ID. Directional title coverage and length ratio help expose partial-title matches. Missing-data indicators let the model distinguish absent metadata from disagreement.

The full run used 2,004 Train papers and 2,439,592 labeled pairs. The selected logistic regression uses all feature groups, `C=0.1` and equal total weight per citation. Its abstention cutoff is 1.804416 in logit units. Valid contained eight papers, split evenly between manual and auto labels. Test evaluation is pending.

| Valid metric | Title similarity | Selected ranker |
| --- | ---: | ---: |
| MRR@5 on all 308 entries | 0.5357 | 0.5390 |
| MRR@5 on 166 matched entries | 0.9940 | 1.0000 |
| No-match F1 | 0.9894 | 0.9965 |

See the [frozen model](src/artifacts/m3-ranker/model.json), [Valid metrics by label source and configuration](src/artifacts/m3-ranker/valid_metrics.json), and [Train feature distributions](src/artifacts/m3-ranker/train_features.png). Point the prediction runtime at the artifact and pass `--ranker src.m3.model:rank_paper`. In section 5 of the Thiên notebook, use `RANKER = "src.m3.model:rank_paper"` and `os.environ["M3_MODEL_ARTIFACT"] = str(Path("src/artifacts/m3-ranker/model.json").resolve())` from the repository directory.

### Published Hugging Face Dataset Schema

Each published dataset repository contains flat, zstd-compressed sharded Parquet tables under `data/` with a YAML `configs` dataset card (`README.md`) and no custom loading script:

| Table | Path | Key Columns & Types |
| --- | --- | --- |
| `papers` | `data/papers/*.parquet` | `arxiv_id` (non-null), `title`, `authors` (`list<string>`), `submission_date`, `revised_dates` (`list<string>`), `venue` |
| `references` | `data/references/*.parquet` | `arxiv_id` (non-null), `ref_arxiv_id` (non-null), `ref_title`, `ref_authors` (`list<string>`), `ref_submission_date`, `ref_s2_id` |
| `elements` | `data/elements/*.parquet` | `element_id` (non-null), `arxiv_id` (non-null), `kind` (`dictionary`), `content` |
| `hierarchy` | `data/hierarchy/*.parquet` | `arxiv_id` (non-null), `version` (`int32`), `element_id` (non-null), `parent_id` (non-null) |
| `bibtex` | `data/bibtex/*.parquet` | `arxiv_id` (non-null), `citation_key` (non-null), `entry_type` (`dictionary`), `title`, `authors` (`list<string>`), `year` (`int32`), `doi`, `journal`, `url` |
| `predictions` | `data/predictions/*.parquet` | `arxiv_id` (non-null), `partition` (`dictionary`: `train`/`valid`/`test`), `citation_key` (non-null), `groundtruth_arxiv_id`, `cand_1`..`cand_5` |

## References

Online documents the crawler and the gathering code rely on:

1. HCMUS CS TA. (2026). *Project Milestone 1 — Data Crawling* and *Milestone 2 — Hierarchical Parsing and Standardization*. https://hcmus-cs-ta.github.io/lab-book/intro-ds/statement/ — keep only `.tex`/`.bib`, empty records, root and `\input`/`\include` rules.
2. arXiv.org. *arXiv API User's Manual*. https://info.arxiv.org/help/api/user-manual.html — `id_list`, version suffix of IDs, Atom fields (`published`, `updated`, `journal_ref`, `doi`).
3. arXiv.org. *Terms of Use for arXiv APIs*. https://info.arxiv.org/help/api/tou.html — at least 3 seconds between requests, one connection.
4. arXiv.org. *Open Archives Initiative (OAI) — arXiv info*. https://info.arxiv.org/help/oa/index.html — OAI-PMH `GetRecord` with `metadataPrefix=arXivRaw` at `https://oaipmh.arxiv.org/oai`.
5. arXiv.org. *Media types*. https://info.arxiv.org/help/mimetypes.html — source formats returned by `/src/` (tar.gz, single gzip, PDF, HTML).
6. Cornell University. (2025). *arXiv Dataset*. Kaggle. https://www.kaggle.com/datasets/Cornell-University/arxiv — optional Kaggle snapshot (`--snapshot`).
7. Allen Institute for AI. (2026). *Semantic Scholar Academic Graph API*. https://api.semanticscholar.org/api-docs/ — `paper/batch`, `paper/{id}/references` and rate limits used by `references.py`.
8. Python Software Foundation. *tarfile*, *gzip*, *zipfile* — Python 3.12 standard library. https://docs.python.org/3.12/library/archiving.html — reading archives member by member without `extractall`.

### Kaggle snapshot-first discovery

Download and unzip `arxiv-metadata-oai-snapshot.json` before starting the benchmark. Set
`SNAPSHOT_PATH = "/content/arxiv-metadata-oai-snapshot.json"` in the Colab notebook (or
pass `snapshot="..."` to `BenchmarkConfig`). Use a new run ID for this code revision.
The CLI also accepts `--snapshot ./arxiv-metadata-oai-snapshot.json`.

With a snapshot configured, valid matching records supply the paper metadata and complete
version dates without base-ID API/OAI calls. Historical version titles/authors still use
versioned API queries; unknown historical fields remain null, never copied from the latest
version. Missing/invalid snapshot records retain the ordinary API/OAI fallback. Version
coverage is explicitly **as of the chosen snapshot**, not a claim of live arXiv freshness.

The benchmark builds `work/snapshot_index.jsonl` once for all selected IDs. Only this small
index and its checksum manifest are checkpointed; the original multi-GB input stays local.
Every new session validates the input SHA-256 against `MANIFEST.json`, and verifies/reuses
the restored index. Keep the input bytes and configured local path unchanged on resume.
Index preparation is sampled as part of the session and added to `discovery_time` in the
Book CSV, with per-session detail in `work/snapshot_preparation/`. Initial manifest checksum
validation and Kaggle download/unzip happen before the session sampler and are setup costs;
they must be disclosed separately from the measured benchmark.

The notebook stays pinned to an immutable implementation commit. Local-only commits must be
published to the configured repository before they can be fetched by a fresh web Colab runtime.
The storage backend is selected explicitly; see the Hugging Face setup guide below.

### Hugging Face output backend

The unified notebook now defaults to a **100-ID HF smoke** with the private Dataset
`PancakeDang/arxiv-nhom1-introds`. Store a writable token as `HF_TOKEN` in Colab Secrets.
See [HF setup, raw layout and resume](docs/huggingface_colab.md) before running.
Set `storage_backend="huggingface", hf_repo_id="PancakeDang/arxiv-nhom1-introds"` in
BenchmarkConfig for programmatic use. The default Python API remains Drive-compatible.
HF mode uses local checkpoint staging, uploads raw deltas, and restores a completed
commit with checksum verification. Existing Drive runs must not be resumed as HF runs.


### Notebook storage selection and recovery

In the current notebook use `USE_HUGGINGFACE=True` for HF or `False` for Drive.
The filtered snapshot is copied from Drive in both modes; this is independent of
the output destination. Use a fresh run ID when changing backend or configuration.
Drive checkpoint restore can fall back to verified hash-addressed prior contents when
a write is interrupted before INDEX publication; `.checkpoint-objects` consumes
additional Drive space. The Drive plain mirror may be ahead of its index after a kill.
The HF backend uses a pinned revision and restores a flat local corpus for M2.
Existing runs keep their saved revision and are not automatically upgraded.
