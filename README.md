# CodeRepo — Group 1, arXiv Project (Introduction to Data Science, HCMUS)

<!-- Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules. -->

> **Do not rename the organization or the repositories before the poster deadline:** the Moodle submission link would break.

- **Book (Moodle submission link):** <https://hcmus-arxiv-nhom1.github.io/arxiv-book/>
- **Code repository:** <https://github.com/hcmus-arxiv-nhom1/CodeRepo>
- **Statement:** <https://hcmus-cs-ta.github.io/lab-book/intro-ds/statement/>
- Data contract: [`docs/data_contract.md`](docs/data_contract.md) · Specs: [`specs/`](specs/)

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

Use the single notebook [`arxiv_nhom1_crawl_benchmark.ipynb`](src/notebooks/arxiv_nhom1_crawl_benchmark.ipynb) for both sample validation and the official full crawl/benchmark. The final benchmark must cover all four assigned ranges (20,000 IDs), including every discovered version. Samples are never reported as final results.

1. Use the designated group crawl account; create `MyDrive/arxiv-nhom1` once. Set `INITIALIZE_TEAM_FOLDER=True` only for its first initialization, then leave it false. Use Colab Secrets for `ARXIV_CONTACT_EMAIL` and optional `S2_API_KEY`.
2. Choose a CPU/None runtime. Review the immutable implementation revision printed by setup and the selected ID count before running.
3. For a trial: use `MODE="sample"`, `RANGES=("dang",)`, `COUNT=100`, and a unique sample run ID. For final: use `MODE="final"`, all four ranges, `COUNT=None`, references enabled, and a different run ID. Default chunk size is 100.
4. New runs refuse existing run IDs. After a runtime reset, set `RESUME=True` with the same run ID/settings. The notebook restores corpus and journals from verified Drive checkpoints using the saved code revision. No cell deletes an earlier run.
5. Operate one group crawler at a time. Session markers remain advisory. A four-hour session budget stops between chunks; only take over a stale shift after confirming its old runtime has stopped.

Checkpoints target 300 seconds at completed-version boundaries and also run after chunks. Only new/changed completed files are archived; archives are read back and verified before a completion record is published. If a runtime dies during upload, restore uses the latest valid chain. This does not guarantee zero lost work: long versions or failed uploads can exceed the target interval. Live Range recovery does not persist an in-flight download across destroyed runtimes.

Full-session measurements include the runner/notebook kernel and all descendants, restore, crawling, reference completion, retries, checkpoints and final corpus sync. RSS is sampled every second; local run data/work and telemetry staging disk are sampled every 30 seconds. Independent checksummed Drive telemetry targets publication every 30 seconds. The narrower chunk profiler and `/usr/bin/time -v` remain cross-checks. Setup, final evidence re-export/download and offline gaps are excluded. Missing summaries after hard runtime loss produce an observed lower bound and incomplete timing, never an invented exact benchmark.

See the [measurement contract](docs/measurements.md) for scopes and formulas. The notebook displays `benchmark_summary.csv`, `corpus_summary.csv` and the measurement audit. `paper_attempt_timings.csv` retains each paper's admission-to-source/reference-readiness latency and source work, including repeated attempts. Use a **new run ID** for this measurement revision; old runs retain their original code and scope.

Inspect `coverage.json` and the source/reference QA reports, then download `bench_results.zip`. The ZIP includes manifests, dependency/hardware session details, journals, logs, profiles and CSVs; corpus remains in `arxiv-nhom1/runs/<RUN_ID>/checkpoints` for M2. Restore that run with the saved implementation revision and leave source `.tex`/`.bib` files unchanged during parsing.

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
- `--snapshot <file>`: optional Kaggle snapshot for version dates.
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

In code: `gather(version_dir)` returns the root file and the `.tex` files reachable through `\input`/`\include`; `expand(version_dir)` returns the root text with every `\input`/`\include` replaced by the included file.

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
