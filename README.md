# CodeRepo — Group 1, arXiv Project (Introduction to Data Science, HCMUS)

<!-- Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules. -->

> **Do not rename the organization or the repositories before the poster deadline:** the Moodle submission link would break.

- **Book (Moodle submission link):** <https://hcmus-arxiv-nhom1.github.io/arxiv-book/>
- **Code repository:** <https://github.com/hcmus-arxiv-nhom1/CodeRepo>
- **Statement:** <https://hcmus-cs-ta.github.io/lab-book/intro-ds/statement/>
- Data contract: [`docs/data_contract.md`](docs/data_contract.md) · Specs: [`specs/`](specs/)

## Environment

- **Python 3.12**, the version of the Google Colab runtime. Every package is pinned in `src/requirements.txt`; development tools (pytest, ruff, pre-commit) are in `src/requirements-dev.txt`.
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

## Running on Google Colab

| Notebook | When |
| --- | --- |
| [`src/notebooks/colab_crawl.ipynb`](src/notebooks/colab_crawl.ipynb) | The official crawl of the 4 ranges by **one** instance, in shifts. It is also the M1 benchmark: every chunk of 500 IDs runs under `/usr/bin/time -v` and the profiler. Syncs Google Drive after every chunk; a `SHIFT.json` marker on Drive prevents two shifts at once |
| [`src/notebooks/colab_benchmark.ipynb`](src/notebooks/colab_benchmark.ipynb) | Re-run the measurement from a fresh CPU-only runtime (a whole range or a sample), print every performance figure and export the CSV files for the book |

Open with *File → Open notebook → GitHub*, paste the repository URL, pick the notebook, then **Run all**.

## Docker Compose: one crawler and scalable parser workers

The single-process crawler is the `single` Compose profile (`docker compose --profile single up --build`); it is the only container that calls arXiv in that mode and its local limiter stays enabled. For several crawler workers see the next section; do not run both modes at once. Parser workers consume M2 jobs from Redis Streams and share the corpus/output volumes. To scale parsing to ten workers:

```bash
Copy-Item .env.example .env  # PowerShell; use `cp` in Bash
# Set ARXIV_CONTACT_EMAIL before starting a live crawl.
docker compose up --build --scale parser-worker=10
```

The default trial uses the first 100 IDs of `thien` and Docker-managed volumes. Set `QUEUE_SEED=10` or `20` to control the initial scheduling batch. Redis AOF persistence is enabled. `docker compose down` retains named volumes; `docker compose down -v` removes them.

## Docker Compose: crawler workers with one global rate limiter

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
