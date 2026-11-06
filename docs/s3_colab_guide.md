<!-- Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules. -->
# Guide — running the M1 crawl on Google Colab (`colab_m1_s3.ipynb`)

One CPU-only notebook that follows the M1 statement:

- **Discovery:** `arxiv.py` 4.x (`client.results(search)`) resolves every assigned ID and its version list; one `metadata.json` per paper with the metadata of each version.
- **Sources, every version:** the latest version of papers not revised after their S3 package was built comes from **S3** (`s3://arxiv/src/`, Requester Pays); every other version comes from `export.arxiv.org/src/{id}v{N}` (one connection, at least 3 s between requests, descriptive User-Agent). Only `.tex` and `.bib` are kept, byte for byte.
- **References:** Semantic Scholar `GET graph/v1/paper/arXiv:{id}`, one request per paper, flat `references.*` fields, every reference kept.
- **Measurements:** the project's own tools (`src/utils/profiler` format, `src/analysis/stats`) write `papers_stats.csv`, `references_stats.csv`, `benchmark_summary.csv` and `resource_timeseries.csv`.

Design and measurements: [`superpowers/plans/2026-10-29-s3-latest-version-lane.md`](superpowers/plans/2026-10-29-s3-latest-version-lane.md). Output layout: [`data_contract.md`](data_contract.md).

## 1. One-time setup

### 1.1 AWS credentials (only when `USE_S3 = True`)

Do not use root keys. Create a read-only IAM user:

1. **IAM → Users → Create user** (e.g. `m1-colab-s3`), no console access.
2. **Create policy → JSON**, attach it to the user:

   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       { "Effect": "Allow", "Action": ["s3:GetObject"], "Resource": "arn:aws:s3:::arxiv/src/*" }
     ]
   }
   ```

3. **Security credentials → Create access key → Application running outside AWS.** The secret is shown once.
4. **Billing → Budgets**: create an alert (e.g. $10). The requester pays the transfer.

### 1.2 Semantic Scholar API key

Request a key at <https://www.semanticscholar.org/product/api#api-key-form>. **A full run needs it**: without a key the per-paper endpoint answered `429` on almost every request in a local test (shared pool), so references would take days. With a key the limit is 1 request/second: 20,000 papers take about 5.6 hours, in a thread that runs next to the arXiv lane.

### 1.3 Colab Secrets

In Colab open **Secrets** (key icon) and add each name with **Notebook access** switched on. Names must match exactly.

| Secret | Value | Needed |
| --- | --- | --- |
| `ARXIV_CONTACT_EMAIL` | group e-mail, added to the User-Agent sent to arXiv | yes |
| `S2_API_KEY` | Semantic Scholar API key | for references (full run) |
| `AWS_ACCESS_KEY_ID` | access key ID of the IAM user | when `USE_S3 = True` |
| `AWS_SECRET_ACCESS_KEY` | its secret | when `USE_S3 = True` |

Never paste a key into a cell. If you did, delete that key (AWS: IAM → Security credentials) and create a new one.

## 2. Run

1. Upload [`src/notebooks/colab_m1_s3.ipynb`](../src/notebooks/colab_m1_s3.ipynb) (*File → Upload notebook*) or open it from GitHub. Runtime: CPU.
2. **Run all** with `RUN = False` (cell 1). Cell 2 asks for Drive access and installs the pinned dependencies (and `arxiv==4.0.2`). Cell 4 prints the plan: number of IDs, S3 packages, GB and list-price cost. Nothing is crawled; with S3 on, only the manifest (about 5 MB) is read.
3. Set `RUN = True` and **Run all** again.
4. If the session dies or the run stops (`budget_hit`), run again unchanged. State is restored from Drive and finished S3 packages are not read again.

Settings (cell 1):

| Setting | Meaning |
| --- | --- |
| `PRESET` | `"pilot"`: 300 IDs, 3 S3 packages (about 1.6 GB, about $0.14), at most 60 arXiv versions per run, so run it twice to see the resume. `"full"`: the four assigned ranges, 20,000 IDs, 144 packages, about 77 GB (about $6.9), no budget |
| `RUN` | `False` prints the plan only |
| `REFERENCES` | `True` crawls Semantic Scholar |
| `USE_S3` | `False` takes every version from `export` and needs no AWS key |
| `DRIVE_DIR` | where progress is saved; `None` disables Drive |

## 3. What it writes

- `/content/m1/corpus/<id>/metadata.json`, `<id>/vN/*.tex|*.bib`, `<id>/references.json`, as in the data contract.
- `/content/m1/work/`: `versions.jsonl`, `discovery.jsonl`, `s3_packages.jsonl`, `failed.csv`, `run_summary.json`, `runs.jsonl`, `run.log`, and `profile/<run>/` (one folder per run, resumed runs included).
- `/content/m1/tables/`: the four CSV files for the book. Wall time is the sum over all runs, RAM and disk are the maximum over runs, output size is the final corpus size.
- On Drive (`DRIVE_DIR`): a copy of `work/`, `tables/` and `parts/*.tar` with the corpus, written incrementally.

The last cell prints the figures the report needs: papers crawled and success rate, mean size before/after removing figures, mean references per paper and reference success rate, and the benchmark table (wall time, discovery time, time per paper, RAM max/mean, peak disk, output size). With S3 on, it also re-fetches a few S3 papers through `export` and compares them byte for byte (`PASS`/`FAIL`).

`versions.jsonl` records written by the S3 lane carry two extra fields, `source` (`s3` / `export`) and `s3_package`, which are not in the data contract.

## 4. Troubleshooting

| Symptom | Cause |
| --- | --- |
| "Không đọc được manifest S3" | missing or wrong AWS secret, secret without Notebook access, or the policy lacks `s3:GetObject` on `arxiv/src/*` |
| `AccessDenied` | wrong `Resource` in the policy, or the policy is not attached to the user |
| lots of `S2 GET ... 429` in `work/run.log` | no or invalid `S2_API_KEY` |
| pip conflict in cell 2 | restart the runtime, then run from cell 2 |
| run stops with `budget_hit` | expected for `pilot`; run again to continue |
| an S3 package fails MD5 | retried 3 times; if it still fails, its versions are fetched through `export` in the second pass |

## 5. Known limits

- Tested on a local machine against real arXiv and a real S3 package (30 IDs, 42 versions: discovery, S3, export, budget stop, resume, restore from a Drive copy, S3-vs-export check), not yet on Colab. Nothing is measured on Colab yet.
- `metadata.json` produced with `arxiv.py` was identical, field by field, to the one produced earlier by the repository's own discovery on those 30 papers.
- The per-paper Semantic Scholar code was tested with mocked responses (ok, 404, server error, resume), not against the live API, because no key was available.
- S3 cost is the AWS list price (about $0.09/GB); not yet confirmed by an invoice.
- A package is a snapshot: a paper revised after it is fetched through `export`. The check uses the package `timestamp` from the manifest, assumed UTC, with a one-day margin (`STALE_MARGIN_DAYS`); the margin itself is untested. About 15 % of papers in the package examined were stale.
- The notebook clones this repository's `main` and imports its modules read-only; it changes no file of the repository.
