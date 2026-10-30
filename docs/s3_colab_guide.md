<!-- Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules. -->
# Guide — S3 lane on Google Colab (`colab_m1_s3.ipynb`)

One notebook, CPU-only, that crawls the assigned ID ranges by combining:

- **S3** (`s3://arxiv/src/`, Requester Pays): the **latest version** of every paper that was not revised after its package was built;
- **`export.arxiv.org/src/`** (3 s pace, one connection): every older version, and the latest version of papers revised after the package snapshot;
- **Semantic Scholar** (optional): references.

Design and measurements: [`superpowers/plans/2026-10-29-s3-latest-version-lane.md`](superpowers/plans/2026-10-29-s3-latest-version-lane.md). Output follows [`data_contract.md`](data_contract.md).

> **Before a full run, check with the lecturer.** The M1 statement describes fetching each version through `export.arxiv.org/src/{id}v{N}` and does not mention S3. Set `USE_S3 = False` to run the original crawler path only.

## 1. AWS credentials (once)

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

## 2. Colab Secrets

In Colab open **Secrets** (key icon) and add each name below with **Notebook access** switched on. Names must match exactly.

| Secret | Value | Needed |
| --- | --- | --- |
| `AWS_ACCESS_KEY_ID` | access key ID of the IAM user | when `USE_S3 = True` |
| `AWS_SECRET_ACCESS_KEY` | its secret | when `USE_S3 = True` |
| `ARXIV_CONTACT_EMAIL` | group e-mail, added to the User-Agent sent to arXiv | recommended |
| `S2_API_KEY` | Semantic Scholar API key | when `REFERENCES = True` |

Never paste a key into a notebook cell. If you did, delete that key in IAM and create a new one.

## 3. Run

1. Upload [`src/notebooks/colab_m1_s3.ipynb`](../src/notebooks/colab_m1_s3.ipynb) (*File → Upload notebook*) or open it from GitHub. Runtime: CPU.
2. **Run all** with `RUN = False` (cell 1). It prints the plan and downloads nothing except the S3 manifest (about 5 MB): number of IDs, S3 packages, GB and the list-price cost.
3. Set `RUN = True` and **Run all** again. Allow Drive access when asked.
4. If the session dies or the run stops (`budget_hit`), run again unchanged. State is restored from Drive and finished S3 packages are not read again.

Settings (cell 1):

| Setting | Meaning |
| --- | --- |
| `PRESET` | `"pilot"`: 300 IDs, 3 packages (about 1.6 GB, about $0.14), at most 60 arXiv versions per run. `"full"`: the four assigned ranges, 20,000 IDs, 144 packages, about 77 GB (about $6.9), no arXiv budget |
| `RUN` | `False` prints the plan only |
| `USE_S3` | `False` fetches every version through `export` |
| `REFERENCES` | `True` also crawls Semantic Scholar |
| `DRIVE_DIR` | where progress is saved; `None` disables Drive |
| `WORKERS_S3` | packages read in parallel |

## 4. What it writes

- `/content/m1/corpus/<id>/metadata.json`, `<id>/vN/*.tex|*.bib` (and `references.json` if enabled), as in the data contract.
- `/content/m1/work/`: `versions.jsonl`, `discovery.jsonl`, `s3_packages.jsonl`, `failed.csv`, `run_summary.json`.
- On Drive (`DRIVE_DIR`): a copy of `work/` and `parts/*.tar` with the corpus, written incrementally.

`versions.jsonl` records written by the S3 lane carry two extra fields, `source` (`s3` / `export`) and `s3_package`. They are not in the data contract: agree on them, or drop them, before merging this data into the shared `work/versions.jsonl`.

The last cell checks a few S3 papers against `export` byte for byte (`VERIFY_SAMPLES`) and prints the M1 figures: success rate, size before/after removing figures, time, request count, RAM, peak disk, S3 volume.

## 5. Troubleshooting

| Symptom | Cause |
| --- | --- |
| "Không đọc được manifest S3" | missing or wrong AWS secret, secret without Notebook access, or the policy lacks `s3:GetObject` on `arxiv/src/*` |
| `AccessDenied` | wrong `Resource` in the policy, or the policy is not attached to the user |
| pip conflict in cell 2 | restart the runtime, then run from cell 2 |
| run stops with `budget_hit` | expected for `pilot`; run again to continue |
| package fails MD5 | retried 3 times; if it still fails its versions fall back to `export` in the second pass |

## 6. Known limits

- Tested on a local machine against real S3 and arXiv (one package, 30 IDs, 42 versions), not yet on Colab at scale. Measured on Colab: nothing yet.
- Cost is the AWS list price (about $0.09/GB); not yet confirmed by an invoice.
- A package is a snapshot: a paper revised after it is fetched from `export`. The check uses the package `timestamp` from the manifest, assumed UTC, with a one-day margin (`STALE_MARGIN_DAYS`); the margin itself is untested.
- The notebook clones this repository's `main` and imports its modules read-only; it changes no file of the repository.
