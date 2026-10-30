<!-- Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules. -->
# Plan — S3 lane for the latest version, to finish M1 faster

- **Status:** draft for review · **Date:** 29/10/2026 · **Statement:** [M1 — Data Crawling](https://hcmus-cs-ta.github.io/lab-book/intro-ds/statement/crawling)
- **Nothing in `src/` is changed by this plan.** Phase 0 lives in the new notebook `src/notebooks/colab_m1_s3.ipynb` (download only).

## 1. What was measured (real S3, 29/10/2026)

| Fact | Value |
| --- | --- |
| Manifest | 13,449 packages; `size` in the manifest is **not** the TAR size (off by ~112 KB), use `ContentLength`; `md5sum` is the TAR MD5 and matched on 2 packages |
| Package for 2606 | ~165 papers, ~530 MB, ordered by ID, one member per paper: `2606/2606.05715.gz` or `.pdf`, **no `vN` suffix → latest version only** |
| The 4 assigned ranges | 36–37 packages each, 19.1–19.6 GB each, 5,026–5,145 papers per 5,000 wanted (**~98 % useful**). Union ≈ **76.7 GB**, ≈ **$6.9** at $0.09/GB (list price; Cost Explorer lags 24 h, not yet confirmed) |
| Throughput here | CLI 27 MB/s (19 s for 529 MB); one boto3 stream 14.5 MB/s → use several streams |
| Local trial (1,000 papers, 28/10) | 1,289 versions, 1.29 versions/paper; wall 14,262 s ≈ **10.5 s per version** against the ideal 3.05 s (429s, cut bodies, big downloads) |

## 2. Why S3 only helps one lane, and which

The arXiv limit (3 s, one connection, whole group) is the critical path. For 20,000 papers (≈ 25,600 versions at 1.28 per paper):

| Plan | arXiv requests for sources | Ideal (3.05 s) | At the measured 10.5 s/version |
| --- | --- | --- | --- |
| All versions through `export.arxiv.org` (current crawler) | ~25,600 | 21.7 h | ~75 h |
| **Latest version from S3**, older versions from `export` | ~5,600 | **4.7 h** | **~16 h** |

The saving is the latest version of every paper, and papers with one version (78 % of them) never touch arXiv for sources. The 77 GB cannot be stored on Colab (about 70 GB free, unverified), so S3 is **streamed** into extraction (no TAR on disk); Phase 0 already has `MODE = "stream_verify"` for the download half.

## 3. Phases

| # | Phase | What | Gate to continue |
| --- | --- | --- | --- |
| 0 | **S3 download** (done in notebook) | Manifest → plan → `stream_verify` (MD5 on the fly, no disk) or `disk` for pilots | MD5 equal, `ContentLength` equal, MB/s measured on **Colab**, with 2 / 4 / 6 workers |
| 1 | **Equivalence gate** (the one that can invalidate everything) | For ~20 papers (some multi-version, some PDF-only, some revised after the TAR timestamp): fetch the latest version from `export` and compare the kept `.tex`/`.bib` byte for byte with the S3 member (same `extract.py` rules). Check what `.gz` members contain (single file or tar.gz) | 100 % identical. Otherwise S3 is dropped for the mismatching kind, or entirely (fallback below). Cost ≈ 25 requests ≈ 2 min |
| 2 | **Discovery** | Existing `discovery.py`: API batches of 100 (+ versioned queries) → `num_versions` and version dates for all 20,000 IDs. ≈ 260 requests ≈ 13 min of arXiv limiter. It starts first because it feeds all other lanes | `papers_found` = IDs requested (missing ones listed) |
| 3 | **Three lanes at once** (Colab threads, separate limiters) | **A (critical path)** arXiv `/src/` for versions `v1…v(N-1)` + any latest version the S3 copy cannot cover; **B** S3 stream → extract latest version (4 streams, extraction ≈ 20 ms/version); **C** Semantic Scholar `POST /paper/batch`, 100 IDs per request (spec 0001, ≈ 200 requests ≈ 4–5 min). Lane A starts as soon as the first discovery batch lands | Every `(paper, version)` has a record in `versions.jsonl` |
| 4 | **Reconcile** | Stale rule: the S3 copy of version N is valid only if the date of vN (API) ≤ package `timestamp` (−1 day margin); otherwise queue vN for lane A. Retry `failed`. Run `qa/validate.py` | 0 missing versions, 0 unexplained empty `vN/` |
| 5 | **Metrics** | Wall time per lane and end to end, discovery time, RAM max/mean (`psutil` sampling), disk peak, corpus size, sizes before/after removing figures, success rates (papers, versions, references) | Numbers reproducible from logs |
| 6 | **Colab orchestration + report** | Drive checkpoints and the `SHIFT.json` marker already in `colab_crawl.ipynb`; README; Jupyter Book section with figures generated at build time; one paragraph on the S3 lane | Fresh CPU-only runtime reproduces the run |

## 4. Order that minimises wall time

`Phase 1 (2 min)` → `Phase 2 discovery` → start lane A the moment papers with `num_versions > 1` are known; lanes B and C run underneath it and finish long before A (S3 ≈ 25–50 min with 4 streams, S2 ≈ 5 min). End-to-end wall ≈ discovery + lane A. Any waiting on S3 for lane A is avoidable: lane A never needs S3 output.

## 5. Risks and decisions for the team

1. **The statement says versions are fetched through `export.arxiv.org/src/{id}v{N}`** and does not mention S3. Efficiency is graded, but "appropriate methods" is too. Mitigation: Phase 1 proves identical output, the report says exactly what came from S3 and why, and the crawler can run with lane B switched off (current behaviour). **Decide with the lecturer/team before the full run.**
2. **Stale packages:** TARs are snapshots (2606_041 is dated 2026-07-05). Handled by the Phase 4 rule; its date logic must be tested against papers revised after the snapshot.
3. **Cost:** ≈ $6.9 at list price, unconfirmed. The account in use is root: use an IAM user with `s3:GetObject` on `arn:aws:s3:::arxiv/src/*` only, and an AWS Budget alert.
4. **Colab:** disk (no TAR on disk), 12 h sessions (checkpoint after every package), RAM (stream, never load a TAR).
5. **Old-style IDs / `.pdf`-only members** become empty `vN/` records per the data contract.

## 5b. Phase 1 result (29/10/2026, notebook `src/notebooks/colab_m1_s3.ipynb`)

One package (`2606_041`, 134 papers, snapshot 2026-07-05 05:45 UTC assumed), 20 papers compared with `export.arxiv.org/src/{id}v{latest}`:

| Group | Papers | Result |
| --- | --- | --- |
| Latest version dated **before** the snapshot (incl. v2, v3, v4 papers and 3 PDF-only) | 15 | **15 / 15 identical byte for byte** (so `.tex`/`.bib` identical too) |
| Revised **after** the snapshot (stale) | 5 | 5 / 5 differ from the latest; the S3 copy equals an **older** version (v1 four times, v2 once), not necessarily N-1 |

- The staleness rule "S3 is valid iff the date of the latest version ≤ package timestamp" predicted all 20 outcomes. No paper fell within ±1 day of the timestamp, so the margin and the UTC assumption are **not** tested.
- Whole package, from the API: **20 of 134 papers (15 %) are stale**, 34 (25 %) have several versions, 178 versions in total (1.33 per paper).
- Revised request count for the arXiv lane, per 134 papers: 44 older versions + 20 stale latest versions = **64 instead of 178 (36 %)**. Scaled to 20,000 papers that is about 9,500 requests, **≈ 8 h ideal** (not 4.7 h) against ≈ 21.7 h. Older snapshots (2604) may be more stale; only one package was measured.
- Member names: `2606/<id>.gz` (125) or `.pdf` (9); no `vN`. MD5 of the whole package matched the manifest.

## 5c. Phase 2 result (29/10/2026, notebook `src/notebooks/colab_m1_s3.ipynb`)

Same package `2606_041`, range `hieu` (115 papers in the package), TAR read from a local copy (MD5 checked), output in a separate folder, repo untouched (read-only import of `extract.py` and `HttpClient`):

- 96 papers extracted in **2.7 s** (≈ 35 papers/s, median 11 ms per paper): 90 `ok` (`tar.gz`), 6 `no_tex` (PDF-only, empty `vN/`). The 19 stale papers (16.5 %) went to `work/export_queue.jsonl` with reason `s3_snapshot_older_than_latest`, not to the corpus.
- Bytes before / after removing figures for these 96 papers: 506.5 MB / 54.4 MB (mean 5.28 MB → 0.57 MB per paper).
- Corpus-level check against the current crawler (`download_version` on `export.arxiv.org`): 6 papers (2 PDF-only, v1–v4 with 1 to 10 kept files) gave identical file trees (names and SHA-256), identical `files` and identical `dropped` stats. Only 6 papers, one package.
- The whole package output is deleted and nothing is written to `versions.jsonl` if the package MD5 differs from the manifest.
- Records carry two extra fields, `source` and `s3_package`, besides the contract's fields: the contract treats field changes as spec changes, so they need the team's approval (or drop them) before merging into the real `work/versions.jsonl`.
- Not done yet: running on Colab, looping over all 144 packages with resume, metadata files (`metadata.json`), the arXiv lane consuming `export_queue.jsonl`.

## 5d. Phase 3 result (29/10/2026, notebook `src/notebooks/colab_m1_s3.ipynb`)

One orchestrating notebook, repo untouched (read-only imports of `discover`, `process_version`, `extract_stream`, `references.crawl`, `summarize`). Tested locally on a 30-ID slice of `hieu` (42 versions, 1 package read from a local copy):

- `PLAN_ONLY` on the four ranges: **20,000 IDs, 144 packages, 76.7 GB, ~$6.90** list price; per-month last sequence number comes from the S3 manifest (`2606` → 32040, equal to the value in the `iter_id_range` docstring), so the month border of Thiên's range is resolved without guessing.
- Run 1 with `MAX_ARXIV_VERSIONS=4`: S3 lane extracted 25 versions (MD5 of the package checked), the arXiv lane stopped cleanly after 4 versions (`budget_hit`), 13 pending.
- Run 2 (resume): the package was not read again (`nothing_to_do`), the arXiv lane did the remaining 13 versions, **42 / 42 versions complete**, 25 from S3 and 17 from `export`; minimum gap between arXiv requests 3.015 s.
- Run 3 after deleting the whole local output (Colab disk lost): `work/` and `parts/*.tar` restored from the Drive copy, **0 arXiv requests**, corpus identical to the one before (341 files, SHA-256).
- Not tested yet: more than one S3 worker, several packages, the Semantic Scholar thread (`REFERENCES=True`), 2607 packages, a range crossing two months, a Colab session, scale. Discovery for 20,000 IDs is not measured (30 IDs: 4 s).

## 6. Fallback

If Phase 1 fails: keep S3 only for the benchmark idea in the report and run the current crawler (≈ 22–75 h). Nothing else in the plan depends on S3.
