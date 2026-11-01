# Hugging Face Colab smoke and handoff

Open `src/notebooks/arxiv_nhom1_crawl_benchmark.ipynb` from the published
`feat/huggingface-storage` branch. Use a fresh CPU runtime. This notebook defaults to
100 Dang IDs, not the 20,000-ID final benchmark.

1. Add the existing writable Dataset token as **HF_TOKEN** in Colab Secrets and enable
   notebook access. Keep `PancakeDang/arxiv-nhom1-introds` private. No login/token files
   are written by the notebook; the token is passed through the process environment.
2. Upload `arxiv-assigned-20000.jsonl` to `MyDrive/arxiv-inputs/`, beside the full
   snapshot (edit SNAPSHOT_DRIVE_PATH if the input folder differs). Run **Copy the
   filtered snapshot from Drive** before HF authorization. It verifies the expected
   SHA-256 and copies only this 39.99 MB input to /content. No Kaggle token/download
   is needed. Drive is input-only; crawl outputs remain on HF. Start a fresh runtime
   without a local full snapshot. Copy/hash is outside benchmark time; the filtered
   file is included in disk measurements. Switching from the old full input requires
   a new RUN_ID; resume must use the identical filtered bytes/path.
3. Keep USE_HUGGINGFACE=True, HF_REPO_ID="PancakeDang/arxiv-nhom1-introds",
   MODE="sample", RANGES=("dang",), COUNT=100, CHUNK_SIZE=500, RESUME=False and TAKEOVER=False.
   Use a new RUN_ID for a new experiment. The notebook is pinned to the implementation
   commit and the repository is the published DankDaPancake fork.
4. Run setup: it checks private-Dataset read access. The first run-reservation commit checks
   write access before crawling. Neither stage prints the token. Read-only token,
   missing token, missing repo, and public repo fail before crawl work starts.
5. Run preflight and benchmark. Expect `Snapshot: 100/100 IDs indexed`; source requests
   still run through the single arXiv limiter. Periodic checkpoints remain at completed
   version boundaries (300-second target); successful chunk boundaries and final sync
   also publish checkpoints. HF calls may run concurrently; arXiv calls remain serialized.
6. Run the final report/download cells and retain bench_results.zip. Check that the HF
   run contains LATEST.json, INDEX.json, MANIFEST.json, data/, work/, measurements/, and
   book_csv/ before handing over the run in group chat.

## Remote layout and M2

Files remain raw, not corpus ZIPs:

```text
PancakeDang/arxiv-nhom1-introds/
  runs/<RUN_ID>/
    MANIFEST.json
    LATEST.json
    INDEX.json
    data/corpus/2604/205/2604-20535/
      metadata.json
      references.json
      v1/main.tex
    work/...
    measurements/<session>/...
    checkpoint-events/...
    book_csv/...
```

The month/sequence-prefix shards keep folder entry counts small. Resume restores the
original local `data/corpus/2604-20535/v1/` layout for the existing M2 tools. Empty
no_tex/unavailable version directories are reconstructed from INDEX.json. For manual
M2 retrieval, use HFCheckpoints.restore with HubStore and prefix `runs/<RUN_ID>` into
an empty local destination; a plain snapshot_download keeps the remote shard layout.

## Checkpoint consistency

Raw deltas are committed in batches targeting up to 500 files or 32 MiB of input, whichever comes first, with Hub parent-commit checks.
LATEST.json is replaced only after the data INDEX.json can be read at its pinned commit.
A partially completed upload never changes the completed pointer. Restore reads all
corpus/journal files at the pointer revision and verifies hashes, then overlays newer
independent session telemetry. Failed sync keeps local evidence and does not certify a
clean full run. Read caches are temporary so repeated INDEX revisions do not fill disk.

SHIFT.json is no longer used. Use a distinct run ID per independent run and never resume a run concurrently. Different runtimes do not share an arXiv limiter or Hugging Face commit budget; concurrent crawling is not yet coordinated.

## Resume after runtime loss

Confirm the old runtime stopped, download/copy the same snapshot again, keep repo ID,
RUN_ID and all dataset settings unchanged; set RESUME=True and TAKEOVER=True if an active
marker remains. Setup resolves the saved implementation revision from HF. Local folders
are new for each runtime and existing local runs are never merged or deleted. Do not
change a saved Drive run into HF mode: start a new HF run instead.

## Measurement scope and validation

Session wall includes corpus restore, crawl and final corpus sync. Snapshot index costs
are included in discovery time. Snapshot bytes, mirrored checkpoint staging, Hub read
cache and configured Xet cache are now included in disk sampling. Environment packages
and remote storage quota are excluded. Final evidence publication/download and initial
setup are outside session measurement. Checkpoint duration includes data upload and
completion-pointer publication; the independent timing-record upload follows the timed
boundary. Do not add checkpoint durations again to session wall.

Offline tests cover versioned roundtrip, upload interruption before and during commits,
corrupt source refusal, empty versions, telemetry restore, resume and SDK argument
compatibility. A live private-HF smoke with the user's Colab Secret is still required.
No live Dataset writes were performed while implementing this backend.


## HF commit-rate protection

HF can reject repository commit creation independently of normal API request quotas.
Our observed server response specified 128 commits/hour; this is not a permanent
published guarantee. HF telemetry now batches resource and paper segments in one
commit per flush, with a 120-second HF flush target (Drive remains 30 seconds).
RAM sampling still targets one second and disk sampling 30 seconds. Runtime loss
can lose up to the unpublished telemetry tail; upload/quota waits can extend it.

A file-locked budget shared by parent and child permits at most 90 commit attempts
per rolling hour in the same staging root. Raw files stay unpacked and each commit
targets at most 500 files or 32 MiB of input. A single file larger than the byte
   target is sent alone; raw files are never split or archived. The budget waits rather than exceeding its local cap.
It cannot account for earlier sessions or other tools/runtimes sharing server quota.
On HTTP 429, all local writers share a cooldown using Retry-After/RateLimit headers,
with a conservative one-hour fallback when neither is usable; retries are bounded
at three attempts per batch. Waiting is logged, keeps local data and contributes to
elapsed benchmark time when inside the session. Sync may exceed checkpoint/session
budget targets while waiting. Existing completed LATEST pointers remain authoritative.

If finalization fails, export_rescue(LOCAL_ROOT / RUN_ID) downloads raw local data,
logs and measurements without contacting HF. Preserve that ZIP before resetting.
Do not label failed finalization as a clean benchmark. The notebook's RESUME option
loads the saved implementation revision, so resuming an older manifest does not
automatically apply this fix; use a new run ID for a clean test of the new revision.


### Larger batches for the group's small source files

The 500-file / 32-MiB target is a workload-specific experiment, above HF's usual
50–100-file HTTP recommendation. HF documents a 60-second server validation timeout;
bandwidth alone does not guarantee that larger commits succeed. No private-HF live
validation was performed for this revision. HubStore accepts max_commit_files and
max_commit_bytes overrides for smaller batches if the next smoke exposes timeouts.
Byte sizing uses stat for source Paths and does not load all raw sources into RAM.

Unchanged MANIFEST, session/checkpoint sidecars and book CSVs are skipped via a local
hash cache. Cache state is advanced only after successful publication; deleting/loss
of the cache causes harmless re-upload, not skipped data. HF also omits the duplicate
final-reports checkpoint: the final checkpoint in cleanup publishes these reports
before release, and final evidence publication is still retained. LATEST remains a
separate commit after pinned INDEX read-back; its consistency contract is unchanged.

The notebook's chunk size now defaults to 500 IDs. COUNT=100 still selects exactly
100 sample IDs in one chunk. On a full crawl, larger chunks reduce prelaunch and
post-chunk checkpoint frequency; 300-second periodic checkpoints are unchanged.
A timed-out upload still leaves the previous completed pointer usable. Start a new
run ID/revision for measurement; changing chunk_size is incompatible with old resume.
