# Hugging Face Colab smoke and handoff

Open `src/notebooks/arxiv_nhom1_crawl_benchmark.ipynb` from the published
`feat/huggingface-storage` branch. Use a fresh CPU runtime. This notebook defaults to
100 Dang IDs, not the 20,000-ID final benchmark.

1. Add the existing writable Dataset token as **HF_TOKEN** in Colab Secrets and enable
   notebook access. Keep `PancakeDang/arxiv-nhom1-introds` private. No login/token files
   are written by the notebook; the token is passed through the process environment.
2. Copy the unchanged Kaggle snapshot to `/content/arxiv-metadata-oai-snapshot.json`.
   A separate Drive mount may be used only to copy this input. HF output mode itself
   does not mount or depend on Drive.
3. Keep STORAGE_BACKEND="huggingface", HF_REPO_ID="PancakeDang/arxiv-nhom1-introds",
   MODE="sample", RANGES=("dang",), COUNT=100, CHUNK_SIZE=100, RESUME=False and TAKEOVER=False.
   Use a new RUN_ID for a new experiment. The notebook is pinned to the implementation
   commit and the repository is the published DankDaPancake fork.
4. Run setup: it checks private-Dataset read access. The first shift-claim commit checks
   write access before crawling. Neither stage prints the token. Read-only token,
   missing token, missing repo, and public repo fail before crawl work starts.
5. Run preflight and benchmark. Expect `Snapshot: 100/100 IDs indexed`; source requests
   still run through the single arXiv limiter. Periodic checkpoints remain at completed
   version boundaries (300-second target); successful chunk boundaries and final sync
   also publish checkpoints. HF calls may run concurrently; arXiv calls remain serialized.
6. Run the final report/download cells and retain bench_results.zip. Check that the HF
   run contains LATEST.json, INDEX.json, MANIFEST.json, data/, work/, measurements/, and
   book_csv/ before handing over the shift in group chat.

## Remote layout and M2

Files remain raw, not corpus ZIPs:

```text
PancakeDang/arxiv-nhom1-introds/
  SHIFT.json
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

Raw deltas are committed in batches of up to 50 files, with Hub parent-commit checks.
LATEST.json is replaced only after the data INDEX.json can be read at its pinned commit.
A partially completed upload never changes the completed pointer. Restore reads all
corpus/journal files at the pointer revision and verifies hashes, then overlays newer
independent session telemetry. Failed sync keeps local evidence and does not certify a
clean full run. Read caches are temporary so repeated INDEX revisions do not fill disk.

SHIFT.json is advisory: coordinate exactly one crawler in group chat. The marker is
claimed once, checked during the run and before checkpoint publication, and released
at cleanup; it is not an atomic distributed lock. Wait for the foreground benchmark and
final report publication to finish before another operator starts.

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
corrupt source refusal, empty versions, telemetry restore, shift handoff and SDK argument
compatibility. A live private-HF smoke with the user's Colab Secret is still required.
No live Dataset writes were performed while implementing this backend.
