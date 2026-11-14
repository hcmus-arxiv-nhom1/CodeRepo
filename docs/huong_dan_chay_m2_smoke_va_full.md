# Running the M2 smoke and full parse on Colab

This guide starts after the M2 notebook changes have been merged into CodeRepo `main`. Both runs parse the five M1 partitions and read recovered `.bbl` files from a separate Drive folder. They do not crawl sources or change the M1 corpus or its benchmark.

## Before starting

1. Open the [M2 notebook on Colab](https://colab.research.google.com/github/hcmus-arxiv-nhom1/CodeRepo/blob/main/src/notebooks/arxiv_nhom1_m2_parse_benchmark.ipynb), save your own copy if prompted, and choose a **CPU** runtime.
2. Use a Google account that can access all five M1 run folders and the complete BBL recovery `run-01` folder. Add shared folders as shortcuts in My Drive if needed, and authorize Drive when prompted.
3. In the directory configuration cell, check `DRIVE_DIR`, the five `RUNS` paths, `BBL_RECOVERY_DIR` (the folder containing `versions.jsonl`, `candidates.json`, `summary.json` and `files/`), `M2_DRIVE_DIR` and `LOCAL_ROOT`. Keep the BBL folder separate from the corpus. Make sure Colab local disk and Drive have sufficient free space for the restored corpus, output and checkpoint copies.
4. For a **new** run, keep `REPO = "https://github.com/hcmus-arxiv-nhom1/CodeRepo.git"` and `CODE_REF = "main"`. The notebook resolves `main` to a commit SHA and saves it in the run manifest. On resume it uses that saved SHA even if main has advanced. Restart the runtime before changing revisions or rerunning setup after modules have loaded.

## Smoke: 1,000 papers

To run this optional sample, change the configuration to:

```python
RUN_MODE = "smoke"
SMOKE_LIMIT = 1000
SMOKE_OFFSET = 0
M2_RUN_ID = "m2-smoke-1000-01"
RESUME = False
RUN_M2 = True
CODE_REF = "main"
```

`RUN_M2` defaults to `False` in the notebook as a safety switch; set it to `True` when ready. Pick a new `M2_RUN_ID` if that Drive output already exists. Smoke selects the first 1,000 papers in stable folder/ID order starting at offset zero; this is a reproducible operational sample, not a random or necessarily representative sample. The preflight still checks all 20,000 assigned IDs and the complete BBL recovery journal.

Run the cells in order. Mount Drive when asked. The five M1 checkpoints are restored and verified before parsing; this can take a long time even for a smoke run. The restore bar counts **files**, updating every 25 files. The default `RESTORE_WORKERS = 8` bounds concurrent Drive reads; the effective throughput depends on the Drive mount. The restore copies only the M2 corpus and `work/versions.jsonl`, not M1 telemetry, and verifies source and local SHA-256. Input verification follows; then the parser progress bar updates every **25 processed papers** and at the end. It shows committed papers already present when resuming, but paper progress can precede durable checkpoint publication. Warnings and errors remain visible.

After the benchmark finishes, review the status output, table checks and results under `M2_DRIVE_DIR/M2_RUN_ID/`. Confirm that `m2_papers.csv` contains 1,000 papers, review `m2_versions.csv` statuses and BBL/reference counts, and inspect the logs and `measurement_audit.json`. A zero process exit does not by itself prove every version parsed correctly. Smoke timing and output counts are not the official full-corpus benchmark.

An interruption can lose work since the latest successful sync. Do not run two runtimes against the same run ID.

## Resume an interrupted run

1. Confirm that the previous runtime is stopped and no other runtime is writing this run ID.
2. On a fresh CPU runtime, open the same notebook and use the **same** `M2_RUN_ID`, `RUN_MODE`, smoke limit/offset, input paths and parser settings. Set `RESUME = True`; if the prior session is still marked active, set `PREVIOUS_SESSION_STOPPED = True` only after confirming it has stopped.
3. Run cells in order. The notebook uses the saved code SHA, checks the input identities, restores M1 data and the M2 checkpoint, then continues with uncommitted papers. The `.m2-restore.json` marker identifies a complete M2-only restore; do not treat it as a full M1 checkpoint. Do not change the recovered BBL files or the M1 checkpoints during the run. If inputs or settings change, use a new run ID instead of resuming.
4. Read `measurement_audit.json`: when a runtime was lost, elapsed time and sampled peaks may be incomplete and are marked as lower bounds. An interrupted session cannot automatically provide measurements that were never synced.

## Full 20,000-paper run

The notebook defaults to full mode. Use a **new** run ID and configuration:

```python
RUN_MODE = "full"
M2_RUN_ID = "m2-full-01"
RESUME = False
RUN_M2 = True
CODE_REF = "main"
```

The smoke limit and offset are ignored in full mode. The 1,000-paper Colab smoke and interruption/resume comparison have not been completed, so the full run is the first end-to-end validation at this scale. The input check requires the exact assigned 20,000 IDs, a complete BBL candidate/journal/summary reconciliation, verified file hashes and matching paper/version mappings. It stops on inconsistencies instead of treating them as successful parsing.

Run cells in order. The progress bar counts papers, not versions; each paper may contain several versions. Resource measurements include parser-side corpus and BBL verification. Restore is separately timed setup, while periodic Drive sync competes for resources and can affect wall time. The corpus `input_size` metric excludes the separate BBL folder on Drive; report its storage separately. After completion, inspect the run summary, version and paper CSVs, table and structure checks, random-paper audit and measurement audit before publishing results or declaring M2 complete. Export files remain under the full run's Drive directory.

## Reusing an unfinished old corpus restore

The original M2 notebook refuses a non-empty local restore directory. The optimized notebook can verify and reuse existing local files: **first ensure that no older cell or other runtime is writing that directory**, preserve the current runtime and set `REUSE_PARTIAL_RESTORE = True`. Files already matching the pinned Drive index are reused; missing or mismatched files are downloaded to temporary files, checked, then moved into place. Extra corpus files and a mismatched checkpoint identity stop the operation. No original Drive data is modified.

A running old cell cannot switch code in place. Do not reset the runtime or delete `/content/m2-local` if you intend to reuse its contents. Use the updated notebook/code only after the old cell has stopped; changing code revisions within a live Python kernel needs a fresh process to avoid cached imports. A new M2 run ID is required when changing parser revision. Exact Colab throughput has not yet been benchmarked; if Drive throttles, adjust `RESTORE_WORKERS` downward and compare measured elapsed times.
## Recover an in-progress restore without discarding the runtime

The following is for the older notebook whose `Checkpoints.restore()` is still running, after the optimized code has been reviewed and merged to `main`. It keeps the existing notebook revision for the smoke parse and uses a separate Python process only for the transport. **Do not run this while the old restore cell is still active.** Stop that cell (do not disconnect or reset the runtime) and wait for execution to stop; confirm no other runtime is writing the same local folder.

Add a new cell in the existing notebook. The variables `DRIVE`, `RUNS`, `LOCAL_ROOT` and `manifest` must still exist from its earlier setup. This command clones a separate helper checkout and runs a fresh process, so it does not replace the notebook's imported Python modules. The helper checkout's exact commit is printed and saved to `/content/m2-restore-helper-revision.txt`; keep it with the smoke evidence. The `--legacy-checkpoint` option verifies all indexed files and writes a checkpoint head compatible with the original restore cell; it does not collect newer, non-indexed M1 telemetry. The smoke parser uses the old notebook's original pinned revision, not this helper revision.

```python
from pathlib import Path
import subprocess
import sys

helper = Path("/content/m2-restore-helper")
if helper.exists():
    raise FileExistsError(f"Use a new empty helper path: {helper}")
subprocess.run(
    [
        "git",
        "clone",
        "--depth",
        "1",
        "https://github.com/hcmus-arxiv-nhom1/CodeRepo.git",
        str(helper),
    ],
    check=True,
)
helper_revision = subprocess.check_output(
    ["git", "-C", str(helper), "rev-parse", "HEAD"], text=True
).strip()
print("Restore helper revision:", helper_revision)
Path("/content/m2-restore-helper-revision.txt").write_text(helper_revision + "\n")
for run_id, rel in RUNS.items():
    print(f"Recovering {run_id}", flush=True)
    subprocess.run(
        [
            sys.executable,
            "-m",
            "src.benchmark.m2_restore",
            "--remote",
            str(DRIVE / rel),
            "--local",
            str(Path(LOCAL_ROOT) / run_id),
            "--pinned-index-sha256",
            manifest["crawl_index_sha256"][run_id],
            "--workers",
            "8",
            "--reuse-partial",
            "--legacy-checkpoint",
        ],
        cwd=helper,
        check=True,
    )
```

If interrupted, leave all files intact and use a fresh helper path or the existing helper checkout in a new process. Review the JSON counts: `reused` are files whose existing local hash matched; `copied` includes files newly copied and `fallback` counts those retrieved from a preserved checkpoint object. Each existing file is checked against the original pinned index before reuse. If the index changed, an unexpected corpus file exists or no verified source is available, stop and inspect the error; do not remove the evidence automatically.

Once all five helper invocations succeed, rerun the **original** restore cell. It sees the compatible checkpoint heads, verifies the local corpus, builds `DATA_ROOTS` and `CRAWL_WORK_DIRS`, and can proceed with the original notebook revision without another corpus copy. Local verification still takes time. A full M2 run remains a separate run, as described above. Four remaining runs were restored with the helper in Colab; no full parser run has been completed yet.
