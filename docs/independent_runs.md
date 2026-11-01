# Independent runs and evidence aggregation

Choose each run's `RANGES`, `OFFSET` and `COUNT` in the notebook. Use
`MODE="partition"` for a portion of the assigned IDs, `MODE="sample"` for
trials, and a distinct `RUN_ID` per run. A partition is not a final benchmark
by itself. Each run has its own checkpoint and raw corpus tree under
`runs/<RUN_ID>/`; no `SHIFT.json` is read or written. Do not run two instances
of the same run ID concurrently, including during resume. HF reserves new run
IDs with a conditional commit; a Drive run ID must be allocated by the team.

**Parallel arXiv crawling is not yet coordinated.** The existing network rate
limiter runs inside each Colab and the HF commit budget is also local. Separate
IDs and separate run IDs do not enforce a shared limit. Run one arXiv crawler
at a time until a shared coordinator is implemented and tested. Multiple
runtimes may also hit the same server-side HF commit quota.

Extract each `bench_results.zip` to its own directory. From the CodeRepo root:

```powershell
python -m src.analysis.group_evidence --run path/to/run1 --run path/to/run2 --out path/to/new-group-report
```

The output directory must not exist and must be outside every input directory.
`group_audit.json` reports missing, extra and duplicated IDs, inconsistent code
or snapshot provenance, per-run measurement completeness and timing scope.
`papers_stats.csv` attributes each row to its run; `run_logs.jsonl` preserves
run ID and original line number. Original evidence is not modified. Coverage
is assessed using per-run QA reports and journals, not by re-reading corpus
files. Sample runs cannot certify final coverage.

`active_wall_sum_s` sums measured active session durations;
`timeline_span_s` includes time gaps and depends on the machines' clocks.
Neither is automatically the official end-to-end benchmark wall time when
runtimes overlap. Memory peaks from different machines are not combined.
`official_final_certified` remains false: confirm the grading treatment of a
distributed run before presenting it as the final single benchmark.

The notebook revision must point to a published commit of this implementation.
The development placeholder deliberately stops setup until `CODE_REF` is set to
that immutable commit. Preserve the same revision and settings when resuming.
