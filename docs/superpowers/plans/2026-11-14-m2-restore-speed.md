# M2 restore performance and partial recovery

Goal: shorten M2 corpus preparation without changing original M1 data, journal, version coverage or parser output. The old M1 checkpoint API remains unchanged.

- Build a dedicated M2 restore that reads each remote file once while copying and hashing, with a bounded worker pool and safe fallback to content-addressed checkpoint objects.
- Restore only `data/corpus/` files and `work/versions.jsonl`; retain every version directory. Do not copy M1 benchmarking telemetry into M2 input. Bind the result to the exact remote `INDEX.json` hash and refuse unsafe paths or symlinks.
- Recover an existing partial local restore only when explicitly enabled and the previous runtime is stopped. Hash each existing indexed file before reuse; replace bad files only after a verified temporary copy is complete. Reject extra corpus files. Never delete or mutate the Drive source. Emit progress by files and separate reused, copied, and fallback counts.
- Keep local post-copy verification and the parser's corpus identity checks, so measurements include verification where currently documented. Preserve separate `.bbl` input and M1 measurement boundaries.
- Wire the notebook with explicit worker count, safe opt-in to reuse an old partial restore, and progress. Document interrupted runtime handling and isolation from existing local data. Test races, corrupted source/local files, fallback, concurrent bounded copies, unsafe paths, recovery and index changes.
- Measure local fixture and/or actual Colab smoke: compare identical scope, output hashes, file counts, wall time and worker settings; do not claim a Drive speedup before measurement. A running old Colab cell cannot pick up code changes; do not stop or reset it without clear user action.

- A separate-process legacy rescue mode may copy every indexed file and produce a compatible original checkpoint head, allowing the old notebook cell to continue in the same runtime after the old cell has stopped. This is opt-in, and does not fabricate source content or mutate Drive. The lighter notebook mode copies only M2 inputs.
