# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""Measure the wall time, RAM and disk of a whole run, child processes included.

Owner: Hiếu (Member 3). Drafted by Thiên on 27/10/2026 to unblock the benchmark.

The statement asks for the maximum and average RAM, the maximum disk and the output
size, measured "in an auditable way", and for a system of several programs the total
memory of the full system. The profiler therefore wraps the command and samples the
RSS of that process plus all its descendants:

    python -m src.utils.profiler --out work/profile \\
        --watch data/corpus --watch work -- \\
        python -m src.run_pipeline --range thien --no-progress

Outputs in ``--out``:
    profile_samples.csv   t_sec, rss_total_mb, n_procs, disk_mb  (one row per sample)
    profile_summary.json  wall_s, exit_code, rss_max_mb, rss_mean_mb, disk_max_mb,
                          output_mb, cpu_count, ram_total_mb, ...

RSS is resident memory (``psutil.Process.memory_info().rss``), MB = 10^6 bytes. Disk is
the total size of the ``--watch`` folders (temporary archives included), walked every
``--disk-every`` seconds; ``output_mb`` is the final size of ``--output`` (default:
the first ``--watch``). The profiler's own memory is not counted. Run it inside
``/usr/bin/time -v`` to cross-check the peak RSS of the main process.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import platform
import shutil
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

import psutil

MB = 1_000_000


def dir_size(path: Path) -> int:
    """Total size in bytes of the regular files under ``path`` (0 if it does not exist)."""
    total = 0
    for root, _dirs, files in os.walk(path):
        for name in files:
            try:
                total += os.lstat(os.path.join(root, name)).st_size
            except OSError:  # file removed while walking (temporary archive)
                pass
    return total


def tree_rss(proc: psutil.Process) -> tuple[int, int]:
    """(RSS in bytes, number of processes) of ``proc`` and all its descendants."""
    try:
        procs = [proc, *proc.children(recursive=True)]
    except psutil.Error:
        return 0, 0
    rss, alive = 0, 0
    for p in procs:
        try:
            rss += p.memory_info().rss
            alive += 1
        except psutil.Error:  # exited between children() and memory_info()
            pass
    return rss, alive


def profile(
    cmd: list[str],
    out_dir: Path,
    watch: list[Path],
    *,
    output: Path | None = None,
    interval: float = 1.0,
    disk_every: float = 30.0,
) -> dict:
    """Run ``cmd``, sample it until it exits, write the CSV and the summary, return it."""
    out_dir.mkdir(parents=True, exist_ok=True)
    started_at = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
    t0 = time.perf_counter()
    # Resolve the program first: on Windows a relative "./.venv/Scripts/python.exe" is not
    # found by CreateProcess otherwise.
    exe = shutil.which(cmd[0]) or cmd[0]
    try:
        child = subprocess.Popen([exe, *cmd[1:]])  # noqa: S603 - the user's own command
    except FileNotFoundError as exc:
        raise SystemExit(f"command not found: {cmd[0]}") from exc
    proc = psutil.Process(child.pid)
    rss_samples: list[float] = []
    disk_mb, disk_max_mb, last_disk = 0.0, 0.0, float("-inf")
    with open(out_dir / "profile_samples.csv", "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(["t_sec", "rss_total_mb", "n_procs", "disk_mb"])
        while True:
            now = time.perf_counter() - t0
            rss, n = tree_rss(proc)
            if now - last_disk >= disk_every:
                disk_mb = sum(dir_size(p) for p in watch) / MB
                disk_max_mb, last_disk = max(disk_max_mb, disk_mb), now
            if n:
                rss_samples.append(rss / MB)
                writer.writerow([round(now, 2), round(rss / MB, 1), n, round(disk_mb, 1)])
                fh.flush()
            if child.poll() is not None:
                break
            time.sleep(interval)
    wall_s = time.perf_counter() - t0
    disk_end = sum(dir_size(p) for p in watch) / MB
    output = output or (watch[0] if watch else None)
    summary = {
        "command": cmd,
        "started_at": started_at,
        "wall_s": round(wall_s, 2),
        "exit_code": child.returncode,
        "samples": len(rss_samples),
        "interval_s": interval,
        "rss_max_mb": round(max(rss_samples), 1) if rss_samples else None,
        "rss_mean_mb": round(sum(rss_samples) / len(rss_samples), 1) if rss_samples else None,
        "disk_max_mb": round(max(disk_max_mb, disk_end), 1),
        "disk_end_mb": round(disk_end, 1),
        "output_mb": round(dir_size(output) / MB, 1) if output else None,
        "watch": [str(p) for p in watch],
        "cpu_count": os.cpu_count(),
        "ram_total_mb": round(psutil.virtual_memory().total / MB),
        "platform": platform.platform(),
        "python": platform.python_version(),
    }
    (out_dir / "profile_summary.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def main(argv: list[str] | None = None) -> int:
    argv = sys.argv[1:] if argv is None else argv
    if "--" not in argv:
        raise SystemExit("usage: python -m src.utils.profiler [options] -- <command ...>")
    split = argv.index("--")
    parser = argparse.ArgumentParser(description="Profile RAM/disk of a command and its children")
    parser.add_argument("--out", type=Path, default=Path("work/profile"))
    parser.add_argument("--watch", type=Path, action="append", default=[], help="repeatable")
    parser.add_argument("--output", type=Path, help="folder whose final size is the output size")
    parser.add_argument("--interval", type=float, default=1.0, help="RAM sampling period (s)")
    parser.add_argument("--disk-every", type=float, default=30.0, help="disk walk period (s)")
    args = parser.parse_args(argv[:split])
    cmd = argv[split + 1 :]
    if not cmd:
        raise SystemExit("no command given after --")
    summary = profile(
        cmd,
        args.out,
        args.watch,
        output=args.output,
        interval=args.interval,
        disk_every=args.disk_every,
    )
    keys = ("wall_s", "exit_code", "rss_max_mb", "rss_mean_mb", "disk_max_mb", "output_mb")
    print(json.dumps({k: summary[k] for k in keys}, indent=2), file=sys.stderr)
    return summary["exit_code"]


if __name__ == "__main__":
    raise SystemExit(main())
