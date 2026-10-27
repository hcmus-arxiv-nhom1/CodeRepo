# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
import csv
import json
import sys

from src.utils.profiler import dir_size, main, profile

# A parent that starts a child holding about 60 MB and writes a 2 MB output file.
CHILD = "import time; x = bytearray(60_000_000); time.sleep(0.8)"
PARENT = (
    "import subprocess, sys, pathlib; "
    "pathlib.Path(sys.argv[1], 'out.bin').write_bytes(b'0' * 2_000_000); "
    f"subprocess.run([sys.executable, '-c', {CHILD!r}], check=True)"
)


def test_profile_counts_child_processes_and_disk(tmp_path):
    out_dir, data = tmp_path / "profile", tmp_path / "data"
    data.mkdir()
    summary = profile(
        [sys.executable, "-c", PARENT, str(data)],
        out_dir,
        [data],
        interval=0.05,
        disk_every=0.1,
    )
    assert summary["exit_code"] == 0
    assert summary["rss_max_mb"] >= 60  # the grandchild's bytearray is counted
    assert summary["rss_mean_mb"] <= summary["rss_max_mb"]
    assert summary["output_mb"] == 2.0 and summary["disk_max_mb"] >= 2.0
    with open(out_dir / "profile_samples.csv", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert rows and max(int(r["n_procs"]) for r in rows) >= 2
    saved = json.loads((out_dir / "profile_summary.json").read_text(encoding="utf-8"))
    assert saved["rss_max_mb"] == summary["rss_max_mb"]


def test_cli_returns_the_child_exit_code(tmp_path):
    argv = ["--out", str(tmp_path / "p"), "--interval", "0.05", "--"]
    assert main([*argv, sys.executable, "-c", "raise SystemExit(3)"]) == 3


def test_dir_size(tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "a" / "x").write_bytes(b"12345")
    (tmp_path / "y").write_bytes(b"123")
    assert dir_size(tmp_path) == 8 and dir_size(tmp_path / "missing") == 0
