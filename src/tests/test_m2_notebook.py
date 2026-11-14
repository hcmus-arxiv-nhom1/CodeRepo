# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""The M2 Colab notebook is a thin, version-pinned, non-destructive entrypoint."""

import ast
import json
from pathlib import Path

import pytest

PATH = Path(__file__).parents[1] / "notebooks" / "arxiv_nhom1_m2_parse_benchmark.ipynb"


@pytest.fixture(scope="module")
def cells():
    return json.loads(PATH.read_text(encoding="utf-8"))["cells"]


def cell(cells, cid):
    return "".join(next(c for c in cells if c.get("id") == cid)["source"])


def test_every_code_cell_is_valid_python_and_has_no_output(cells):
    for c in cells:
        if c["cell_type"] == "code":
            ast.parse("".join(c["source"]))
            assert c["outputs"] == [] and c["execution_count"] is None


def test_configuration_cells_run_and_name_the_five_runs(cells):
    values = {}
    exec(cell(cells, "directory-config"), values)
    exec(cell(cells, "run-config"), values)
    assert set(values["RUNS"]) == {
        "only-v1-all",
        "multi-dang-0000-4999",
        "multi-hieu-0000-4999",
        "multi-thien-0000-4999",
        "multi-tien-0000-4999",
    }
    assert values["EXPECTED_IDS"] == 20000 and values["WORKERS"] == 2
    # nothing starts by accident
    assert values["RUN_M2"] is False and values["RUN_SCAN"] is False and values["RESUME"] is False


def test_the_notebook_does_the_steps_in_order(cells):
    src = "\n".join("".join(c["source"]) for c in cells)
    order = [
        "fresh-kernel-guard",
        "resolve-revision",
        "checkout-revision",
        "preflight",
        "restore-corpora",
        "benchmark",
        "verify",
        "m1-stats",
        "export",
    ]
    ids = [c.get("id") for c in cells]
    assert [ids.index(i) for i in order] == sorted(ids.index(i) for i in order)
    for needle in ("restore_m2(", "--expect-assigned", "src.benchmark.m2_benchmark",
                   "sync_to_drive(", "restore_from_drive(", "src.qa.check_tables",
                   "src.qa.audit_structure", "src.qa.audit_sample",
                   "src.analysis.stats"):  # fmt: skip
        assert needle in src, needle


def test_preflight_stops_on_an_incomplete_crawl_and_a_resume_keeps_the_revision(cells):
    preflight = cell(cells, "preflight")
    for needle in ("incomplete_chunks", "papers_source_complete", "EXPECTED_IDS", "raise"):
        assert needle in preflight
    assert 'json.loads(saved_path.read_text())["code_revision"]' in cell(cells, "resolve-revision")
    assert "RESUME needs the same" in cell(cells, "write-manifest")


def fake_drive(tmp_path, required, complete=True, chunks=()):
    runs = {}
    for name, n in required.items():
        base = tmp_path / name
        (base / "work").mkdir(parents=True)
        (base / "INDEX.json").write_text("{}")
        (base / "MANIFEST.json").write_text(json.dumps({"code_revision": "ad03e0a" + "0" * 33}))
        coverage = {
            "required_ids": n,
            "papers_source_complete": n,
            "complete": complete,
            "incomplete_chunks": list(chunks),
            "source_mode": "arxiv",
        }
        (base / "work" / "coverage.json").write_text(json.dumps(coverage))
        runs[name] = name
    return runs


def run_preflight(cells, tmp_path, required, **kwargs):
    runs = fake_drive(tmp_path, required, **kwargs)
    scope = {"DRIVE": tmp_path, "RUNS": runs, "EXPECTED_IDS": 20000, "json": json,
             "display": lambda frame: None}  # fmt: skip
    exec(cell(cells, "preflight"), scope)
    return scope


def test_preflight_passes_a_complete_crawl_of_20000_ids(cells, tmp_path):
    required = {"a": 14913, "b": 1466, "c": 1343, "d": 1164, "e": 1114}
    scope = run_preflight(cells, tmp_path, required)
    assert int(scope["preflight"]["required_ids"].sum()) == 20000


@pytest.mark.parametrize(
    "required, kwargs, message",
    [
        ({"a": 14913, "b": 1466}, {}, "expected 20000"),
        ({"a": 10000, "b": 10000}, {"complete": False}, "not complete"),
        ({"a": 10000, "b": 10000}, {"chunks": ["c1"]}, "not complete"),
    ],
)
def test_preflight_stops_on_the_wrong_ids_or_an_unfinished_crawl(
    cells, tmp_path, required, kwargs, message
):
    with pytest.raises(RuntimeError, match=message):
        run_preflight(cells, tmp_path, required, **kwargs)


def test_the_restore_is_verified_and_outside_the_benchmark(cells):
    guide = "".join(next(c for c in cells if c.get("id") == "restore-guide")["source"])
    assert "SHA-256" in guide and "not benchmarked" in guide
    assert "m2_benchmark" not in cell(cells, "restore-corpora")


def test_the_book_build_never_runs_it():
    meta = json.loads(PATH.read_text(encoding="utf-8"))["metadata"]
    assert "skip-execution" in meta["tags"]


def test_notebook_supplies_separate_bbl_input_to_parser(cells):
    directory = cell(cells, "directory-config")
    assert "BBL_RECOVERY_DIR" in directory
    scan = cell(cells, "scan")
    benchmark = cell(cells, "benchmark")
    assert '"--bbl-recovery-dir"' in scan
    assert '"--bbl-recovery-dir"' in benchmark


def test_notebook_records_interrupted_sessions_and_requires_confirmation(cells):
    config = cell(cells, "run-config")
    benchmark = cell(cells, "benchmark")
    assert "PREVIOUS_SESSION_STOPPED = False" in config
    assert "begin_session(" in benchmark
    assert "finish_session(" in benchmark
    assert "annotate_report(" in benchmark
    assert "previous_session_stopped=PREVIOUS_SESSION_STOPPED" in benchmark


def test_qa_and_export_do_not_run_until_output_exists(cells):
    for cid in ("verify", "export"):
        assert "RUN_M2" in cell(cells, cid), cid


def test_restore_checks_cached_corpus_against_pinned_checkpoint(cells):
    restore = cell(cells, "restore-corpora")
    assert "restore_m2(" in restore
    assert "expected_index_sha256" in restore
    assert "REUSE_PARTIAL_RESTORE" in restore
    assert "crawl_index_sha256" in restore
    assert "hashlib.sha256" in restore


def test_notebook_starts_in_full_mode_and_can_select_a_smoke_sample(cells):
    values = {}
    exec(cell(cells, "directory-config"), values)
    exec(cell(cells, "run-config"), values)
    assert values["RUN_MODE"] == "full"
    assert values["M2_RUN_ID"] == "m2-full-01"
    assert values["SMOKE_LIMIT"] == 1000
    assert values["SMOKE_OFFSET"] == 0
    assert '"--limit"' in cell(cells, "benchmark")
    assert '"--offset"' in cell(cells, "benchmark")
    assert '"run_mode"' in cell(cells, "write-manifest")


def test_new_runs_track_main_but_resume_uses_saved_revision(cells):
    values = {}
    exec(cell(cells, "run-config"), values)
    assert values["CODE_REF"] == "main"
    assert values["REPO"] == "https://github.com/hcmus-arxiv-nhom1/CodeRepo.git"
    assert '["code_revision"]' in cell(cells, "resolve-revision")
    manifest = cell(cells, "write-manifest")
    assert '"smoke_limit"' in manifest and '"smoke_offset"' in manifest


def test_smoke_manifest_rejects_changed_selection_on_resume(cells, tmp_path):
    bbl = tmp_path / "bbl"
    bbl.mkdir()
    (bbl / "versions.jsonl").write_text("{}\n", encoding="utf8")
    drive = tmp_path / "drive"
    (drive / "a").mkdir(parents=True)
    (drive / "a/INDEX.json").write_text("{}", encoding="utf8")
    saved = tmp_path / "M2_MANIFEST.json"
    values = {
        "Path": Path,
        "json": json,
        "BBL_RECOVERY_DIR": str(bbl),
        "RUN_MODE": "smoke",
        "SMOKE_LIMIT": 1000,
        "SMOKE_OFFSET": 0,
        "EXPECTED_IDS": 20000,
        "M2_RUN_ID": "smoke-1000",
        "REVISION": "abc",
        "RUNS": {"a": "a"},
        "DRIVE": drive,
        "WORKERS": 2,
        "SHARD_PAPERS": 100,
        "PAPER_TIMEOUT_S": 600,
        "preflight": {"revision": ["abc"]},
        "RESUME": False,
        "saved_path": saved,
    }
    exec(cell(cells, "write-manifest"), values)
    values["RESUME"] = True
    values["SMOKE_OFFSET"] = 1
    with pytest.raises(ValueError, match="smoke_offset"):
        exec(cell(cells, "write-manifest"), values)


def test_m2_restore_has_bounded_parallelism_and_explicit_partial_reuse(cells):
    values = {}
    exec(cell(cells, "run-config"), values)
    assert values["RESTORE_WORKERS"] == 8
    assert values["REUSE_PARTIAL_RESTORE"] is False
    restore = cell(cells, "restore-corpora")
    assert "RESTORE_WORKERS" in restore and "on_progress=" in restore
