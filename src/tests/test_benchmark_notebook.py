"""The notebook is a thin, non-destructive, version-pinned Colab entrypoint."""

import ast
import json
from pathlib import Path
from types import SimpleNamespace

import pytest


def test_single_notebook_has_safe_smoke_defaults_and_no_outputs():
    folder = Path(__file__).parents[1] / "notebooks"
    path = folder / "arxiv_nhom1_crawl_benchmark.ipynb"
    notebook = json.loads(path.read_text(encoding="utf-8"))
    assert not (folder / "colab_crawl.ipynb").exists()
    assert not (folder / "colab_benchmark.ipynb").exists()
    source = "\n".join("".join(c["source"]) for c in notebook["cells"])
    assert 'MODE = "sample"' in source
    assert "COUNT = 100" in source
    assert "RESUME = False" in source
    assert "SESSION_BUDGET_S = 4 * 60 * 60" in source
    assert "run_benchmark(" in source and "prepare_drive(" in source
    assert "rm -rf" not in source
    assert "shutil.rmtree" not in source
    for cell in notebook["cells"]:
        if cell["cell_type"] == "code":
            assert not cell.get("outputs") and cell.get("execution_count") is None
            ast.parse("".join(cell["source"]))


def test_notebook_refuses_cached_pipeline_modules_before_setup():
    path = Path(__file__).parents[1] / "notebooks/arxiv_nhom1_crawl_benchmark.ipynb"
    cells = json.loads(path.read_text(encoding="utf-8"))["cells"]
    guards = [(i, c) for i, c in enumerate(cells) if c.get("id") == "fresh-kernel-guard"]
    assert len(guards) == 1
    index, guard = guards[0]
    setup = next(i for i, c in enumerate(cells) if "drive.mount(" in "".join(c["source"]))
    assert index < setup
    tree = ast.parse("".join(guard["source"]))
    checks = ast.Module(
        body=[n for n in tree.body if not isinstance(n, ast.Import)], type_ignores=[]
    )
    code = compile(ast.fix_missing_locations(checks), "kernel_guard", "exec")
    exec(code, {"sys": SimpleNamespace(modules={"pandas": object()})})
    with pytest.raises(RuntimeError, match="Restart runtime"):
        exec(code, {"sys": SimpleNamespace(modules={"src.benchmark.runner": object()})})


def test_hf_notebook_defaults_to_private_dataset_smoke_without_embedded_token():
    path = Path(__file__).parents[1] / "notebooks/arxiv_nhom1_crawl_benchmark.ipynb"
    nb = json.loads(path.read_text(encoding="utf-8"))
    config = next(c for c in nb["cells"] if "MODE = " in "".join(c["source"]))
    values = {}
    exec("".join(config["source"]), values)
    assert values["STORAGE_BACKEND"] == "huggingface"
    assert values["HF_REPO_ID"] == "PancakeDang/arxiv-nhom1-introds"
    assert values["MODE"] == "sample" and values["COUNT"] == 100
    assert values["SNAPSHOT_PATH"] == "/content/arxiv-metadata-oai-snapshot.json"
    assert "HF_TOKEN" not in values


@pytest.mark.parametrize("state", ["download", "existing", "interrupted"])
def test_snapshot_cell_publishes_only_finished_download_before_storage(
    tmp_path, monkeypatch, state
):
    import subprocess
    import sys

    path = Path(__file__).parents[1] / "notebooks/arxiv_nhom1_crawl_benchmark.ipynb"
    cells = json.loads(path.read_text(encoding="utf-8"))["cells"]
    matches = [(i, c) for i, c in enumerate(cells) if c.get("id") == "download-snapshot"]
    assert len(matches) == 1, "Missing snapshot download cell"
    index, cell = matches[0]
    storage = next(i for i, c in enumerate(cells) if "drive.mount(" in "".join(c["source"]))
    assert index < storage
    target = tmp_path / "snapshot.json"
    record = b'{"id":"2604.20535","versions":[{"version":"v1"}]}\n'
    if state == "existing":
        target.write_bytes(record)

    def download(handle, *, path, output_dir):
        assert handle == "Cornell-University/arxiv/versions/123"
        assert path == "arxiv-metadata-oai-snapshot.json"
        downloaded = Path(output_dir) / path
        downloaded.write_bytes(record[:10] if state == "interrupted" else record)
        if state == "interrupted":
            raise ConnectionError("interrupted")
        return str(downloaded)

    def install(*args, **kwargs):
        if state == "existing":
            pytest.fail("Existing input should not require install/network")

    monkeypatch.setattr(subprocess, "run", install)
    monkeypatch.setitem(sys.modules, "kagglehub", SimpleNamespace(dataset_download=download))
    values = {
        "SNAPSHOT_PATH": str(target),
        "KAGGLE_DATASET": "Cornell-University/arxiv/versions/123",
    }
    if state == "interrupted":
        with pytest.raises(ConnectionError):
            exec("".join(cell["source"]), values)
        assert not target.exists()
    else:
        exec("".join(cell["source"]), values)
        assert target.read_bytes() == record
