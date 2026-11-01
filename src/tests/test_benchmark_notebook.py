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
    assert "SHIFT_OWNER" not in source
    assert "TAKEOVER" not in source
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
    assert values["SNAPSHOT_PATH"] == "/content/arxiv-assigned-20000.jsonl"
    assert "HF_TOKEN" not in values


@pytest.mark.parametrize("state", ["copy", "existing", "interrupted", "corrupt"])
def test_snapshot_cell_verifies_drive_input_before_storage(tmp_path, monkeypatch, state):
    import hashlib
    import shutil
    import sys

    path = Path(__file__).parents[1] / "notebooks/arxiv_nhom1_crawl_benchmark.ipynb"
    cells = json.loads(path.read_text(encoding="utf-8"))["cells"]
    index, cell = next((i, c) for i, c in enumerate(cells) if c.get("id") == "download-snapshot")
    storage = next(
        i for i, c in enumerate(cells) if 'userdata.get("HF_TOKEN")' in "".join(c["source"])
    )
    assert index < storage
    target, source = tmp_path / "snapshot.jsonl", tmp_path / "drive" / "input.jsonl"
    source.parent.mkdir()
    record = b'{"id":"2604.20535","versions":[{"version":"v1"}]}\n'
    source.write_bytes(record if state != "corrupt" else b"wrong input")
    if state == "existing":
        target.write_bytes(record)

    def mount(*args, **kwargs):
        if state == "existing":
            pytest.fail("Verified existing local input should not mount Drive")

    monkeypatch.setitem(
        sys.modules, "google.colab", SimpleNamespace(drive=SimpleNamespace(mount=mount))
    )
    if state == "interrupted":

        def interrupted(src, dst):
            Path(dst).write_bytes(record[:10])
            raise OSError("interrupted")

        monkeypatch.setattr(shutil, "copyfile", interrupted)
    values = {
        "SNAPSHOT_PATH": str(target),
        "SNAPSHOT_DRIVE_PATH": str(source),
        "SNAPSHOT_SHA256": hashlib.sha256(record).hexdigest(),
    }
    if state in ("interrupted", "corrupt"):
        with pytest.raises((OSError, ValueError)):
            exec("".join(cell["source"]), values)
        assert not target.exists()
    else:
        exec("".join(cell["source"]), values)
        assert target.read_bytes() == record
    assert source.exists(), "Never delete the Drive input"


@pytest.mark.parametrize("toggle,expected", [(True, "huggingface"), (False, "drive")])
def test_storage_toggle_selects_exactly_one_backend(toggle, expected):
    path = Path(__file__).parents[1] / "notebooks/arxiv_nhom1_crawl_benchmark.ipynb"
    cells = json.loads(path.read_text(encoding="utf-8"))["cells"]
    config = next(c for c in cells if c.get("id") == "f94c4b6e")
    source = "".join(config["source"]).replace(
        "USE_HUGGINGFACE = True", f"USE_HUGGINGFACE = {toggle}"
    )
    values = {}
    exec(source, values)
    assert values["STORAGE_BACKEND"] == expected


def test_storage_toggle_rejects_nonboolean():
    path = Path(__file__).parents[1] / "notebooks/arxiv_nhom1_crawl_benchmark.ipynb"
    cells = json.loads(path.read_text(encoding="utf-8"))["cells"]
    source = "".join(next(c for c in cells if c.get("id") == "f94c4b6e")["source"])
    with pytest.raises((TypeError, ValueError)):
        exec(source.replace("USE_HUGGINGFACE = True", 'USE_HUGGINGFACE = "False"'), {})


def test_drive_authorization_does_not_read_hf_secret(tmp_path, monkeypatch):
    import sys

    path = Path(__file__).parents[1] / "notebooks/arxiv_nhom1_crawl_benchmark.ipynb"
    cells = json.loads(path.read_text(encoding="utf-8"))["cells"]
    cell = next(c for c in cells if c.get("id") == "authorize-storage")

    def forbidden(name):
        pytest.fail(f"Drive branch must not access secret {name}")

    monkeypatch.setitem(
        sys.modules,
        "google.colab",
        SimpleNamespace(
            userdata=SimpleNamespace(get=forbidden), drive=SimpleNamespace(mount=lambda *a: None)
        ),
    )
    values = {"STORAGE_BACKEND": "drive", "DRIVE_DIR": str(tmp_path)}
    exec("".join(cell["source"]), values)
    assert values["DRIVE"] == tmp_path


def test_hf_authorization_reads_only_configured_secret(monkeypatch):
    import subprocess
    import sys

    path = Path(__file__).parents[1] / "notebooks/arxiv_nhom1_crawl_benchmark.ipynb"
    cells = json.loads(path.read_text(encoding="utf-8"))["cells"]
    cell = next(c for c in cells if c.get("id") == "authorize-storage")
    seen = []

    def get(name):
        seen.append(name)
        return "fake-token"

    class Api:
        def __init__(self, token):
            assert token == "fake-token"

        def repo_info(self, repo_id, repo_type):
            assert (repo_id, repo_type) == ("team/private", "dataset")
            return SimpleNamespace(private=True)

    monkeypatch.setitem(
        sys.modules, "google.colab", SimpleNamespace(userdata=SimpleNamespace(get=get))
    )
    monkeypatch.setitem(
        sys.modules,
        "huggingface_hub",
        SimpleNamespace(HfApi=Api, hf_hub_download=lambda *a, **k: None),
    )
    monkeypatch.setattr(subprocess, "run", lambda *a, **k: None)
    monkeypatch.delenv("HF_TOKEN", raising=False)
    monkeypatch.delenv("HF_XET_CACHE", raising=False)
    values = {"STORAGE_BACKEND": "huggingface", "HF_REPO_ID": "team/private"}
    exec("".join(cell["source"]), values)
    assert seen == ["HF_TOKEN"]
    assert "m1-hf-staging" in str(values["DRIVE"])
