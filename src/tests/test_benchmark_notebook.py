"""The notebook is a thin, non-destructive, version-pinned Colab entrypoint."""

import ast
import json
from pathlib import Path


def test_single_notebook_has_safe_full_defaults_and_no_outputs():
    folder = Path(__file__).parents[1] / "notebooks"
    path = folder / "arxiv_nhom1_crawl_benchmark.ipynb"
    notebook = json.loads(path.read_text(encoding="utf-8"))
    assert not (folder / "colab_crawl.ipynb").exists()
    assert not (folder / "colab_benchmark.ipynb").exists()
    source = "\n".join("".join(c["source"]) for c in notebook["cells"])
    assert 'MODE = "final"' in source
    assert "COUNT = None" in source
    assert "RESUME = False" in source
    assert "SESSION_BUDGET_S = 4 * 60 * 60" in source
    assert "run_benchmark(" in source and "prepare_drive(" in source
    assert "rm -rf" not in source
    assert "shutil.rmtree" not in source
    for cell in notebook["cells"]:
        if cell["cell_type"] == "code":
            assert not cell.get("outputs") and cell.get("execution_count") is None
            ast.parse("".join(cell["source"]))
