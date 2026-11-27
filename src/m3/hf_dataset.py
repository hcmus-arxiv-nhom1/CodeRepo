# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""Publish one member's range as a public Hugging Face dataset and check it reloads.

    HF_TOKEN=... python -m src.m3.hf_dataset --package-dir pkg/tien \
        --repo-id <hf-user>/arxiv-nhom1-tien --range tien [--labels-dir labels]

``--package-dir`` holds the six tables as sharded Parquet, one folder per table::

    papers/*.parquet  references/*.parquet  elements/*.parquet
    hierarchy/*.parquet  bibtex/*.parquet  predictions/*.parquet

The script checks the folders, writes a dataset card with YAML ``configs`` (one config per
table, no loading script) for the real ``--repo-id`` (replacing any card written earlier,
e.g. by the packager with a ``<hf-user>`` placeholder), creates the repo **public** on the
token owner's account, uploads the folder (plus the label matrices of this range under
``labels/``) and then reloads every table with ``datasets`` in streaming mode. The token is
read from ``HF_TOKEN`` (Colab Secret) and is never printed or written to disk.
"""

from __future__ import annotations

import argparse
import os
import shutil
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.config import RANGES
from src.m3.labels import iter_label_files, range_owner

TABLES = ["papers", "references", "elements", "hierarchy", "bibtex", "predictions"]

# Required columns per M1/M2/M3 statements (used by --strict-schema).
REQUIRED_COLUMNS: dict[str, tuple[str, ...]] = {
    "papers": ("arxiv_id", "title", "authors", "submission_date", "revised_dates", "venue"),
    "references": (
        "arxiv_id",
        "ref_arxiv_id",
        "ref_title",
        "ref_authors",
        "ref_submission_date",
        "ref_s2_id",
    ),
    "elements": ("element_id", "arxiv_id", "kind", "content"),
    "hierarchy": ("arxiv_id", "version", "element_id", "parent_id"),
    "bibtex": (
        "arxiv_id",
        "citation_key",
        "entry_type",
        "title",
        "authors",
        "year",
        "doi",
        "journal",
        "url",
    ),
    "predictions": (
        "arxiv_id",
        "partition",
        "citation_key",
        "groundtruth_arxiv_id",
        "cand_1",
        "cand_2",
        "cand_3",
        "cand_4",
        "cand_5",
    ),
}

DICTIONARY_COLUMNS: dict[str, tuple[str, ...]] = {
    "elements": ("kind",),
    "bibtex": ("entry_type",),
    "predictions": ("partition",),
}


def detect_table_prefix(pkg: Path) -> str:
    """Return ``'data/'`` when the M3 ``data/<table>/`` layout is present, else ``''``."""
    return "data/" if any((pkg / "data" / t).is_dir() for t in TABLES) else ""


def check_package(pkg: Path, *, strict_schema: bool = False) -> dict[str, int]:
    """Rows per table; raises if a table is missing, nested, or (when strict) off-schema."""
    prefix = detect_table_prefix(pkg)
    base = pkg / "data" if prefix else pkg
    rows = {}
    for t in TABLES:
        files = sorted((base / t).glob("*.parquet"))
        if not files:
            raise SystemExit(f"{base / t}: no Parquet shard")
        schema = pq.read_schema(files[0])
        nested = [f.name for f in schema if str(f.type).startswith(("struct", "map"))]
        if nested:
            raise SystemExit(f"{t}: nested columns {nested}; columns must be flat")
        if strict_schema:
            names = set(schema.names)
            missing = [c for c in REQUIRED_COLUMNS[t] if c not in names]
            if missing:
                raise SystemExit(f"{t}: missing required columns {missing}")
            for col in DICTIONARY_COLUMNS.get(t, ()):
                if not pa.types.is_dictionary(schema.field(col).type):
                    raise SystemExit(f"{t}.{col}: expected dictionary-encoded string column")
        rows[t] = sum(pq.ParquetFile(f).metadata.num_rows for f in files)
    return rows


def card(repo_id: str, owner: str, rows: dict[str, int], *, prefix: str = "") -> str:
    start, end = RANGES[owner]
    configs = "\n".join(
        f"- config_name: {t}\n  data_files:\n  - split: train\n    path: {prefix}{t}/*.parquet"
        for t in TABLES
    )
    counts = "\n".join(f"| `{t}` | {rows[t]:,} |" for t in TABLES)
    return f"""---
license: cc-by-4.0
pretty_name: arXiv group 1 — {start} to {end}
configs:
{configs}
---

# arXiv group 1, range `{start}` – `{end}` ({owner})

Milestone 3 output of group 1 (Intro to Data Science, HCMUS): papers crawled in M1,
parsed in M2, and the BibTeX-to-reference matching of M3. Code:
https://github.com/hcmus-arxiv-nhom1/CodeRepo

```python
from datasets import load_dataset
ds = load_dataset("{repo_id}", "predictions", split="train")
```

| Table | Rows |
| --- | ---: |
{counts}

`labels/manual/*.csv` and `labels/auto/*.csv` are the label matrices: one row per BibTeX
entry (`citation_key`), one column per candidate reference (`ref_arxiv_id`), cells `yes`/`no`,
at most one `yes` per row; a row of only `no` means no candidate is the cited paper.
"""


def stage_labels(labels_dir: Path, owner: str, pkg: Path) -> int:
    n = 0
    for method, pid, path in iter_label_files(labels_dir):
        if range_owner(pid) == owner:
            dest = pkg / "labels" / method / path.name
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, dest)
            n += 1
    return n


def verify(repo_id: str, rows: dict[str, int]) -> None:
    from datasets import load_dataset

    for t in TABLES:
        it = load_dataset(repo_id, t, split="train", streaming=True)
        first = next(iter(it), None)
        if rows[t] and first is None:
            raise SystemExit(f"{t}: reload returned no row")
        print(f"reload ok  {t:<12} columns={list(first or {})}")


def main(argv=None) -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--package-dir", type=Path, required=True)
    p.add_argument("--repo-id", required=True, help="<hf-user>/<dataset-name>")
    p.add_argument("--range", required=True, choices=sorted(RANGES))
    p.add_argument("--labels-dir", type=Path, help="copy this range's label matrices into labels/")
    p.add_argument(
        "--strict-schema",
        action="store_true",
        help="validate required column names and dictionary-encoded columns of all 6 tables",
    )
    p.add_argument("--dry-run", action="store_true", help="check and write the card, no upload")
    a = p.parse_args(argv)

    prefix = detect_table_prefix(a.package_dir)
    rows = check_package(a.package_dir, strict_schema=a.strict_schema)
    for t in TABLES:
        print(f"{t:<12} {rows[t]:>12,} rows")
    if a.labels_dir:
        print(f"labels: {stage_labels(a.labels_dir, a.range, a.package_dir)} files")
    readme = a.package_dir / "README.md"
    # always rewrite: a card left by the packager may name a placeholder repo
    readme.write_text(card(a.repo_id, a.range, rows, prefix=prefix), encoding="utf-8")
    print(f"wrote {readme}")
    if a.dry_run:
        return

    from huggingface_hub import HfApi

    token = os.environ.get("HF_TOKEN", "").strip()
    if not token:
        raise SystemExit("HF_TOKEN missing; set it as a Colab Secret, never in code")
    api = HfApi(token=token)
    api.create_repo(a.repo_id, repo_type="dataset", private=False, exist_ok=True)
    api.update_repo_settings(
        a.repo_id, repo_type="dataset", private=False
    )  # existing repo: force public
    api.upload_folder(
        repo_id=a.repo_id,
        repo_type="dataset",
        folder_path=str(a.package_dir),
        allow_patterns=["README.md", *(f"{prefix}{t}/*.parquet" for t in TABLES), "labels/**"],
        commit_message="M3: six tables, labels and dataset card",
    )
    print(f"uploaded https://huggingface.co/datasets/{a.repo_id}")
    verify(a.repo_id, rows)


if __name__ == "__main__":
    main()
