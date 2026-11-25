# Drafted with the help of an AI assistant (Gemini, Google), cited per the course rules.
"""Bounded-memory sharded candidate matrix builder for Milestone 3.

    python -m src.m3.candidates --m2-dir M2_OUT --refs-dir m1_tables/data/references \
        --out-dir candidates/

Pairs each publication's M2 BibTeX entries with its M1 arXiv-keyed candidate references,
processing one M2 shard (``shard-*/bibtex.parquet``) at a time so the full corpus never sits
in RAM. Supports reading candidates from either exported ``references/*.parquet`` shards
(fast, via PyArrow filter pushdown) or M1 ``corpus_dir/<folder>/references.json``.
"""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path

import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from src.config import RANGES
from src.m3.labels import BIBTEX_COLUMNS, load_candidates, range_owner

COMPRESSION = "zstd"

REF_COLUMNS = [
    "arxiv_id",
    "ref_arxiv_id",
    "ref_title",
    "ref_authors",
    "ref_submission_date",
]

CANDIDATE_MATRIX_SCHEMA = pa.schema(
    [
        pa.field("arxiv_id", pa.string(), nullable=False),
        pa.field("citation_key", pa.string(), nullable=False),
        pa.field("ref_arxiv_id", pa.string(), nullable=False),
    ]
)


def _open_references_dataset(refs_dir: Path) -> ds.Dataset:
    files = sorted(Path(refs_dir).glob("*.parquet"))
    if not files:
        files = sorted(Path(refs_dir).glob("**/references*.parquet"))
    if not files:
        raise FileNotFoundError(f"no Parquet shards found under {refs_dir}")
    return ds.dataset([str(f) for f in files], format="parquet")


def load_candidates_batch(
    arxiv_ids: list[str],
    *,
    refs_dataset: ds.Dataset | None = None,
    corpus_dir: Path | None = None,
) -> dict[str, list[dict]]:
    """Load deduplicated candidate reference lists for a batch of citing ``arxiv_ids``."""
    if not arxiv_ids:
        return {}
    if refs_dataset is not None:
        flt = ds.field("arxiv_id").isin(sorted(set(arxiv_ids)))
        rows = refs_dataset.to_table(columns=REF_COLUMNS, filter=flt).to_pylist()
        by_paper: dict[str, list[dict]] = defaultdict(list)
        seen: dict[str, set[str]] = defaultdict(set)
        for row in rows:
            pid = row["arxiv_id"]
            ref_id = row.get("ref_arxiv_id")
            if not ref_id or ref_id in seen[pid]:
                continue
            seen[pid].add(ref_id)
            by_paper[pid].append(
                {
                    "ref_arxiv_id": ref_id,
                    "ref_title": row.get("ref_title"),
                    "ref_authors": row.get("ref_authors") or [],
                    "ref_submission_date": row.get("ref_submission_date"),
                }
            )
        return dict(by_paper)
    if corpus_dir is not None:
        return {pid: load_candidates(corpus_dir, pid) for pid in arxiv_ids}
    raise ValueError("either refs_dataset or corpus_dir must be provided")


def iter_shard_papers(
    m2_dir: Path,
    *,
    refs_dir: Path | None = None,
    corpus_dir: Path | None = None,
    arxiv_ids: set[str] | None = None,
    only_range: str | None = None,
    columns: list[str] = BIBTEX_COLUMNS,
) -> Iterator[tuple[str, str, list[dict], list[dict]]]:
    """Yield ``(shard_name, arxiv_id, entries, candidates)`` one M2 shard at a time."""
    if refs_dir is None and corpus_dir is None:
        raise ValueError("provide either refs_dir or corpus_dir")
    shard_files = sorted(Path(m2_dir).glob("shard-*/bibtex.parquet"))
    if not shard_files:
        raise FileNotFoundError(f"no shard-*/bibtex.parquet under {m2_dir}")
    refs_dataset = _open_references_dataset(refs_dir) if refs_dir is not None else None
    filters = None if arxiv_ids is None else [("arxiv_id", "in", sorted(arxiv_ids))]

    for shard_file in shard_files:
        shard_name = shard_file.parent.name
        table = pq.read_table(shard_file, columns=columns, filters=filters)
        if table.num_rows == 0:
            continue
        by_paper: dict[str, list[dict]] = defaultdict(list)
        for row in table.to_pylist():
            pid = row["arxiv_id"]
            if only_range is not None and range_owner(pid) != only_range:
                continue
            by_paper[pid].append(row)
        if not by_paper:
            continue
        pids = sorted(by_paper)
        cands_by_paper = load_candidates_batch(
            pids, refs_dataset=refs_dataset, corpus_dir=corpus_dir
        )
        for pid in pids:
            entries = sorted(by_paper[pid], key=lambda e: e["citation_key"])
            yield shard_name, pid, entries, cands_by_paper.get(pid, [])


def build_candidate_shards(
    m2_dir: Path,
    out_dir: Path,
    *,
    refs_dir: Path | None = None,
    corpus_dir: Path | None = None,
    arxiv_ids: set[str] | None = None,
    only_range: str | None = None,
) -> dict:
    """Materialize sharded ``(arxiv_id, citation_key, ref_arxiv_id)`` Parquet candidate matrices."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    current_shard: str | None = None
    buf_arxiv: list[str] = []
    buf_key: list[str] = []
    buf_ref: list[str] = []

    stats = {
        "shards_written": 0,
        "papers_with_entries": 0,
        "papers_with_candidates": 0,
        "entries_total": 0,
        "entries_with_candidates": 0,
        "candidate_pairs_total": 0,
        "max_candidates_per_paper": 0,
    }

    def flush(shard_name: str) -> None:
        if not buf_arxiv:
            return
        table = pa.Table.from_arrays(
            [
                pa.array(buf_arxiv, type=pa.string()),
                pa.array(buf_key, type=pa.string()),
                pa.array(buf_ref, type=pa.string()),
            ],
            schema=CANDIDATE_MATRIX_SCHEMA,
        )
        dest = out_dir / f"{shard_name}.parquet"
        tmp = dest.with_suffix(".parquet.tmp")
        pq.write_table(table, tmp, compression=COMPRESSION)
        tmp.replace(dest)
        stats["shards_written"] += 1
        buf_arxiv.clear()
        buf_key.clear()
        buf_ref.clear()

    for shard_name, pid, entries, candidates in iter_shard_papers(
        m2_dir,
        refs_dir=refs_dir,
        corpus_dir=corpus_dir,
        arxiv_ids=arxiv_ids,
        only_range=only_range,
        columns=["arxiv_id", "citation_key"],
    ):
        if current_shard is not None and shard_name != current_shard:
            flush(current_shard)
        current_shard = shard_name
        stats["papers_with_entries"] += 1
        stats["entries_total"] += len(entries)
        if not candidates:
            continue
        stats["papers_with_candidates"] += 1
        stats["entries_with_candidates"] += len(entries)
        stats["max_candidates_per_paper"] = max(stats["max_candidates_per_paper"], len(candidates))
        cand_ids = [c["ref_arxiv_id"] for c in candidates]
        for entry in entries:
            key = entry["citation_key"]
            for ref_id in cand_ids:
                buf_arxiv.append(pid)
                buf_key.append(key)
                buf_ref.append(ref_id)
        stats["candidate_pairs_total"] += len(entries) * len(cand_ids)

    if current_shard is not None:
        flush(current_shard)

    (out_dir / "candidates_summary.json").write_text(
        json.dumps(stats, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return stats


def main(argv=None) -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--m2-dir", type=Path, required=True, help="folder with shard-*/bibtex.parquet")
    src_group = p.add_mutually_exclusive_group(required=True)
    src_group.add_argument(
        "--refs-dir", type=Path, help="exported references Parquet directory (recommended)"
    )
    src_group.add_argument("--corpus-dir", type=Path, help="M1 corpus holding references.json")
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument("--range", dest="only_range", choices=sorted(RANGES))
    a = p.parse_args(argv)
    stats = build_candidate_shards(
        a.m2_dir,
        a.out_dir,
        refs_dir=a.refs_dir,
        corpus_dir=a.corpus_dir,
        only_range=a.only_range,
    )
    print(json.dumps(stats, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
