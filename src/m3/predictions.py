# Drafted with the help of an AI assistant (Gemini, Google), cited per the course rules.
"""Milestone 3 ``predictions`` table schema, validator, MRR@5 check and sharded Parquet writer.

    python -m src.m3.predictions --m2-dir M2_OUT --refs-dir m1_tables/data/references \
        --labels-dir labels --partitions partitions.csv --out-dir predictions/ \
        --ranker src.m3.model:rank_paper

Exact schema required by the Milestone 3 statement:
- ``arxiv_id`` (string, non-null)
- ``partition`` (string, dictionary: ``train``, ``valid``, or ``test``, non-null)
- ``citation_key`` (string, non-null)
- ``groundtruth_arxiv_id`` (string, nullable)
- ``cand_1`` .. ``cand_5`` (string, nullable)

Only publications present in ``partitions.csv`` (Train / Valid / Test) appear in this table,
with one row per BibTeX entry of those publications.
"""

from __future__ import annotations

import argparse
import csv
import importlib
import json
from collections import defaultdict
from collections.abc import Callable
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from src.config import RANGES
from src.m3.candidates import iter_shard_papers
from src.m3.cleaning import (
    author_surname_overlap_min,
    entry_mentions_arxiv_id,
    parse_year,
    title_char_similarity,
    title_token_jaccard,
)
from src.m3.labels import iter_label_files, range_owner, read_matrix

COMPRESSION = "zstd"
VALID_PARTITIONS = frozenset({"train", "valid", "test"})
CAND_COLS = ("cand_1", "cand_2", "cand_3", "cand_4", "cand_5")
_DICT = pa.dictionary(pa.int32(), pa.string())

PREDICTIONS_SCHEMA = pa.schema(
    [
        pa.field("arxiv_id", pa.string(), nullable=False),
        pa.field("partition", _DICT, nullable=False),
        pa.field("citation_key", pa.string(), nullable=False),
        pa.field("groundtruth_arxiv_id", pa.string(), nullable=True),
        *(pa.field(col, pa.string(), nullable=True) for col in CAND_COLS),
    ]
)

RankerFn = Callable[[list[dict], list[dict]], dict[str, list[str]]]


def load_partitions(path: Path) -> dict[str, str]:
    """Read ``partitions.csv`` (``arxiv_id,partition``) and validate partition values."""
    out: dict[str, str] = {}
    with Path(path).open(newline="", encoding="utf-8") as f:
        for i, row in enumerate(csv.DictReader(f), start=2):
            pid = (row.get("arxiv_id") or "").strip()
            part = (row.get("partition") or "").strip().lower()
            if not pid:
                raise ValueError(f"{path}:{i}: empty arxiv_id")
            if part not in VALID_PARTITIONS:
                raise ValueError(f"{path}:{i}: invalid partition {part!r}")
            if pid in out:
                raise ValueError(f"{path}:{i}: duplicate arxiv_id {pid}")
            out[pid] = part
    return out


def load_groundtruth(
    labels_dir: Path, *, arxiv_ids: set[str] | None = None
) -> tuple[dict[str, dict[str, str | None]], dict[str, str]]:
    """Load label matrices for the requested papers, or all papers by default.

    Return ``(truth[pid][key], method[pid])`` and reject overlapping sources.
    """
    truth: dict[str, dict[str, str | None]] = {}
    methods: dict[str, str] = {}
    for method, pid, path in iter_label_files(labels_dir):
        if arxiv_ids is not None and pid not in arxiv_ids:
            continue
        if pid in methods:
            raise ValueError(f"{pid} appears in both {methods[pid]} and {method} labels")
        _, answers = read_matrix(path)
        truth[pid] = answers
        methods[pid] = method
    return truth, methods


def baseline_rank_paper(entries: list[dict], candidates: list[dict]) -> dict[str, list[str]]:
    """Classical heuristic baseline ranker (used for smoke tests until the trained model lands)."""
    ranked: dict[str, list[str]] = {}
    for entry in entries:
        eyear = parse_year(entry.get("year"))
        scored = []
        for cand in candidates:
            cid = cand["ref_arxiv_id"]
            id_hit = 1.0 if entry_mentions_arxiv_id(entry, cid) else 0.0
            tsim = max(
                title_char_similarity(entry.get("title"), cand.get("ref_title")),
                title_token_jaccard(entry.get("title"), cand.get("ref_title")),
            )
            asim = author_surname_overlap_min(entry.get("authors"), cand.get("ref_authors"))
            cyear = parse_year(cand.get("ref_submission_date"))
            ybonus = 0.05 if (eyear and cyear and abs(eyear - cyear) <= 2) else 0.0
            score = 2.0 * id_hit + 0.70 * tsim + 0.25 * asim + ybonus
            scored.append((score, cid))
        scored.sort(key=lambda item: (-item[0], item[1]))
        ranked[entry["citation_key"]] = [cid for _, cid in scored[:5]]
    return ranked


def resolve_ranker(spec: str | None) -> RankerFn:
    """Import a pluggable ranker ``module.path:callable_name`` or return the baseline ranker."""
    if not spec or spec == "baseline":
        return baseline_rank_paper
    if ":" not in spec:
        raise ValueError(f"ranker spec must be 'module:function', got {spec!r}")
    mod_name, fn_name = spec.split(":", 1)
    mod = importlib.import_module(mod_name)
    fn = getattr(mod, fn_name)
    if not callable(fn):
        raise TypeError(f"{spec} is not callable")
    return fn


def validate_prediction_rows(
    rows: list[dict],
    *,
    allowed_partitions: dict[str, str] | None = None,
    candidates_by_paper: dict[str, set[str]] | None = None,
) -> None:
    """Validate Milestone 3 schema and integrity constraints on prediction rows."""
    seen: set[tuple[str, str]] = set()
    for i, row in enumerate(rows):
        pid = row.get("arxiv_id")
        part = row.get("partition")
        key = row.get("citation_key")
        if not pid or not isinstance(pid, str):
            raise ValueError(f"row {i}: non-null string arxiv_id required")
        if part not in VALID_PARTITIONS:
            raise ValueError(f"row {i}: invalid partition {part!r}")
        if not key or not isinstance(key, str):
            raise ValueError(f"row {i}: non-null string citation_key required")
        if allowed_partitions is not None:
            expected_part = allowed_partitions.get(pid)
            if expected_part is None:
                raise ValueError(f"row {i}: paper {pid} is not in Train/Valid/Test")
            if expected_part != part:
                raise ValueError(f"row {i}: paper {pid} partition {part!r} != {expected_part!r}")
        pair = (pid, key)
        if pair in seen:
            raise ValueError(f"row {i}: duplicate (arxiv_id, citation_key) {pair}")
        seen.add(pair)

        cands = [row.get(col) for col in CAND_COLS]
        saw_null = False
        seen_cands: set[str] = set()
        valid_cands = candidates_by_paper.get(pid) if candidates_by_paper is not None else None
        for col, cid in zip(CAND_COLS, cands, strict=True):
            if cid is None:
                saw_null = True
                continue
            if saw_null:
                raise ValueError(f"row {i} ({pid}/{key}): {col} is non-null after a null slot")
            if cid in seen_cands:
                raise ValueError(f"row {i} ({pid}/{key}): duplicate candidate {cid} in top-5")
            seen_cands.add(cid)
            if valid_cands is not None and cid not in valid_cands:
                raise ValueError(f"row {i} ({pid}/{key}): {col}={cid} not in paper candidates")

        gt = row.get("groundtruth_arxiv_id")
        if gt is not None and valid_cands is not None and gt not in valid_cands:
            raise ValueError(f"row {i} ({pid}/{key}): groundtruth {gt} not in paper candidates")


def to_predictions_table(rows: list[dict]) -> pa.Table:
    """Build an Arrow table conforming strictly to ``PREDICTIONS_SCHEMA``."""
    validate_prediction_rows(rows)
    columns = []
    for field in PREDICTIONS_SCHEMA:
        values = [row.get(field.name) for row in rows]
        if pa.types.is_dictionary(field.type):
            columns.append(pa.array(values, type=pa.string()).dictionary_encode())
        else:
            columns.append(pa.array(values, type=field.type))
    return pa.Table.from_arrays(columns, schema=PREDICTIONS_SCHEMA)


def compute_mrr_at_5(rows: list[dict], methods: dict[str, str] | None = None) -> dict:
    """Compute MRR@5 overall and grouped by partition (and label method when supplied).

    Reports both ``mrr5_matched_only`` (entries with a non-null ``groundtruth_arxiv_id``) and
    ``mrr5_all_entries`` (counting all entries in ``|Q|``, where no-match rows score 0).
    """

    def _agg(subset: list[dict]) -> dict:
        n_all = len(subset)
        rr_sum = 0.0
        n_matched = 0
        hits_at_1 = 0
        hits_at_5 = 0
        for row in subset:
            gt = row.get("groundtruth_arxiv_id")
            if gt is None:
                continue
            n_matched += 1
            cands = [row.get(col) for col in CAND_COLS]
            if gt in cands:
                rank = cands.index(gt) + 1
                rr_sum += 1.0 / rank
                hits_at_5 += 1
                if rank == 1:
                    hits_at_1 += 1
        return {
            "n_entries": n_all,
            "n_with_groundtruth": n_matched,
            "mrr5_matched_only": round(rr_sum / n_matched, 4) if n_matched else None,
            "mrr5_all_entries": round(rr_sum / n_all, 4) if n_all else None,
            "recall_at_1": round(hits_at_1 / n_matched, 4) if n_matched else None,
            "recall_at_5": round(hits_at_5 / n_matched, 4) if n_matched else None,
        }

    by_part: dict[str, list[dict]] = defaultdict(list)
    by_part_method: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        part = row["partition"]
        by_part[part].append(row)
        if methods and row["arxiv_id"] in methods:
            by_part_method[f"{part}_{methods[row['arxiv_id']]}"].append(row)

    out = {
        "overall": _agg(rows),
        "by_partition": {k: _agg(v) for k, v in sorted(by_part.items())},
    }
    if by_part_method:
        out["by_partition_and_method"] = {k: _agg(v) for k, v in sorted(by_part_method.items())}
    return out


def _write_prediction_shards(
    rows: list[dict],
    out_dir: Path,
    *,
    shard_rows: int = 100_000,
) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    if not rows:
        tmp = out_dir / "part-00000.parquet.tmp"
        pq.write_table(to_predictions_table([]), tmp, compression=COMPRESSION)
        tmp.replace(out_dir / "part-00000.parquet")
        return 1
    n_shards = 0
    for start in range(0, len(rows), shard_rows):
        chunk = rows[start : start + shard_rows]
        dest = out_dir / f"part-{n_shards:05d}.parquet"
        tmp = dest.with_suffix(".parquet.tmp")
        pq.write_table(to_predictions_table(chunk), tmp, compression=COMPRESSION)
        tmp.replace(dest)
        n_shards += 1
    return n_shards


def run_prediction_pipeline(
    m2_dir: Path,
    partitions_csv: Path,
    out_dir: Path,
    *,
    refs_dir: Path | None = None,
    corpus_dir: Path | None = None,
    labels_dir: Path | None = None,
    ranker: RankerFn = baseline_rank_paper,
    split_by_range: bool = False,
    use_data_prefix: bool = True,
    shard_rows: int = 100_000,
) -> dict:
    """Run ``ranker`` on every Train/Valid/Test paper and write ``predictions`` Parquet shards."""
    partitions = load_partitions(partitions_csv)
    truth_by_paper: dict[str, dict[str, str | None]] = {}
    methods: dict[str, str] = {}
    if labels_dir is not None and Path(labels_dir).is_dir():
        truth_by_paper, methods = load_groundtruth(labels_dir)

    all_rows: list[dict] = []
    cands_lookup: dict[str, set[str]] = {}
    seen_papers: set[str] = set()

    for _, pid, entries, candidates in iter_shard_papers(
        m2_dir,
        refs_dir=refs_dir,
        corpus_dir=corpus_dir,
        arxiv_ids=set(partitions),
    ):
        seen_papers.add(pid)
        part = partitions[pid]
        valid_cands = {c["ref_arxiv_id"] for c in candidates}
        cands_lookup[pid] = valid_cands
        ranked = ranker(entries, candidates) if candidates else {}
        paper_truth = truth_by_paper.get(pid, {})

        for entry in entries:
            key = entry["citation_key"]
            top5_raw = list(ranked.get(key) or [])
            # Deduplicate while preserving rank order and cap at 5.
            top5: list[str | None] = []
            used: set[str] = set()
            for cid in top5_raw:
                if cid is not None and cid not in used:
                    used.add(cid)
                    top5.append(cid)
                if len(top5) == 5:
                    break
            while len(top5) < 5:
                top5.append(None)

            all_rows.append(
                {
                    "arxiv_id": pid,
                    "partition": part,
                    "citation_key": key,
                    "groundtruth_arxiv_id": paper_truth.get(key),
                    "cand_1": top5[0],
                    "cand_2": top5[1],
                    "cand_3": top5[2],
                    "cand_4": top5[3],
                    "cand_5": top5[4],
                }
            )

    validate_prediction_rows(
        all_rows,
        allowed_partitions=partitions,
        candidates_by_paper=cands_lookup,
    )

    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    if split_by_range:
        grouped: dict[str, list[dict]] = defaultdict(list)
        for row in all_rows:
            owner = range_owner(row["arxiv_id"])
            if owner:
                grouped[owner].append(row)
        shards_by_range = {}
        for owner in sorted(RANGES):
            target_dir = (
                out_dir / owner / "data" / "predictions"
                if use_data_prefix
                else out_dir / owner / "predictions"
            )
            shards_by_range[owner] = _write_prediction_shards(
                grouped.get(owner, []), target_dir, shard_rows=shard_rows
            )
    else:
        target_dir = (
            out_dir / "data" / "predictions" if use_data_prefix else out_dir / "predictions"
        )
        shards_by_range = {
            "all": _write_prediction_shards(all_rows, target_dir, shard_rows=shard_rows)
        }

    missing_papers = sorted(set(partitions) - seen_papers)
    summary = {
        "papers_requested": len(partitions),
        "papers_written": len(seen_papers),
        "papers_missing_in_m2": len(missing_papers),
        "rows_written": len(all_rows),
        "shards": shards_by_range,
        "metrics": compute_mrr_at_5(all_rows, methods),
    }
    (out_dir / "predictions_summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return summary


def main(argv=None) -> None:
    p = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    p.add_argument("--m2-dir", type=Path, required=True)
    src = p.add_mutually_exclusive_group(required=True)
    src.add_argument("--refs-dir", type=Path)
    src.add_argument("--corpus-dir", type=Path)
    p.add_argument("--labels-dir", type=Path, help="folder with manual/ and auto/ CSV matrices")
    p.add_argument("--partitions", type=Path, required=True, help="CSV arxiv_id,partition")
    p.add_argument("--out-dir", type=Path, required=True)
    p.add_argument(
        "--ranker",
        default="baseline",
        help="pluggable ranker 'module:function' (default: classical heuristic baseline)",
    )
    p.add_argument("--split-by-range", action="store_true")
    p.add_argument("--no-data-prefix", action="store_true")
    p.add_argument("--shard-rows", type=int, default=100_000)
    a = p.parse_args(argv)

    summary = run_prediction_pipeline(
        a.m2_dir,
        a.partitions,
        a.out_dir,
        refs_dir=a.refs_dir,
        corpus_dir=a.corpus_dir,
        labels_dir=a.labels_dir,
        ranker=resolve_ranker(a.ranker),
        split_by_range=a.split_by_range,
        use_data_prefix=not a.no_data_prefix,
        shard_rows=a.shard_rows,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
