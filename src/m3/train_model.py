# Drafted with the help of an AI assistant (Codex, OpenAI), cited per the course rules.
"""Fit and select the M3 ranker on Train/Valid and write a portable artifact."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import subprocess
import time
from collections import Counter
from importlib.metadata import version
from pathlib import Path

import numpy as np
import psutil
from sklearn.linear_model import LogisticRegression
from tqdm import tqdm

from src.m3.candidates import iter_shard_papers
from src.m3.labels import label_path, title_sim
from src.m3.model import FEATURE_VERSION, FEATURES, pair_features, rank_indices
from src.m3.predictions import load_groundtruth, load_partitions
from src.utils.ids import to_folder_name


def evaluate(scores, reciprocal_ranks, matched, threshold):
    accepted = scores >= threshold
    rr = reciprocal_ranks * accepted
    true_none = ~matched
    predicted_none = ~accepted
    tp = int(np.count_nonzero(true_none & predicted_none))
    predicted = int(predicted_none.sum())
    actual = int(true_none.sum())
    return {
        "entries": len(scores),
        "matched_entries": int(matched.sum()),
        "mrr5_all_entries": float(rr.mean()) if len(rr) else None,
        "mrr5_matched_only": float(rr[matched].mean()) if matched.any() else None,
        "no_match_precision": tp / predicted if predicted else 0.0,
        "no_match_recall": tp / actual if actual else 0.0,
        "no_match_f1": 2 * tp / (predicted + actual) if predicted + actual else 0.0,
    }


def select_threshold(scores, reciprocal_ranks, matched, minimum_mrr=0.0):
    finite = scores[np.isfinite(scores)]
    thresholds = np.unique(finite)
    reject_all = float(np.nextafter(thresholds[-1], np.inf)) if len(thresholds) else 0.0
    best = None
    best_key = None
    for threshold in [*thresholds, reject_all]:
        metrics = evaluate(scores, reciprocal_ranks, matched, threshold)
        key = _selection_key(metrics, minimum_mrr)
        if best_key is None or key > best_key:
            best = {"threshold": float(threshold), **metrics, "meets_mrr_floor": key[0]}
            best_key = key
    return best


def _selection_key(metrics, minimum_mrr):
    mrr = metrics["mrr5_matched_only"] or 0.0
    meets_floor = mrr + 1e-12 >= minimum_mrr
    return meets_floor, 0.0 if meets_floor else mrr, metrics["no_match_f1"], mrr


def _evaluate_ranker(valid, coefficients=None, intercept=0.0):
    scores, rr, matched, sources = [], [], [], []
    for features, ids, correct, method, baseline in valid:
        values = baseline if coefficients is None else features @ coefficients + intercept
        order = rank_indices(values, ids)[:5]
        top = [ids[i] for i in order]
        scores.append(float(values[order[0]]) if order else -np.inf)
        rr.append(1 / (top.index(correct) + 1) if correct in top else 0.0)
        matched.append(correct is not None)
        sources.append(method)
    return np.asarray(scores), np.asarray(rr), np.asarray(matched, dtype=bool), np.asarray(sources)


def _validation_report(values, threshold):
    scores, rr, matched, sources = values
    return {
        method: evaluate(scores[mask], rr[mask], matched[mask], threshold)
        for method in ("manual", "auto")
        for mask in (sources == method,)
    }


def _write_feature_evidence(output, features, targets, sizes, methods):
    from matplotlib.backends.backend_agg import FigureCanvasAgg
    from matplotlib.figure import Figure

    sources = np.repeat(methods, sizes)
    figure = Figure(figsize=(13, 12), layout="constrained")
    FigureCanvasAgg(figure)
    axes = figure.subplots(5, 2).ravel()
    evidence = {}
    groups = {
        (method, label): (sources == method) & (targets == label)
        for method in ("manual", "auto")
        for label in (True, False)
    }
    for index, (name, axis) in enumerate(zip(FEATURES, axes, strict=True)):
        evidence[name] = {}
        distributions, labels = [], []
        for (method, label), mask in groups.items():
            values = features[mask, index]
            group = f"{method}_{'yes' if label else 'no'}"
            if not len(values):
                evidence[name][group] = {"count": 0}
                continue
            quartiles = np.quantile(values, [0.25, 0.5, 0.75]).tolist()
            evidence[name][group] = {
                "count": len(values),
                "mean": float(values.mean()),
                "quartiles": quartiles,
            }
            lower, median, upper = quartiles
            distributions.append(
                {
                    "q1": lower,
                    "med": median,
                    "q3": upper,
                    "whislo": float(values.min()),
                    "whishi": float(values.max()),
                }
            )
            labels.append(group)
        if distributions:
            axis.bxp(distributions, showfliers=False)
            axis.set_xticks(range(1, len(labels) + 1), labels)
        axis.set_title(name)
    figure.savefig(output / "train_features.png", dpi=145)
    return evidence


def _peak_memory_bytes():
    if hasattr(psutil.Process().memory_info(), "peak_wset"):
        return psutil.Process().memory_info().peak_wset
    import resource
    import sys

    scale = 1 if sys.platform == "darwin" else 1024
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * scale


def _load_examples(args, train_ids, valid_ids, truth, methods):
    feature_blocks, target_blocks, size_blocks, method_blocks = [], [], [], []
    valid = []
    audit = Counter()
    source_counts = Counter()
    selected_ids = train_ids | valid_ids
    papers = iter_shard_papers(
        args.m2_dir, refs_dir=args.refs_dir, corpus_dir=args.corpus_dir, arxiv_ids=selected_ids
    )
    seen = set()
    for _, paper, entries, candidates in tqdm(
        papers, total=len(selected_ids), desc="Build features"
    ):
        seen.add(paper)
        by_key = {entry["citation_key"]: entry for entry in entries}
        if len(by_key) != len(entries):
            raise ValueError(f"{paper}: duplicate citation_key in M2")
        candidate_ids = [candidate["ref_arxiv_id"] for candidate in candidates]
        answers = truth[paper]
        method = methods[paper]
        with label_path(args.labels_dir, method, paper).open(
            newline="", encoding="utf-8-sig"
        ) as stream:
            label_candidates = set(next(csv.reader(stream))[1:])
        current_candidates = set(candidate_ids)
        if current_candidates != label_candidates:
            audit["papers_with_candidate_drift"] += 1
            audit["missing_label_candidates"] += len(label_candidates - current_candidates)
            audit["new_unlabeled_candidates"] += len(current_candidates - label_candidates)
        training_candidates = [
            candidate for candidate in candidates if candidate["ref_arxiv_id"] in label_candidates
        ]
        training_ids = [candidate["ref_arxiv_id"] for candidate in training_candidates]
        audit["unlabeled_entries"] += len(by_key.keys() - answers.keys())
        audit["missing_bibtex"] += len(answers.keys() - by_key.keys())
        for key, correct in answers.items():
            if key not in by_key:
                continue
            if correct is not None and correct not in candidate_ids:
                raise ValueError(f"{paper}/{key}: labeled reference {correct} is absent from M1")
            entry = by_key[key]
            if paper in valid_ids:
                features = pair_features(entry, candidates)
                baseline = np.array(
                    [title_sim(entry.get("title"), c.get("ref_title")) for c in candidates]
                )
                valid.append((features, candidate_ids, correct, method, baseline))
            elif training_ids:
                # New reference rows have no human or heuristic judgment in this matrix.
                feature_blocks.append(pair_features(entry, training_candidates))
                target_blocks.append(
                    np.fromiter((cid == correct for cid in training_ids), dtype=bool)
                )
                size_blocks.append(len(training_ids))
                method_blocks.append(method)
                source_counts[method] += 1
            else:
                audit["train_no_candidates"] += 1
    if missing := selected_ids - seen:
        raise ValueError(f"{len(missing)} selected papers missing from M2, e.g. {min(missing)}")
    if not feature_blocks or not valid:
        raise ValueError("Both Train pairs and Valid citations are required")
    return (
        np.concatenate(feature_blocks),
        np.concatenate(target_blocks),
        np.asarray(size_blocks),
        np.asarray(method_blocks),
        source_counts,
        valid,
        audit,
    )


def _fit_models(features, y, sizes, methods, source_counts, valid_values, valid):
    baseline_metrics = select_threshold(*valid_values[:3])
    reports = {
        "title_sim": {
            **baseline_metrics,
            **_validation_report(valid_values, baseline_metrics["threshold"]),
        }
    }
    minimum_mrr = baseline_metrics["mrr5_matched_only"] or 0.0
    subsets = {"title": 5, "title_authors": 7, "all": len(FEATURES)}
    best = None
    best_key = None
    with tqdm(total=len(subsets) * 6, desc="Fit and validate") as progress:
        for name, width in subsets.items():
            active_features = np.ascontiguousarray(features[:, :width], dtype=np.float64)
            for balanced in (False, True):
                source_weight = (
                    np.asarray(
                        [len(sizes) / (len(source_counts) * source_counts[m]) for m in methods]
                    )
                    if balanced
                    else np.ones(len(sizes))
                )
                weights = np.repeat(source_weight / sizes, sizes)
                weights /= weights.mean()
                for c in (0.1, 1.0, 10.0):
                    model = LogisticRegression(C=c, max_iter=400)
                    model.fit(active_features, y, sample_weight=weights)
                    coefficients = np.zeros(len(FEATURES))
                    coefficients[:width] = model.coef_[0]
                    intercept = float(model.intercept_[0])
                    values = _evaluate_ranker(valid, coefficients, intercept)
                    metrics = select_threshold(*values[:3], minimum_mrr=minimum_mrr)
                    model_name = f"{name}_C{c:g}_balanced{int(balanced)}"
                    reports[model_name] = {
                        **metrics,
                        **_validation_report(values, metrics["threshold"]),
                    }
                    key = (
                        *_selection_key(metrics, minimum_mrr),
                        -width,
                        not balanced,
                        -c,
                    )
                    if best_key is None or key > best_key:
                        best = (model_name, coefficients, intercept, metrics["threshold"])
                        best_key = key
                    progress.update()
    return best, reports


def _snapshot_digest(root, paths):
    digest = hashlib.sha256()
    for path in sorted(paths):
        digest.update(path.relative_to(root).as_posix().encode() + b"\0")
        with path.open("rb") as stream:
            digest.update(hashlib.file_digest(stream, "sha256").digest())
    return digest.hexdigest()


def _input_fingerprints(args, selected_ids, methods):
    label_files = (label_path(args.labels_dir, methods[pid], pid) for pid in selected_ids)
    if args.refs_dir is not None:
        ref_root = args.refs_dir
        ref_files = list(ref_root.glob("*.parquet")) or list(ref_root.rglob("references*.parquet"))
    else:
        ref_root = args.corpus_dir
        ref_files = [ref_root / to_folder_name(pid) / "references.json" for pid in selected_ids]
        ref_files = [path for path in ref_files if path.is_file()]
    return {
        "labels": _snapshot_digest(args.labels_dir, label_files),
        "m2_bibtex": _snapshot_digest(args.m2_dir, args.m2_dir.glob("shard-*/bibtex.parquet")),
        "m1_references": _snapshot_digest(ref_root, ref_files),
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--m2-dir", type=Path, required=True)
    sources = parser.add_mutually_exclusive_group(required=True)
    sources.add_argument("--refs-dir", type=Path)
    sources.add_argument("--corpus-dir", type=Path)
    parser.add_argument("--labels-dir", type=Path, required=True)
    parser.add_argument("--partitions", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument(
        "--train-limit-per-method", type=int, help="Deterministic Train subset for smoke runs"
    )
    args = parser.parse_args(argv)
    if args.train_limit_per_method is not None and args.train_limit_per_method < 1:
        parser.error("--train-limit-per-method must be positive")

    partitions = load_partitions(args.partitions)
    train_ids = {pid for pid, part in partitions.items() if part == "train"}
    valid_ids = {pid for pid, part in partitions.items() if part == "valid"}
    truth, methods = load_groundtruth(args.labels_dir, arxiv_ids=train_ids | valid_ids)
    if missing := (train_ids | valid_ids) - truth.keys():
        raise ValueError(f"{len(missing)} Train/Valid papers missing labels, e.g. {min(missing)}")
    if args.train_limit_per_method:
        train_ids = {
            pid
            for method in ("manual", "auto")
            for pid in sorted(p for p in train_ids if methods[p] == method)[
                : args.train_limit_per_method
            ]
        }
    artifact = {
        "feature_version": FEATURE_VERSION,
        "features": FEATURES,
        "partitions_sha256": hashlib.sha256(args.partitions.read_bytes()).hexdigest(),
        "input_sha256": _input_fingerprints(args, train_ids | valid_ids, methods),
        "package_versions": {name: version(name) for name in ("numpy", "scikit-learn", "pyarrow")},
        "code_revision": subprocess.run(
            ["git", "rev-parse", "HEAD"], capture_output=True, text=True
        ).stdout.strip(),
        "source_sha256": {
            name: hashlib.sha256(Path(__file__).with_name(name).read_bytes()).hexdigest()
            for name in (
                "model.py",
                "train_model.py",
                "cleaning.py",
                "candidates.py",
                "labels.py",
                "predictions.py",
            )
        },
        "train_limit_per_method": args.train_limit_per_method,
    }
    start = time.perf_counter()
    features, targets, sizes, sources, counts, valid, audit = _load_examples(
        args, train_ids, valid_ids, truth, methods
    )
    feature_seconds = time.perf_counter() - start
    feature_peak = _peak_memory_bytes()
    best, reports = _fit_models(
        features, targets, sizes, sources, counts, _evaluate_ranker(valid), valid
    )
    name, coefficients, intercept, threshold = best
    meets_floor = reports[name]["meets_mrr_floor"]
    artifact.update(
        selected_model=name,
        coefficients=coefficients.tolist(),
        intercept=intercept,
        threshold=threshold,
        meets_baseline_mrr_floor=meets_floor,
    )
    report = {
        "selected_model": name,
        "train_papers": len(train_ids),
        "valid_papers": len(valid_ids),
        "train_pairs": len(targets),
        "train_citations_by_source": counts,
        "audit": audit,
        "models": reports,
        "feature_seconds": feature_seconds,
        "feature_peak_rss_bytes": feature_peak,
        "peak_rss_bytes": _peak_memory_bytes(),
    }
    args.out_dir.mkdir(parents=True, exist_ok=True)
    report["train_feature_evidence"] = _write_feature_evidence(
        args.out_dir, features, targets, sizes, sources
    )
    for filename, data in (("model.json", artifact), ("valid_metrics.json", report)):
        path = args.out_dir / filename
        temporary = path.with_suffix(".tmp")
        temporary.write_text(json.dumps(data, indent=2, allow_nan=False) + "\n", encoding="utf-8")
        temporary.replace(path)
    print(f"Selected {name}; wrote model.json and valid_metrics.json to {args.out_dir}")
    if not meets_floor:
        print("No supervised configuration reached the baseline Valid matched-only MRR@5.")


if __name__ == "__main__":
    main()
