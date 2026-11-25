# Drafted with the help of an AI assistant (Codex, OpenAI), cited per the course rules.
"""Shared M3 pair features and the inference entry point for predictions.py."""

from __future__ import annotations

import json
import os
import re
import unicodedata
from difflib import SequenceMatcher
from functools import lru_cache
from pathlib import Path

import numpy as np

from src.m3.cleaning import (
    TITLE_STOPWORDS,
    clean_text,
    entry_mentions_arxiv_id,
    extract_author_surnames,
    parse_year,
)

FEATURE_VERSION = 2
FEATURES = (
    "title_similarity",
    "entry_title_coverage",
    "reference_title_coverage",
    "title_length_ratio",
    "title_missing",
    "author_overlap",
    "author_missing",
    "year_difference",
    "year_missing",
    "explicit_arxiv_id",
)
_GREEK = {
    letter: unicodedata.name(letter).split()[-1].lower() for letter in "αβγδεζηθικλμνξοπρστυφχψω"
}
_GREEK.update(
    {"ς": "sigma", "ϑ": "theta", "ϕ": "phi", "ϵ": "epsilon", "ϰ": "kappa", "ϱ": "rho", "ϖ": "pi"}
)
_GREEK_NAMES = set(_GREEK.values()) | {"lambda"}
_GREEK_NAMES.discard("lamda")
_GREEK["λ"] = "lambda"
_MATH_COMMAND = re.compile(r"\\([a-zA-Z]+)\b")
_CHARGE = re.compile(r"\^\s*(?:\{\s*([+\-−])\s*\}|([+\-−]))|([⁺⁻])")


def _charge_token(match):
    sign = next(group for group in match.groups() if group is not None)
    return " positivecharge " if sign in "+⁺" else " negativecharge "


def _math_command(match):
    name = match.group(1).lower().removeprefix("var")
    return f" {name} " if name in _GREEK_NAMES else match.group()


@lru_cache(maxsize=8192)
def _title(value):
    text = unicodedata.normalize("NFKC", _CHARGE.sub(_charge_token, value or ""))
    text = "".join(f" {_GREEK[c.lower()]} " if c.lower() in _GREEK else c for c in text)
    normalized = clean_text(_MATH_COMMAND.sub(_math_command, text))
    tokens = normalized.split()
    content = [
        token
        for token in tokens
        if token not in TITLE_STOPWORDS and (len(token) > 1 or token.isdigit())
    ]
    return normalized, frozenset(content or tokens)


@lru_cache(maxsize=8192)
def _authors(values):
    return frozenset(extract_author_surnames(values))


def pair_features(entry, candidates):
    """Return one numeric row per candidate, in the fixed FEATURES order."""
    title, tokens = _title(entry.get("title"))
    authors = _authors(tuple(entry.get("authors") or ()))
    year = parse_year(entry.get("year"))
    rows = np.empty((len(candidates), len(FEATURES)), dtype=np.float32)
    for index, candidate in enumerate(candidates):
        other_title, other_tokens = _title(candidate.get("ref_title"))
        other_authors = _authors(tuple(candidate.get("ref_authors") or ()))
        other_year = parse_year(candidate.get("ref_submission_date"))
        title_missing = not title or not other_title
        authors_missing = not authors or not other_authors
        year_missing = year is None or other_year is None
        common = len(tokens & other_tokens)
        rows[index] = (
            0.0 if title_missing else SequenceMatcher(None, title, other_title).ratio(),
            common / max(1, len(tokens)),
            common / max(1, len(other_tokens)),
            min(len(title), len(other_title)) / max(1, len(title), len(other_title)),
            title_missing,
            0.0 if authors_missing else len(authors & other_authors) / len(authors | other_authors),
            authors_missing,
            0 if year_missing else abs(year - other_year),
            year_missing,
            entry_mentions_arxiv_id(entry, candidate.get("ref_arxiv_id")),
        )
    return rows


def rank_indices(scores, candidate_ids):
    return sorted(range(len(scores)), key=lambda i: (-scores[i], candidate_ids[i]))


@lru_cache(maxsize=4)
def _load_artifact(path, mtime_ns):
    artifact = json.loads(path.read_text(encoding="utf-8"))
    coefficients = np.asarray(artifact["coefficients"], dtype=np.float64)
    intercept = float(artifact["intercept"])
    threshold = float(artifact["threshold"])
    if (
        artifact["feature_version"] != FEATURE_VERSION
        or tuple(artifact["features"]) != FEATURES
        or coefficients.shape != (len(FEATURES),)
        or not np.isfinite(coefficients).all()
        or not np.isfinite([intercept, threshold]).all()
    ):
        raise ValueError(f"Invalid M3 model artifact: {path}")
    return coefficients, intercept, threshold


def rank_paper(entries: list[dict], candidates: list[dict]) -> dict[str, list[str]]:
    """Return up to five candidate arXiv IDs per citation, or [] on abstention."""
    if not entries or not candidates:
        return {entry["citation_key"]: [] for entry in entries}
    artifact_path = os.environ.get("M3_MODEL_ARTIFACT")
    if not artifact_path:
        raise ValueError("Set M3_MODEL_ARTIFACT to the trained model.json path")
    path = Path(artifact_path).resolve()
    coefficients, intercept, threshold = _load_artifact(path, path.stat().st_mtime_ns)
    candidate_ids = [candidate["ref_arxiv_id"] for candidate in candidates]
    ranked = {}
    for entry in entries:
        scores = pair_features(entry, candidates) @ coefficients + intercept
        order = rank_indices(scores, candidate_ids)
        ranked[entry["citation_key"]] = (
            [candidate_ids[i] for i in order[:5]] if scores[order[0]] >= threshold else []
        )
    return ranked
