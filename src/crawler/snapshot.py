"""Small, verified snapshot subset shared by all chunks of a benchmark run."""

# Drafted with assistance from OpenAI Codex, disclosed per course rules.
from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path

from src.crawler.discovery import load_snapshot


def sha256(path: Path) -> str:
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def prepare_snapshot_index(source, ids, destination, *, expected_sha256=None):
    source, destination = Path(source), Path(destination)
    started = time.perf_counter()
    fingerprint = sha256(source)
    if expected_sha256 is not None and fingerprint != expected_sha256:
        raise ValueError("Kaggle snapshot checksum differs from the saved run")
    required = sorted(set(ids))
    sidecar = destination.with_suffix(".manifest.json")
    if sidecar.is_file() and destination.is_file():
        try:
            saved = json.loads(sidecar.read_text(encoding="utf-8"))
            if (
                saved["sha256"] == fingerprint
                and saved["required_ids"] == required
                and saved["index_sha256"] == sha256(destination)
            ):
                return dict(saved, reused=True, preparation_s=time.perf_counter() - started)
        except (ValueError, KeyError, OSError):
            pass
    records = load_snapshot(source, required)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temporary = destination.with_suffix(".tmp")
    with temporary.open("w", encoding="utf-8") as output:
        for i, rec in records.items():
            # Retain the public Kaggle schema so the ordinary streaming loader can read the index.
            row = dict(
                id=i,
                title=rec["title"],
                authors_parsed=[[name, "", ""] for name in rec["authors"]],
                versions=[
                    dict(version=f"v{k}", created=date)
                    for k, date in sorted(rec["versions"].items())
                ],
                categories=" ".join(rec["categories"]),
                doi=rec["doi"],
            )
            row["journal-ref"] = rec["journal_ref"]
            output.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")
    temporary.replace(destination)
    summary = dict(
        sha256=fingerprint,
        index_sha256=sha256(destination),
        required_ids=required,
        found=len(records),
        missing_ids=sorted(set(required) - records.keys()),
        preparation_s=time.perf_counter() - started,
    )
    temporary = sidecar.with_suffix(".tmp")
    temporary.write_text(json.dumps(summary), encoding="utf-8")
    temporary.replace(sidecar)
    return dict(summary, reused=False)
