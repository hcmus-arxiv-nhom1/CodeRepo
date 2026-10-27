# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""Integrity checks for the crawled corpus (M1) -> ``qa_report.csv``.

Usage:
    python -m src.qa.validate --range tien --data-root data/corpus --work-dir work
    python -m src.qa.validate --range tien --out work/qa_report.csv
    python -m src.qa.validate --start 2606.00001 --end 2606.00200 --no-require-versions-log

Checks, per paper of the selected range (one row per problem in ``qa_report.csv``):

    missing_dir        the ``<yymm-id>/`` folder does not exist (ID crawled nowhere)
    unexpected_dir     a folder in the data root that is not an ID of the range (--scan-extra)
    metadata_missing   no metadata.json
    metadata_schema    invalid JSON, a missing / wrongly typed field, bad ISO date,
                       ``arxiv_id`` different from the folder, ``num_versions`` inconsistent
    version_missing    ``vN/`` absent for some N in 1..num_versions
    version_extra      ``vN/`` with N > num_versions, or a folder not named ``vN``
    binary_file        a file that is not .tex/.bib, a symlink, or a file containing NUL bytes
    status_missing     (needs versions.jsonl) no record for a version
    status_failed      (needs versions.jsonl) latest record is ``failed``
    status_mismatch    ``ok`` without any .tex file, or ``no_tex`` / ``unavailable`` with files
    references_schema  references.json exists but is not valid JSON
    references_missing (only with --require-references) no references.json

Severity is ``error`` for everything above, except ``tex_empty`` (a .tex file of 0 bytes),
which is a ``warning``. Exit code 1 when any error is found.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from collections.abc import Iterable, Iterator
from dataclasses import astuple, dataclass
from datetime import datetime
from pathlib import Path

from src import config
from src.utils.ids import FOLDER_RE, from_folder_name, iter_id_range, to_folder_name

KEEP = config.KEEP_EXTENSIONS
VERSION_DIR_RE = re.compile(r"^v(?P<n>[1-9]\d*)$")
FINAL_STATUSES = frozenset({"ok", "no_tex", "unavailable"})
REPORT_FIELDS = ["range_owner", "arxiv_id", "version", "check", "severity", "detail"]


@dataclass(frozen=True)
class Issue:
    range_owner: str
    arxiv_id: str
    version: int | str
    check: str
    severity: str
    detail: str


def _is_iso_utc(value: object) -> bool:
    if not isinstance(value, str):
        return False
    try:
        datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
    except ValueError:
        return False
    return True


def _is_str_list(value: object) -> bool:
    return isinstance(value, list) and all(isinstance(x, str) for x in value)


def check_metadata(meta: object, folder_id: str) -> list[str]:
    """Return human-readable schema problems of one metadata.json (empty list = valid)."""
    if not isinstance(meta, dict):
        return ["not a JSON object"]
    problems: list[str] = []
    if meta.get("arxiv_id") != folder_id:
        problems.append(f"arxiv_id {meta.get('arxiv_id')!r} != folder id {folder_id!r}")
    title = meta.get("title")
    if not isinstance(title, str) or not title.strip():
        problems.append("title missing or empty")
    authors = meta.get("authors")
    if not _is_str_list(authors) or not authors:
        problems.append("authors must be a non-empty list of strings")
    if not _is_iso_utc(meta.get("submission_date")):
        problems.append(f"submission_date not ISO UTC: {meta.get('submission_date')!r}")
    revised = meta.get("revised_dates")
    if not _is_str_list(revised) or not all(_is_iso_utc(d) for d in revised):
        problems.append("revised_dates must be a list of ISO UTC strings")
    for key in ("venue", "doi"):
        if key not in meta:
            problems.append(f"{key} missing (null is allowed)")
        elif meta[key] is not None and not isinstance(meta[key], str):
            problems.append(f"{key} must be a string or null")
    if "categories" in meta and not _is_str_list(meta["categories"]):
        problems.append("categories must be a list of strings")
    n = meta.get("num_versions")
    if not isinstance(n, int) or isinstance(n, bool) or n < 1:
        problems.append(f"num_versions must be an int >= 1, got {n!r}")
    elif _is_str_list(revised) and n != 1 + len(revised):
        problems.append(f"num_versions={n} but {len(revised)} revised_dates")
    return problems


def _scan_version_dir(vdir: Path) -> tuple[list[str], list[str], list[str]]:
    """Return (kept files, offending files, empty .tex files) under one vN/ folder."""
    kept, bad, empty_tex = [], [], []
    for path in sorted(vdir.rglob("*")):
        if path.is_dir() and not path.is_symlink():
            continue
        rel = path.relative_to(vdir).as_posix()
        if path.is_symlink() or path.suffix.lower() not in KEEP:
            bad.append(f"{rel}: not a regular .tex/.bib file")
            continue
        try:
            head = path.read_bytes()
        except OSError as exc:
            bad.append(f"{rel}: unreadable ({exc})")
            continue
        if b"\x00" in head[:8192]:
            bad.append(f"{rel}: binary content (NUL byte)")
            continue
        kept.append(rel)
        if path.suffix.lower() == ".tex" and not head.strip():
            empty_tex.append(rel)
    return kept, bad, empty_tex


def load_versions_log(path: Path) -> dict[tuple[str, int], dict]:
    """Latest record per (arxiv_id, version) from versions.jsonl (torn last line ignored)."""
    state: dict[tuple[str, int], dict] = {}
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            try:
                rec = json.loads(line)
                state[(rec["arxiv_id"], int(rec["version"]))] = rec
            except (json.JSONDecodeError, KeyError, ValueError):
                continue
    return state


def validate_paper(
    arxiv_id: str,
    data_root: Path,
    *,
    owner: str,
    versions_log: dict[tuple[str, int], dict] | None = None,
    require_references: bool = False,
) -> list[Issue]:
    """All problems of one paper folder."""
    issues: list[Issue] = []

    def add(version: int | str, check: str, detail: str, severity: str = "error") -> None:
        issues.append(Issue(owner, arxiv_id, version, check, severity, detail))

    folder = data_root / to_folder_name(arxiv_id)
    if not folder.is_dir():
        add("", "missing_dir", f"{folder.name}/ not found")
        return issues

    # metadata.json
    n_versions: int | None = None
    meta_path = folder / "metadata.json"
    if not meta_path.is_file():
        add("", "metadata_missing", "metadata.json not found")
    else:
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            add("", "metadata_schema", f"invalid JSON: {exc}")
        else:
            for problem in check_metadata(meta, arxiv_id):
                add("", "metadata_schema", problem)
            n = meta.get("num_versions") if isinstance(meta, dict) else None
            if isinstance(n, int) and not isinstance(n, bool) and n >= 1:
                n_versions = n

    # vN/ folders
    present: dict[int, Path] = {}
    for child in sorted(folder.iterdir()):
        m = VERSION_DIR_RE.match(child.name)
        if child.is_dir() and m:
            present[int(m.group("n"))] = child
        elif child.is_dir():
            add("", "version_extra", f"unexpected folder {child.name}/")
    if n_versions is not None:
        for v in range(1, n_versions + 1):
            if v not in present:
                add(v, "version_missing", f"v{v}/ not found (num_versions={n_versions})")
        for v in sorted(present):
            if v > n_versions:
                add(v, "version_extra", f"v{v}/ exists but num_versions={n_versions}")

    # file contents and status consistency
    for v, vdir in sorted(present.items()):
        kept, bad, empty_tex = _scan_version_dir(vdir)
        for entry in bad:
            add(v, "binary_file", entry)
        for rel in empty_tex:
            add(v, "tex_empty", f"{rel} is empty", "warning")
        if versions_log is None:
            continue
        rec = versions_log.get((arxiv_id, v))
        has_tex = any(f.lower().endswith(".tex") for f in kept)
        if rec is None:
            add(v, "status_missing", "no record in versions.jsonl")
        elif rec.get("status") == "failed":
            add(v, "status_failed", f"{rec.get('stage', '')}: {rec.get('reason', '')}")
        elif rec.get("status") not in FINAL_STATUSES:
            add(v, "status_mismatch", f"unknown status {rec.get('status')!r}")
        elif rec["status"] == "ok" and not has_tex:
            add(v, "status_mismatch", "status ok but no .tex file")
        elif rec["status"] in ("no_tex", "unavailable") and kept:
            add(v, "status_mismatch", f"status {rec['status']} but {len(kept)} file(s) kept")
    if versions_log is not None and n_versions is not None:
        for v in range(1, n_versions + 1):
            if v not in present and (arxiv_id, v) not in versions_log:
                add(v, "status_missing", "no record in versions.jsonl")

    # references.json (Semantic Scholar): format owned by data_contract.md, only JSON here
    ref_path = folder / "references.json"
    if ref_path.is_file():
        try:
            json.loads(ref_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            add("", "references_schema", f"invalid JSON: {exc}")
    elif require_references:
        add("", "references_missing", "references.json not found")
    return issues


def unexpected_folders(data_root: Path, expected: set[str]) -> Iterator[str]:
    """Folders in the data root that look like paper folders but are not in ``expected``."""
    if not data_root.is_dir():
        return
    for child in sorted(data_root.iterdir()):
        if child.is_dir() and FOLDER_RE.match(child.name):
            if from_folder_name(child.name) not in expected:
                yield child.name


def validate_range(
    ids: Iterable[str],
    data_root: Path,
    *,
    owner: str,
    versions_log: dict[tuple[str, int], dict] | None = None,
    require_references: bool = False,
    scan_extra: bool = False,
) -> tuple[list[Issue], int]:
    """Validate every ID; return (issues, number of IDs checked)."""
    ids = list(ids)
    issues: list[Issue] = []
    for arxiv_id in ids:
        issues.extend(
            validate_paper(
                arxiv_id,
                data_root,
                owner=owner,
                versions_log=versions_log,
                require_references=require_references,
            )
        )
    if scan_extra:
        for name in unexpected_folders(data_root, set(ids)):
            issues.append(
                Issue(owner, from_folder_name(name), "", "unexpected_dir", "error", name + "/")
            )
    return issues, len(ids)


def write_report(path: Path, issues: list[Issue]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(REPORT_FIELDS)
        writer.writerows(astuple(i) for i in issues)


# --------------------------------------------------------------------------- CLI


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    which = parser.add_mutually_exclusive_group(required=True)
    which.add_argument("--range", choices=sorted(config.RANGES), help="assigned range by owner")
    which.add_argument("--start", help="first arXiv ID (with --end)")
    parser.add_argument("--end", help="last arXiv ID (inclusive)")
    parser.add_argument("--month-end", action="append", default=[], metavar="YYMM=N")
    parser.add_argument("--offset", type=int, default=0, help="skip the first N IDs")
    parser.add_argument("--limit", type=int, help="check at most N IDs")
    parser.add_argument("--data-root", type=Path, default=Path(config.DEFAULT_DATA_ROOT))
    parser.add_argument("--work-dir", type=Path, default=Path(config.DEFAULT_WORK_DIR))
    parser.add_argument("--out", type=Path, help="default: <work-dir>/qa_report.csv")
    parser.add_argument(
        "--no-require-versions-log",
        action="store_true",
        help="do not read versions.jsonl (skip the status_* checks)",
    )
    parser.add_argument("--require-references", action="store_true")
    parser.add_argument("--scan-extra", action="store_true", help="report unknown folders too")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    month_end = dict(config.MONTH_END)
    for item in args.month_end:
        yymm, _, num = item.partition("=")
        month_end[yymm] = int(num)
    if args.range:
        start, end = config.RANGES[args.range]
        owner = args.range
    else:
        if not args.end:
            raise SystemExit("--start needs --end")
        start, end, owner = args.start, args.end, "custom"
    ids = list(iter_id_range(start, end, month_end))[args.offset :]
    if args.limit is not None:
        ids = ids[: args.limit]

    versions_log = None
    log_path = args.work_dir / "versions.jsonl"
    if not args.no_require_versions_log:
        if not log_path.is_file():
            raise SystemExit(f"{log_path} not found (use --no-require-versions-log to skip)")
        versions_log = load_versions_log(log_path)

    issues, checked = validate_range(
        ids,
        args.data_root,
        owner=owner,
        versions_log=versions_log,
        require_references=args.require_references,
        scan_extra=args.scan_extra,
    )
    out = args.out or args.work_dir / "qa_report.csv"
    write_report(out, issues)

    errors = [i for i in issues if i.severity == "error"]
    by_check = Counter(i.check for i in issues)
    papers_bad = len({i.arxiv_id for i in errors})
    print(
        json.dumps(
            {
                "range": owner,
                "ids_checked": checked,
                "papers_with_errors": papers_bad,
                "errors": len(errors),
                "warnings": len(issues) - len(errors),
                "by_check": dict(sorted(by_check.items())),
                "report": str(out),
            },
            indent=2,
        )
    )
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
