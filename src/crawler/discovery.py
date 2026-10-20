"""Entry discovery and paper metadata.

For a list of IDs: find every paper, how many versions it has, and the submission date
of each version, then write ``<data_root>/<yymm-id>/metadata.json``::

    {"arxiv_id", "title", "authors": [...], "submission_date", "revised_dates": [...],
     "venue", "num_versions", "categories": [...], "doi"}

Dates are ISO 8601 UTC strings (``2026-06-30T17:59:57Z``); ``venue`` is the journal-ref
(nullable). Sources, all through the shared arXiv limiter:

1. arXiv API ``id_list`` with 100 base IDs per request: title, authors, latest version
   (suffix of the entry id), ``published`` = v1 date, ``updated`` = latest version date.
   That is enough for papers with 1 or 2 versions.
2. For 3+ versions, the dates of v2..v(N-1): the Kaggle snapshot when given, otherwise
   the API again with versioned IDs (``2606.30762v2``: ``updated`` is that version's
   date; checked against OAI-PMH on 2026-10-18).
3. OAI-PMH ``GetRecord`` (``arXivRaw``) as the fallback for IDs the API did not return.
"""

from __future__ import annotations

import json
import logging
import os
import re
import time
from collections.abc import Callable, Iterable
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
from pathlib import Path

from lxml import etree

from src import config
from src.crawler.http_client import FetchError, HttpClient
from src.utils.ids import split_version, to_folder_name

log = logging.getLogger(__name__)

ATOM = "{http://www.w3.org/2005/Atom}"
ARXIV = "{http://arxiv.org/schemas/atom}"
OAI = "{http://www.openarchives.org/OAI/2.0/}"
ARXIV_RAW = "{http://arxiv.org/OAI/arXivRaw/}"

_XML_PARSER = etree.XMLParser(resolve_entities=False, no_network=True)


@dataclass
class PaperMetadata:
    arxiv_id: str
    title: str
    authors: list[str]
    submission_date: str
    revised_dates: list[str]
    venue: str | None
    num_versions: int
    categories: list[str] = field(default_factory=list)
    doi: str | None = None


@dataclass
class ApiEntry:
    arxiv_id: str  # base ID, no version suffix
    version: int  # version described by this entry (the latest one for base queries)
    title: str
    authors: list[str]
    published: str  # v1 submission date
    updated: str  # submission date of ``version``
    journal_ref: str | None
    doi: str | None
    categories: list[str]


@dataclass
class DiscoveryResult:
    found: dict[str, int] = field(default_factory=dict)  # arxiv_id -> num_versions
    missing: dict[str, str] = field(default_factory=dict)  # arxiv_id -> reason
    elapsed_s: float = 0.0


# --------------------------------------------------------------------------- parsing


def _clean(text: str | None) -> str | None:
    if text is None:
        return None
    text = " ".join(text.split())
    return text or None


def iso_utc(value: str) -> str:
    """Atom (``2026-06-30T17:59:57Z``) or RFC 2822 (``Tue, 30 Jun 2026 17:59:57 GMT``) -> ISO."""
    value = value.strip()
    try:
        dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        dt = parsedate_to_datetime(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def parse_api_feed(xml: bytes) -> list[ApiEntry]:
    root = etree.fromstring(xml, parser=_XML_PARSER)
    entries = []
    for entry in root.iter(f"{ATOM}entry"):
        id_url = entry.findtext(f"{ATOM}id") or ""
        if "/abs/" not in id_url:  # error entries use http://arxiv.org/api/errors#...
            log.warning("arXiv API error entry: %s", _clean(entry.findtext(f"{ATOM}summary")))
            continue
        base, version = split_version(id_url.split("/abs/", 1)[1])
        entries.append(
            ApiEntry(
                arxiv_id=base,
                version=version or 1,
                title=_clean(entry.findtext(f"{ATOM}title")) or "",
                authors=[
                    name
                    for a in entry.iter(f"{ATOM}author")
                    if (name := _clean(a.findtext(f"{ATOM}name")))
                ],
                published=iso_utc(entry.findtext(f"{ATOM}published")),
                updated=iso_utc(entry.findtext(f"{ATOM}updated")),
                journal_ref=_clean(entry.findtext(f"{ARXIV}journal_ref")),
                doi=_clean(entry.findtext(f"{ARXIV}doi")),
                categories=[c.get("term") for c in entry.iter(f"{ATOM}category") if c.get("term")],
            )
        )
    return entries


def _split_authors(raw: str) -> list[str]:
    """arXivRaw / snapshot author string -> list ("A, B and C")."""
    parts = re.split(r",\s*|\s+and\s+", " ".join(raw.split()))
    return [p for p in (s.strip() for s in parts) if p]


def parse_oai_record(xml: bytes) -> dict | None:
    """Parse an OAI-PMH GetRecord response in ``arXivRaw``; None if the ID does not exist."""
    root = etree.fromstring(xml, parser=_XML_PARSER)
    error = root.find(f"{OAI}error")
    if error is not None:
        if error.get("code") == "idDoesNotExist":
            return None
        raise ValueError(f"OAI-PMH error {error.get('code')}: {_clean(error.text)}")
    raw = root.find(f".//{ARXIV_RAW}arXivRaw")
    if raw is None:
        raise ValueError("OAI-PMH response without an arXivRaw record")
    versions = {}
    for v in raw.iter(f"{ARXIV_RAW}version"):
        n = int(v.get("version", "v0").lstrip("v"))
        versions[n] = iso_utc(v.findtext(f"{ARXIV_RAW}date"))
    return {
        "arxiv_id": _clean(raw.findtext(f"{ARXIV_RAW}id")),
        "title": _clean(raw.findtext(f"{ARXIV_RAW}title")) or "",
        "authors": _split_authors(raw.findtext(f"{ARXIV_RAW}authors") or ""),
        "versions": versions,
        "journal_ref": _clean(raw.findtext(f"{ARXIV_RAW}journal-ref")),
        "doi": _clean(raw.findtext(f"{ARXIV_RAW}doi")),
        "categories": (raw.findtext(f"{ARXIV_RAW}categories") or "").split(),
    }


def parse_snapshot_line(line: str) -> dict:
    """One line of the Kaggle ``arxiv-metadata-oai-snapshot.json``."""
    rec = json.loads(line)
    versions = {int(v["version"].lstrip("v")): iso_utc(v["created"]) for v in rec["versions"]}
    authors = []
    for parts in rec.get("authors_parsed") or []:
        last, first, suffix = (list(parts) + ["", "", ""])[:3]  # [last, first, suffix]
        if name := " ".join(p for p in (first, last, suffix) if p):
            authors.append(name)
    authors = authors or _split_authors(rec.get("authors") or "")
    return {
        "arxiv_id": rec["id"],
        "title": _clean(rec.get("title")) or "",
        "authors": authors,
        "versions": versions,
        "journal_ref": _clean(rec.get("journal-ref")),
        "doi": _clean(rec.get("doi")),
        "categories": (rec.get("categories") or "").split(),
    }


def load_snapshot(path: Path, wanted: Iterable[str]) -> dict[str, dict]:
    """Stream the Kaggle snapshot (several GB) line by line, keeping only ``wanted`` IDs."""
    wanted = set(wanted)
    found = {}
    prefix = '{"id":"'
    with open(path, encoding="utf-8") as fh:
        for line in fh:
            if not line.startswith(prefix):
                continue
            arxiv_id = line[len(prefix) : line.index('"', len(prefix))]
            if arxiv_id in wanted:
                found[arxiv_id] = parse_snapshot_line(line)
                if len(found) == len(wanted):
                    break
    return found


# --------------------------------------------------------------------------- network


def api_query(client: HttpClient, ids: list[str]) -> list[ApiEntry]:
    """One arXiv API call for up to ``config.ARXIV_API_BATCH`` (base or versioned) IDs."""
    url = f"{config.ARXIV_API_URL}?id_list={','.join(ids)}&max_results={len(ids)}"
    res = client.get(url)
    if res.status != 200:
        raise FetchError(url, f"HTTP {res.status}", res.status, res.attempts)
    return parse_api_feed(res.content)


def oai_get_record(client: HttpClient, arxiv_id: str) -> dict | None:
    url = (
        f"{config.ARXIV_OAI_URL}?verb=GetRecord"
        f"&identifier=oai:arXiv.org:{arxiv_id}&metadataPrefix=arXivRaw"
    )
    res = client.get(url)
    if res.status != 200:
        raise FetchError(url, f"HTTP {res.status}", res.status, res.attempts)
    return parse_oai_record(res.content)


def _batches(items: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


# --------------------------------------------------------------------------- metadata files


def metadata_path(data_root: Path, arxiv_id: str) -> Path:
    return data_root / to_folder_name(arxiv_id) / "metadata.json"


def read_metadata(data_root: Path, arxiv_id: str) -> dict | None:
    path = metadata_path(data_root, arxiv_id)
    try:
        meta = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    if meta.get("num_versions") != 1 + len(meta.get("revised_dates", [])):
        return None
    return meta


def write_metadata(data_root: Path, meta: PaperMetadata) -> Path:
    path = metadata_path(data_root, meta.arxiv_id)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(asdict(meta), ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)
    return path


# --------------------------------------------------------------------------- discovery


def discover(
    ids: list[str],
    client: HttpClient,
    data_root: Path,
    *,
    snapshot: Path | None = None,
    refresh: bool = False,
    journal: Callable[[dict], None] | None = None,
) -> DiscoveryResult:
    """Resolve ``ids``, write their metadata.json and return how many versions each has.

    IDs that already have a valid metadata.json are skipped unless ``refresh``.
    ``journal`` receives one status dict per ID (written to discovery.jsonl by the pipeline).
    """
    start = time.perf_counter()
    result = DiscoveryResult()
    note = journal or (lambda _rec: None)

    todo = []
    for arxiv_id in ids:
        meta = None if refresh else read_metadata(data_root, arxiv_id)
        if meta is not None:
            result.found[arxiv_id] = meta["num_versions"]
        else:
            todo.append(arxiv_id)
    log.info("discovery: %d IDs cached, %d to resolve", len(ids) - len(todo), len(todo))

    snap = load_snapshot(snapshot, todo) if snapshot and todo else {}
    if snapshot:
        log.info("discovery: %d/%d IDs found in the Kaggle snapshot", len(snap), len(todo))

    # 1. Base IDs through the API (one retry for IDs a batch did not return).
    entries: dict[str, ApiEntry] = {}
    for attempt in (1, 2):
        pending = [i for i in todo if i not in entries]
        for batch in _batches(pending, config.ARXIV_API_BATCH):
            try:
                entries.update((e.arxiv_id, e) for e in api_query(client, batch))
            except (FetchError, etree.XMLSyntaxError) as exc:  # e.g. feed cut mid-body
                log.error("discovery: API batch failed (%s)", exc)
        if attempt == 1 and len(entries) < len(todo):
            log.warning("discovery: API missed %d IDs, retrying once", len(todo) - len(entries))

    # 2. Dates of every version.
    dates: dict[str, dict[int, str]] = {}
    for arxiv_id, e in entries.items():
        d = {e.version: e.updated}
        d[1] = e.published  # v1-only papers: published is the authoritative v1 date
        snap_versions = snap.get(arxiv_id, {}).get("versions", {})
        for k in range(2, e.version):
            if k in snap_versions:
                d[k] = snap_versions[k]
        dates[arxiv_id] = d

    needed = [
        f"{i}v{k}" for i, e in entries.items() for k in range(2, e.version) if k not in dates[i]
    ]
    for batch in _batches(needed, config.ARXIV_API_BATCH):
        try:
            for e in api_query(client, batch):
                if e.arxiv_id in dates:
                    dates[e.arxiv_id][e.version] = e.updated
        except (FetchError, etree.XMLSyntaxError) as exc:
            log.error("discovery: versioned API batch failed (%s)", exc)

    # 3. OAI-PMH fallback: IDs the API missed and papers with incomplete version history.
    oai: dict[str, dict] = {}
    incomplete = [i for i, e in entries.items() if len(dates[i]) != e.version]
    for arxiv_id in [i for i in todo if i not in entries] + incomplete:
        try:
            rec = oai_get_record(client, arxiv_id)
        except (FetchError, ValueError, etree.XMLSyntaxError) as exc:
            result.missing[arxiv_id] = f"error: {exc}"
            continue
        if rec is None:
            result.missing[arxiv_id] = "not_found"
        else:
            oai[arxiv_id] = rec

    # 4. Write metadata.json.
    for arxiv_id in todo:
        if arxiv_id in entries:
            e = entries[arxiv_id]
            source = "api+snapshot" if arxiv_id in snap else "api"
            versions = dates[arxiv_id]
            n = e.version
            if arxiv_id in oai:
                versions = {**oai[arxiv_id]["versions"], **versions}
                n = max([n, *oai[arxiv_id]["versions"]])
                source += "+oai"
            title, authors, venue, doi, cats = (
                e.title,
                e.authors,
                e.journal_ref,
                e.doi,
                e.categories,
            )
        elif arxiv_id in oai:
            r = oai[arxiv_id]
            source, versions = "oai", r["versions"]
            title, authors, venue, doi, cats = (
                r["title"],
                r["authors"],
                r["journal_ref"],
                r["doi"],
                r["categories"],
            )
            n = max(versions) if versions else 0
        else:
            reason = result.missing.setdefault(arxiv_id, "not_found")
            note({"arxiv_id": arxiv_id, "status": "missing", "reason": reason})
            continue

        if n < 1 or sorted(versions) != list(range(1, n + 1)):
            reason = f"incomplete version history: have {sorted(versions)}, latest v{n}"
            result.missing[arxiv_id] = reason
            note({"arxiv_id": arxiv_id, "status": "missing", "reason": reason})
            continue
        meta = PaperMetadata(
            arxiv_id=arxiv_id,
            title=title,
            authors=authors,
            submission_date=versions[1],
            revised_dates=[versions[k] for k in range(2, n + 1)],
            venue=venue,
            num_versions=n,
            categories=cats,
            doi=doi,
        )
        write_metadata(data_root, meta)
        result.missing.pop(arxiv_id, None)
        result.found[arxiv_id] = n
        note({"arxiv_id": arxiv_id, "status": "found", "source": source, "num_versions": n})

    result.elapsed_s = time.perf_counter() - start
    log.info(
        "discovery: %d found, %d missing in %.1f s",
        len(result.found),
        len(result.missing),
        result.elapsed_s,
    )
    return result
