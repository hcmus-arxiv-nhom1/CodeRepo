# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""Reference metadata from Semantic Scholar (M1), see specs/0001-references.md.

Owner: Đăng (Member 4). Drafted by Thiên on 27/10/2026 to unblock the official crawl.

``run_pipeline`` calls ``crawl(arxiv_ids, data_root)`` in a background thread, so it
runs while the arXiv lane waits on its 3-second pace; it can also run on its own:

    python -m src.crawler.references --data-root data/corpus          # every crawled paper
    python -m src.crawler.references --data-root data/corpus --ids 2606.30762,2607.00001

One ``POST /paper/batch`` call returns up to ``BATCH_SIZE`` papers with their nested
references (checked on 27/10/2026: 218 references for 2606.30762, ``null`` for an unknown
ID), so 20,000 papers need about 200 requests instead of 20,000. A paper whose nested
list reaches ``NESTED_REFERENCE_CAP`` is completed through the paginated
``/paper/{id}/references`` endpoint. Every reference is kept, with or without an arXiv
ID ("Keep all crawled references locally"); the ``references`` table of M3 filters later.

Each paper gets ``<data_root>/<yymm-id>/references.json`` with ``status`` ``ok`` or
``not_found``; papers already written are skipped, papers whose batch failed are left
without a file and retried on the next run.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import random
import time
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from pathlib import Path

import requests

from src import config
from src.crawler.ratelimit import RateLimiter, semantic_scholar_limiter
from src.utils.ids import to_folder_name

log = logging.getLogger(__name__)

BATCH_URL = config.SEMANTIC_SCHOLAR_API + "/paper/batch"
REFERENCES_URL = config.SEMANTIC_SCHOLAR_API + "/paper/{paper_id}/references"
REF_FIELDS = ("title", "authors", "year", "publicationDate", "venue", "externalIds")
BATCH_FIELDS = ",".join(["externalIds", *(f"references.{f}" for f in REF_FIELDS)])
BATCH_SIZE = 100  # the API accepts up to 500 IDs; 100 keeps one retry cheap
NESTED_REFERENCE_CAP = 1000  # assumed cap of nested lists, see spec 0001
PAGE_LIMIT = 1000
FINAL_STATUSES = frozenset({"ok", "not_found"})
RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


class S2Error(Exception):
    """A Semantic Scholar request still failed after all retries."""


# --------------------------------------------------------------------------- records


def normalize_reference(raw: dict) -> dict:
    """One reference as returned by S2 -> the shape stored in references.json."""
    ext = raw.get("externalIds") or {}
    year = raw.get("year")
    return {
        "s2_id": raw.get("paperId"),  # null when S2 could not resolve the citation
        "title": raw.get("title"),
        "authors": [a["name"] for a in raw.get("authors") or [] if a.get("name")],
        # ref_submission_date of the M3 table: full date when known, else the year
        "submission_date": raw.get("publicationDate") or (str(year) if year else None),
        "year": year,
        "venue": raw.get("venue") or None,
        "arxiv_id": ext.get("ArXiv"),  # old-style IDs such as hep-th/9901001 kept as is
        "external_ids": ext,
    }


def build_record(arxiv_id: str, paper: dict | None, references: list[dict] | None) -> dict:
    refs = [normalize_reference(r) for r in references or []]
    return {
        "arxiv_id": arxiv_id,
        "status": "ok" if paper is not None else "not_found",
        "s2_paper_id": paper.get("paperId") if paper else None,
        "fetched_at": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "num_references": len(refs),
        "num_arxiv_references": sum(r["arxiv_id"] is not None for r in refs),
        "references": refs,
    }


def references_path(data_root: Path, arxiv_id: str) -> Path:
    return data_root / to_folder_name(arxiv_id) / "references.json"


def is_done(data_root: Path, arxiv_id: str) -> bool:
    try:
        rec = json.loads(references_path(data_root, arxiv_id).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return False
    return rec.get("status") in FINAL_STATUSES


def write_record(data_root: Path, record: dict) -> None:
    path = references_path(data_root, record["arxiv_id"])
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    # Compact JSON: 23 % smaller than indented on the 800 test papers (output size is graded).
    text = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


# --------------------------------------------------------------------------- HTTP


class S2Client:
    """Semantic Scholar calls through the shared S2 limiter, with retries on 429/5xx."""

    def __init__(
        self,
        session: requests.Session | None = None,
        limiter: RateLimiter | None = None,
        api_key: str | None = config.SEMANTIC_SCHOLAR_API_KEY,
        *,
        max_attempts: int = 5,
        backoff_base_s: float = 2.0,
        timeout: tuple[float, float] = (15.0, 120.0),
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.session = session or requests.Session()
        self.session.headers["User-Agent"] = config.USER_AGENT
        if api_key:
            self.session.headers["x-api-key"] = api_key
        self.limiter = limiter or semantic_scholar_limiter(api_key is not None)
        self.max_attempts = max_attempts
        self.backoff_base_s = backoff_base_s
        self.timeout = timeout
        self._sleep = sleep
        self.requests_sent = 0

    def _call(self, method: str, url: str, **kwargs) -> object:
        reason = "no attempt"
        for attempt in range(1, self.max_attempts + 1):
            try:
                with self.limiter:
                    self.requests_sent += 1
                    resp = self.session.request(method, url, timeout=self.timeout, **kwargs)
                    status = resp.status_code
                    # "S2 <METHOD> <url> -> <status>" is what stats.py counts in run.log
                    log.info("S2 %s %s -> %d (attempt %d)", method, url, status, attempt)
                    if status == 200:
                        return resp.json()
                    retry_after = resp.headers.get("Retry-After")
            except (requests.ConnectionError, requests.Timeout, ValueError) as exc:
                reason = f"{type(exc).__name__}: {exc}"
                log.warning("S2 %s %s failed (%s), attempt %d", method, url, reason, attempt)
                self._sleep(self.backoff_base_s**attempt + random.random())
                continue
            reason = f"HTTP {status}"
            if status not in RETRYABLE_STATUS:
                break
            delay = self.backoff_base_s**attempt + random.random()
            if retry_after and retry_after.isdigit():
                delay = max(delay, float(retry_after))
            self._sleep(min(delay, 300.0))
        raise S2Error(f"{method} {url}: {reason}")

    def batch(self, arxiv_ids: list[str]) -> list[dict | None]:
        """One /paper/batch call; the result is aligned with ``arxiv_ids`` (None = unknown)."""
        data = self._call(
            "POST",
            BATCH_URL,
            params={"fields": BATCH_FIELDS},
            json={"ids": [f"arXiv:{i}" for i in arxiv_ids]},
        )
        if not isinstance(data, list) or len(data) != len(arxiv_ids):
            raise S2Error(f"batch answer does not match the {len(arxiv_ids)} requested IDs")
        return data

    def all_references(self, paper_id: str) -> list[dict]:
        """Every reference of one paper through the paginated endpoint."""
        refs, offset = [], 0
        while offset is not None:
            page = self._call(
                "GET",
                REFERENCES_URL.format(paper_id=paper_id),
                params={"fields": ",".join(REF_FIELDS), "limit": PAGE_LIMIT, "offset": offset},
            )
            refs += [d.get("citedPaper") or {} for d in page.get("data") or []]
            offset = page.get("next")
        return refs


# --------------------------------------------------------------------------- crawl


def _batches(items: list[str], size: int) -> Iterable[list[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


def crawl(
    arxiv_ids: list[str],
    data_root: Path,
    *,
    client: S2Client | None = None,
    batch_size: int = BATCH_SIZE,
) -> dict:
    """Write references.json for every paper of ``arxiv_ids`` not done yet; return counts."""
    data_root = Path(data_root)
    client = client or S2Client()
    todo = [i for i in arxiv_ids if not is_done(data_root, i)]
    log.info("references: %d papers, %d already done", len(arxiv_ids), len(arxiv_ids) - len(todo))
    counts = {"ok": 0, "not_found": 0, "failed": 0, "references": 0, "arxiv_references": 0}
    start = time.perf_counter()
    for batch in _batches(todo, batch_size):
        try:
            papers = client.batch(batch)
        except S2Error as exc:
            log.error("references: batch of %d failed, retried next run (%s)", len(batch), exc)
            counts["failed"] += len(batch)
            continue
        for arxiv_id, paper in zip(batch, papers, strict=True):
            refs = (paper or {}).get("references") or []
            if paper is not None and len(refs) >= NESTED_REFERENCE_CAP:
                try:
                    refs = client.all_references(paper["paperId"])
                except S2Error as exc:
                    log.error("references: %s paginated fetch failed (%s)", arxiv_id, exc)
                    counts["failed"] += 1
                    continue
            record = build_record(arxiv_id, paper, refs)
            write_record(data_root, record)
            counts[record["status"]] += 1
            counts["references"] += record["num_references"]
            counts["arxiv_references"] += record["num_arxiv_references"]
    counts.update(
        papers=len(arxiv_ids),
        skipped=len(arxiv_ids) - len(todo),
        requests=client.requests_sent,
        elapsed_s=round(time.perf_counter() - start, 1),
    )
    log.info("references: %s", counts)
    return counts


def corpus_ids(data_root: Path) -> list[str]:
    """IDs of every paper folder that has a metadata.json (i.e. was found by discovery)."""
    return sorted(
        json.loads((p / "metadata.json").read_text(encoding="utf-8"))["arxiv_id"]
        for p in data_root.iterdir()
        if (p / "metadata.json").exists()
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Crawl Semantic Scholar reference metadata.")
    parser.add_argument("--data-root", type=Path, default=Path(config.DEFAULT_DATA_ROOT))
    parser.add_argument("--ids", help="comma-separated IDs (default: every crawled paper)")
    parser.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ids = args.ids.split(",") if args.ids else corpus_ids(args.data_root)
    counts = crawl(ids, args.data_root, batch_size=args.batch_size)
    print(json.dumps(counts, indent=2))
    return 1 if counts["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
