"""Download ``https://export.arxiv.org/src/{id}v{N}`` for one version.

Pacing, retries, ``Retry-After``, the 403 circuit breaker and the ``.part`` handling live
in ``HttpClient``; this module maps the HTTP outcome to a crawl status:

- ``ok``: archive saved to ``dest_dir`` (type is detected later by ``extract``);
- ``unavailable``: arXiv answered 404/410 for a version discovery reported, which
  becomes an empty record rather than a dropped publication;
- ``failed``: anything else after all retries (goes to failed.csv and is retried next run).
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path

from src import config
from src.crawler.http_client import BlockedError, FetchError, HttpClient
from src.utils.ids import to_folder_name

_FILENAME_RE = re.compile(r'filename="?([^";]+)"?')


@dataclass
class DownloadOutcome:
    status: str  # ok | unavailable | failed
    path: Path | None = None
    http_status: int | None = None
    content_type: str | None = None
    filename: str | None = None  # from Content-Disposition, e.g. arXiv-2606.32040v1.tar.gz
    nbytes: int = 0
    elapsed_s: float = 0.0
    attempts: int = 0
    reason: str | None = None


def source_url(arxiv_id: str, version: int) -> str:
    return config.ARXIV_SRC_URL.format(arxiv_id=arxiv_id, version=version)


def download_version(
    client: HttpClient, arxiv_id: str, version: int, dest_dir: Path
) -> DownloadOutcome:
    url = source_url(arxiv_id, version)
    dest = dest_dir / f"{to_folder_name(arxiv_id)}v{version}.src"
    try:
        # identity: keep the stored bytes as they are, magic-byte detection does the rest.
        res = client.get(url, dest=dest, headers={"Accept-Encoding": "identity"})
    except BlockedError:
        raise
    except FetchError as exc:
        return DownloadOutcome(
            status="failed", http_status=exc.status, attempts=exc.attempts, reason=exc.reason
        )

    common = {
        "http_status": res.status,
        "content_type": res.headers.get("Content-Type"),
        "elapsed_s": res.elapsed_s,
        "attempts": res.attempts,
    }
    if res.status == 200:
        match = _FILENAME_RE.search(res.headers.get("Content-Disposition", ""))
        return DownloadOutcome(
            status="ok",
            path=res.path,
            filename=match.group(1) if match else None,
            nbytes=res.nbytes,
            **common,
        )
    if res.status in (404, 410):
        return DownloadOutcome(status="unavailable", reason=f"HTTP {res.status}", **common)
    return DownloadOutcome(status="failed", reason=f"HTTP {res.status}", **common)
