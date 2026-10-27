# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""Download ``https://export.arxiv.org/src/{id}v{N}`` and unpack it while it arrives.

The source archive is streamed straight into ``extract.extract_stream``: only the
``.tex``/``.bib`` members ever reach the disk, a source without TeX is recognised from
its first bytes and the rest is not downloaded, and a transfer cut mid-body resumes with
a Range request (``HttpClient.stream``). Pacing, retries, ``Retry-After`` and the 403
circuit breaker live in ``HttpClient``; this module maps the outcome to a crawl status:

- ``ok``: the version was unpacked into ``vdir`` (``extract`` holds kind, files, sizes);
- ``unavailable``: arXiv answered 404/410 for a version discovery reported, which
  becomes an empty record rather than a dropped publication;
- ``corrupt``: the archive could not be read to the end (downloaded again by the caller);
- ``failed``: anything else after all retries (goes to failed.csv and is retried next run).
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

from src import config
from src.crawler.extract import CorruptArchiveError, ExtractResult, extract_stream
from src.crawler.http_client import BlockedError, FetchError, HttpClient, ResumableBody

_FILENAME_RE = re.compile(r'filename="?([^";]+)"?')


@dataclass
class DownloadOutcome:
    status: str  # ok | unavailable | corrupt | failed
    extract: ExtractResult | None = None
    http_status: int | None = None
    content_type: str | None = None
    filename: str | None = None  # from Content-Disposition, e.g. arXiv-2606.32040v1.tar.gz
    nbytes: int = 0  # body bytes actually received (less than the archive for PDF-only)
    elapsed_s: float = 0.0  # download + extraction, which overlap
    attempts: int = 0
    resumes: int = 0
    reason: str | None = None


def source_url(arxiv_id: str, version: int) -> str:
    return config.ARXIV_SRC_URL.format(arxiv_id=arxiv_id, version=version)


def download_version(
    client: HttpClient,
    arxiv_id: str,
    version: int,
    vdir: Path,
    keep_extensions: Iterable[str] = config.KEEP_EXTENSIONS,
    archive_dir: Path | None = None,
) -> DownloadOutcome:
    """Stream one version into ``vdir``; ``archive_dir`` also keeps the raw response."""
    url = source_url(arxiv_id, version)
    filename: str | None = None

    def consume(body: ResumableBody, headers: Mapping[str, str]) -> ExtractResult:
        nonlocal filename
        match = _FILENAME_RE.search(headers.get("Content-Disposition", ""))
        filename = match.group(1) if match else None
        length = headers.get("Content-Length")
        size = int(length) if length is not None else None
        if archive_dir is None:
            return extract_stream(body, vdir, keep_extensions, filename, size)
        archive_dir.mkdir(parents=True, exist_ok=True)  # fixtures for the spike
        name = Path(filename or f"{arxiv_id}v{version}.src").name
        with open(archive_dir / name, "wb") as tee:
            return extract_stream(body, vdir, keep_extensions, filename, size, tee=tee)

    try:
        # identity: keep the stored bytes as they are, magic-byte detection does the rest.
        res = client.stream(url, consume, headers={"Accept-Encoding": "identity"})
    except BlockedError:
        raise
    except FetchError as exc:
        return DownloadOutcome(
            status="failed", http_status=exc.status, attempts=exc.attempts, reason=exc.reason
        )
    except CorruptArchiveError as exc:
        return DownloadOutcome(status="corrupt", filename=filename, reason=str(exc))

    common = {
        "http_status": res.status,
        "content_type": res.headers.get("Content-Type"),
        "filename": filename,
        "nbytes": res.nbytes,
        "elapsed_s": res.elapsed_s,
        "attempts": res.attempts,
        "resumes": res.resumes,
    }
    if res.status == 200:
        return DownloadOutcome(status="ok", extract=res.value, **common)
    if res.status in (404, 410):
        return DownloadOutcome(status="unavailable", reason=f"HTTP {res.status}", **common)
    return DownloadOutcome(status="failed", reason=f"HTTP {res.status}", **common)
