"""Test doubles: a fake clock, a scripted HTTP session and builders for arXiv-like payloads."""

from __future__ import annotations

import gzip
import io
import tarfile
import zipfile
from collections.abc import Callable
from urllib.parse import parse_qs, urlparse

from requests.structures import CaseInsensitiveDict

from src.crawler.http_client import HttpClient
from src.crawler.ratelimit import RateLimiter


class FakeClock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t

    def sleep(self, seconds: float) -> None:
        self.t += seconds


class FakeResponse:
    def __init__(
        self,
        status: int = 200,
        body: bytes = b"",
        headers: dict | None = None,
        fail_midway: Exception | None = None,
    ) -> None:
        self.status_code = status
        self._body = body
        self.headers = CaseInsensitiveDict(headers or {})
        self._fail_midway = fail_midway

    @property
    def content(self) -> bytes:
        return self._body

    def iter_content(self, chunk_size: int):
        half = len(self._body) // 2
        yield self._body[:half]
        if self._fail_midway is not None:
            raise self._fail_midway
        yield self._body[half:]

    def close(self) -> None:
        pass


class FakeSession:
    """Calls ``handler(url)`` for every GET; the handler returns a FakeResponse or raises."""

    def __init__(self, handler: Callable[[str], FakeResponse]) -> None:
        self.handler = handler
        self.headers: dict[str, str] = {}
        self.calls: list[str] = []

    def get(self, url: str, headers=None, timeout=None, stream=False) -> FakeResponse:
        self.calls.append(url)
        return self.handler(url)


def make_client(
    handler: Callable[[str], FakeResponse], clock: FakeClock | None = None, **kw
) -> HttpClient:
    clock = clock or FakeClock()
    limiter = RateLimiter(3.0, 0.0, clock=clock, sleep=clock.sleep)
    kw.setdefault("rng", lambda: 0.0)
    return HttpClient(limiter, "test-agent", session=FakeSession(handler), **kw)


def query(url: str) -> dict[str, str]:
    return {k: v[0] for k, v in parse_qs(urlparse(url).query).items()}


def atom_feed(entries: list[dict]) -> bytes:
    """Minimal arXiv API feed. Entry keys: id, version, published, updated, title, authors..."""
    out = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        '<feed xmlns="http://www.w3.org/2005/Atom" xmlns:arxiv="http://arxiv.org/schemas/atom">',
    ]
    for e in entries:
        out.append("<entry>")
        out.append(f"<id>http://arxiv.org/abs/{e['id']}v{e.get('version', 1)}</id>")
        out.append(f"<updated>{e.get('updated', '2026-06-01T00:00:00Z')}</updated>")
        out.append(f"<published>{e.get('published', '2026-06-01T00:00:00Z')}</published>")
        out.append(f"<title>{e.get('title', 'A title')}</title>")
        for name in e.get("authors", ["Ada Lovelace"]):
            out.append(f"<author><name>{name}</name></author>")
        if e.get("journal_ref"):
            out.append(f"<arxiv:journal_ref>{e['journal_ref']}</arxiv:journal_ref>")
        out.append('<category term="cs.LG" scheme="http://arxiv.org/schemas/atom"/>')
        out.append("</entry>")
    out.append("</feed>")
    return "".join(out).encode()


def tar_bytes(files: dict[str, bytes], compress: bool = True, extra: list | None = None) -> bytes:
    """Build a (gzipped) tar; ``extra`` holds ready-made TarInfo objects (links, odd names)."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz" if compress else "w") as tf:
        for name, data in files.items():
            info = tarfile.TarInfo(name)
            info.size = len(data)
            tf.addfile(info, io.BytesIO(data))
        for info, data in extra or []:
            tf.addfile(info, io.BytesIO(data) if data is not None else None)
    return buf.getvalue()


def gzip_bytes(data: bytes, name: str | None = None) -> bytes:
    buf = io.BytesIO()
    with gzip.GzipFile(filename=name or "", fileobj=buf, mode="wb", mtime=0) as gz:
        gz.write(data)
    return buf.getvalue()


def zip_bytes(files: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for name, data in files.items():
            zf.writestr(name, data)
    return buf.getvalue()


MAIN_TEX = (
    b"\\documentclass{article}\n\\begin{document}\n\\input{sections/intro}\n"
    b"\\begin{figure}\\includegraphics{figs/plot.png}\\caption{A plot}\\end{figure}\n"
    b"\\end{document}\n"
)
PNG = b"\x89PNG\r\n\x1a\n" + bytes(range(256)) * 4
