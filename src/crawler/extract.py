# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""Detect the source type, unpack, keep text files only.

``/src/`` returns whatever the authors submitted, so the type is detected from magic
bytes, never from the file name: gzipped tar (the usual case), a single gzipped file
(normally one .tex), plain tar/zip, PDF or PostScript (no TeX), or an HTML page.
Gzipped HTML (``html.gz``) is a real submission, e.g. a workshop proceedings volume
(2606.25881 is an EPTCS index page); plain ``html`` on HTTP 200 is likely an error page.

Only files whose extension is in ``keep_extensions`` are written; .tex content is never
modified, so ``figure`` environments and ``\\includegraphics`` lines stay for M2.
Members are read and written one by one (no ``extractall``): absolute paths, ``..``,
links and names the OS cannot store are skipped. A version without any .tex file is an
empty ``vN/`` folder: an empty record, not a dropped publication.

Sizes: ``bytes_before`` is the uncompressed size of every regular file in the source,
``bytes_after`` the size of the kept files (the M1 "size before/after removing figures").
"""

from __future__ import annotations

import errno
import gzip
import os
import shutil
import stat
import tarfile
import zipfile
import zlib
from collections.abc import Iterable
from dataclasses import dataclass, field
from pathlib import Path, PurePosixPath
from typing import BinaryIO

from src import config

HEAD_BYTES = 1024
TEX_MARKERS = (
    b"\\documentclass",
    b"\\documentstyle",
    b"\\begin{document}",
    b"\\input",
    b"\\section",
    b"\\title",
    b"\\author",
    b"\\bye",
)
NO_TEX_KINDS = frozenset({"pdf", "ps", "html", "html.gz", "text", "empty", "unknown"})
_WINDOWS_RESERVED = {"CON", "PRN", "AUX", "NUL"} | {
    f"{p}{i}" for p in ("COM", "LPT") for i in range(1, 10)
}
_WINDOWS_BAD_CHARS = set('<>:"|?*')
_GZIP_ERRORS = (gzip.BadGzipFile, EOFError, zlib.error)
_CHUNK = 1 << 16


class CorruptArchiveError(Exception):
    """The archive cannot be read to the end (truncated or damaged download)."""


@dataclass
class ExtractResult:
    kind: str  # tar.gz | tar | gz | tex | zip | pdf | ps | html | html.gz | text | empty | unknown
    has_tex: bool = False
    files: list[str] = field(default_factory=list)  # kept files, posix paths inside vN/
    archive_bytes: int = 0  # size of the downloaded file
    bytes_before: int = 0
    bytes_after: int = 0
    skipped_unsafe: int = 0  # links, path traversal, names unusable on this OS
    # Files left out by the whitelist: extension (".png", "" when none) or, for a source
    # without any archive member, "(pdf)" / "(html.gz)"... -> {"files": n, "bytes": size}.
    dropped: dict[str, dict[str, int]] = field(default_factory=dict)

    def drop(self, key: str, size: int) -> None:
        entry = self.dropped.setdefault(key, {"files": 0, "bytes": 0})
        entry["files"] += 1
        entry["bytes"] += size


# --------------------------------------------------------------------------- detection


def _looks_like_tex(data: bytes) -> bool:
    head = data[: 1 << 20]
    return b"\x00" not in head[:HEAD_BYTES] and any(m in head for m in TEX_MARKERS)


def _sniff(head: bytes) -> str:
    if not head:
        return "empty"
    if head.startswith(b"\x1f\x8b"):
        return "gzip"
    if head.startswith(b"%PDF"):
        return "pdf"
    if head.startswith(b"%!PS"):
        return "ps"
    if head.startswith((b"PK\x03\x04", b"PK\x05\x06")):
        return "zip"
    if len(head) >= 262 and head[257:262] == b"ustar":
        return "tar"
    lowered = head.lstrip().lower()
    if lowered.startswith((b"<!doctype html", b"<html")):
        return "html"
    if b"\x00" not in head:
        return "tex" if _looks_like_tex(head) else "text"
    return "unknown"


def _is_tar(path: Path) -> bool:
    try:
        with tarfile.open(path, "r:*") as tf:
            return tf.next() is not None
    except (tarfile.TarError, *_GZIP_ERRORS):
        return False


def detect_kind(path: Path) -> str:
    with open(path, "rb") as fh:
        head = fh.read(HEAD_BYTES)
    kind = _sniff(head)
    if kind != "gzip":
        return kind
    try:
        with gzip.open(path, "rb") as gz:
            inner = gz.read(HEAD_BYTES)
    except _GZIP_ERRORS as exc:
        raise CorruptArchiveError(f"{path.name}: {exc}") from exc
    inner_kind = _sniff(inner)
    if inner_kind == "tar" or (inner_kind in ("text", "unknown") and _is_tar(path)):
        return "tar.gz"  # second test catches old tar formats without the ustar magic
    if inner_kind == "html":
        return "html.gz"  # submitted HTML (proceedings index), not an error page
    if inner_kind in ("pdf", "ps", "empty"):
        return inner_kind
    return "gz"  # one compressed file, normally a .tex


def _gzip_name_from_header(head: bytes) -> str | None:
    """Original file name stored in a gzip header (FNAME field), if any."""
    if len(head) < 10 or head[:2] != b"\x1f\x8b":
        return None
    flags, pos = head[3], 10
    if flags & 0x04:  # FEXTRA
        pos += 2 + int.from_bytes(head[10:12], "little")
    if not flags & 0x08:  # FNAME
        return None
    end = head.find(b"\x00", pos)
    return head[pos : end if end != -1 else None].decode("latin-1") or None


def _tar_header_ok(block: bytes) -> bool:
    """True when ``block`` is a valid tar header, ustar magic or not (old tar formats)."""
    try:
        tarfile.TarInfo.frombuf(block[:512], "utf-8", "surrogateescape")
    except tarfile.TarError:
        return False
    return True


class _Reader:
    """Read-ahead wrapper: ``peek`` the first bytes to detect the type, count raw bytes,
    optionally copy them to ``tee`` (raw responses kept as fixtures by the spike)."""

    def __init__(self, raw: BinaryIO, tee: BinaryIO | None = None) -> None:
        self._raw, self._tee = raw, tee
        self._buf = b""
        self.count = 0

    def _more(self, n: int) -> bytes:
        chunk = self._raw.read(n)
        if chunk:
            self.count += len(chunk)
            if self._tee is not None:
                self._tee.write(chunk)
        return chunk

    def peek(self, n: int) -> bytes:
        while len(self._buf) < n and (chunk := self._more(max(n - len(self._buf), _CHUNK))):
            self._buf += chunk
        return self._buf[:n]

    def read(self, n: int = -1) -> bytes:
        if n is None or n < 0:
            parts = [self._buf]
            while chunk := self._more(_CHUNK):
                parts.append(chunk)
            self._buf = b""
            return b"".join(parts)
        if not self._buf:
            return self._more(n)
        out, self._buf = self._buf[:n], self._buf[n:]
        return out

    def drain(self) -> None:
        while self.read(_CHUNK):
            pass


# --------------------------------------------------------------------------- safe writing


def _bad_on_windows(part: str) -> bool:
    return (
        part != part.rstrip(" .")
        or any(c in _WINDOWS_BAD_CHARS or ord(c) < 32 for c in part)
        or part.split(".")[0].upper() in _WINDOWS_RESERVED
    )


def safe_relpath(name: str) -> PurePosixPath | None:
    """Archive member name -> safe relative path, or None (absolute, '..', unusable)."""
    path = PurePosixPath(name.replace("\\", "/"))
    if path.is_absolute():
        return None
    parts = [p for p in path.parts if p not in ("", ".")]
    if not parts or ".." in parts or any("\x00" in p for p in parts):
        return None
    if os.name == "nt" and any(_bad_on_windows(p) for p in parts):
        return None
    return PurePosixPath(*parts)


def _write_member(src: BinaryIO, out_dir: Path, rel: PurePosixPath) -> bool:
    target = out_dir.joinpath(*rel.parts)
    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        with open(target, "wb") as dst:
            shutil.copyfileobj(src, dst)
    except UnicodeError:
        return False
    except OSError as exc:
        if exc.errno == errno.ENOSPC:
            raise
        return False  # name the filesystem refuses
    return True


def _keep(rel: PurePosixPath, keep_extensions: Iterable[str]) -> bool:
    return rel.suffix.lower() in keep_extensions


# --------------------------------------------------------------------------- extractors


def _extract_tar(src, out_dir: Path, keep: frozenset[str], res: ExtractResult) -> None:
    """Members of a tar *stream* one by one (``r|``: no seeking, so it works on a download
    in progress); only kept members are written, the others are read past."""
    try:
        with tarfile.open(fileobj=src, mode="r|") as tf:
            for member in tf:
                if member.issym() or member.islnk():
                    res.skipped_unsafe += 1
                    continue
                if not member.isfile():
                    continue
                res.bytes_before += member.size
                rel = safe_relpath(member.name)
                if rel is None:
                    res.skipped_unsafe += 1
                    continue
                if not _keep(rel, keep):
                    res.drop(rel.suffix.lower(), member.size)
                    continue
                src = tf.extractfile(member)
                if src is None:
                    continue
                with src:
                    if not _write_member(src, out_dir, rel):
                        res.skipped_unsafe += 1
                        continue
                res.bytes_after += member.size
                res.files.append(rel.as_posix())
    except (tarfile.TarError, *_GZIP_ERRORS) as exc:
        raise CorruptArchiveError(f"tar: {exc}") from exc


def _extract_zip(archive: Path, out_dir: Path, keep: frozenset[str], res: ExtractResult) -> None:
    try:
        with zipfile.ZipFile(archive) as zf:
            for info in zf.infolist():
                if info.is_dir():
                    continue
                if stat.S_ISLNK(info.external_attr >> 16):
                    res.skipped_unsafe += 1
                    continue
                res.bytes_before += info.file_size
                rel = safe_relpath(info.filename)
                if rel is None:
                    res.skipped_unsafe += 1
                    continue
                if not _keep(rel, keep):
                    res.drop(rel.suffix.lower(), info.file_size)
                    continue
                with zf.open(info) as src:
                    if not _write_member(src, out_dir, rel):
                        res.skipped_unsafe += 1
                        continue
                res.bytes_after += info.file_size
                res.files.append(rel.as_posix())
    except (zipfile.BadZipFile, NotImplementedError, *_GZIP_ERRORS) as exc:
        raise CorruptArchiveError(f"{archive.name}: {exc}") from exc


def _single_file_name(candidate: str | None, keep: frozenset[str]) -> PurePosixPath:
    base = PurePosixPath(candidate.replace("\\", "/")).name if candidate else ""
    rel = safe_relpath(base) if base else None
    if rel is None:
        return PurePosixPath("main.tex")
    if not _keep(rel, keep):
        rel = PurePosixPath(rel.name + ".tex")  # append: "arXiv-2606.32040v1" has no real suffix
    return rel


def _extract_single(
    data: bytes, name: str | None, out_dir: Path, keep: frozenset[str], res: ExtractResult
) -> None:
    res.bytes_before = len(data)
    sniffed = _sniff(data[:HEAD_BYTES])
    if sniffed not in ("tex", "text"):
        res.drop(f"({sniffed})", len(data))
        return  # PDF, PostScript, HTML, binary...: empty record
    named_tex = name is not None and name.lower().endswith(".tex")
    if not (named_tex or _looks_like_tex(data)):
        res.drop("(text)", len(data))
        return  # plain text that is not TeX: empty record
    rel = _single_file_name(name, keep)
    (out_dir / rel).write_bytes(data)
    res.bytes_after = len(data)
    res.files.append(rel.as_posix())


def extract_stream(
    raw: BinaryIO,
    out_dir: Path,
    keep_extensions: Iterable[str] = config.KEEP_EXTENSIONS,
    filename_hint: str | None = None,
    size_hint: int | None = None,
    tee: BinaryIO | None = None,
) -> ExtractResult:
    """Unpack a source *while it is read* into a fresh ``out_dir``, keeping text files.

    The archive is never stored: tar members are written only when their extension is
    kept and read past otherwise, and a source without TeX (PDF, PostScript, HTML) is
    recognised from its first bytes, so the rest of it is not even downloaded.
    ``size_hint`` (Content-Length or file size) is the archive size for those early
    stops; ``filename_hint`` (Content-Disposition) names a single uncompressed file.
    Raises ``CorruptArchiveError`` for unreadable archives.
    """
    keep = frozenset(e.lower() for e in keep_extensions)
    if out_dir.exists():
        shutil.rmtree(out_dir)  # leftovers of an interrupted extraction
    out_dir.mkdir(parents=True)

    src = _Reader(raw, tee)
    head = src.peek(HEAD_BYTES)
    res = ExtractResult(kind=_sniff(head))
    if res.kind == "gzip":
        gz = _Reader(gzip.GzipFile(fileobj=src, mode="rb"))
        try:
            inner = gz.peek(HEAD_BYTES)
        except _GZIP_ERRORS as exc:
            raise CorruptArchiveError(f"gzip: {exc}") from exc
        inner_kind = _sniff(inner)
        if inner_kind == "tar" or (inner_kind in ("text", "unknown") and _tar_header_ok(inner)):
            res.kind = "tar.gz"  # second test catches old tar formats without the ustar magic
            _extract_tar(gz, out_dir, keep, res)
        elif inner_kind == "html":
            res.kind = "html.gz"  # submitted HTML (proceedings index), not an error page
        elif inner_kind in ("pdf", "ps", "empty"):
            res.kind = inner_kind
        else:  # one compressed file, normally a .tex
            res.kind = "gz"
            try:
                data = gz.read()
            except _GZIP_ERRORS as exc:
                raise CorruptArchiveError(f"gzip: {exc}") from exc
            _extract_single(data, _gzip_name_from_header(head), out_dir, keep, res)
        if res.kind in ("tar.gz", "gz"):
            try:
                gz.drain()  # reach the gzip trailer: its CRC check catches corrupt bodies
            except _GZIP_ERRORS as exc:
                raise CorruptArchiveError(f"gzip: {exc}") from exc
    elif res.kind == "tar":
        _extract_tar(src, out_dir, keep, res)
    elif res.kind == "zip":  # the central directory is at the end: spool to disk (rare)
        spool = out_dir.parent / f".{out_dir.name}.zip.part"
        try:
            with open(spool, "wb") as fh:
                fh.write(src.read())
            _extract_zip(spool, out_dir, keep, res)
        finally:
            spool.unlink(missing_ok=True)
    elif res.kind == "tex":
        _extract_single(src.read(), filename_hint, out_dir, keep, res)

    if res.kind in ("tar.gz", "tar", "gz", "zip", "tex") or tee is not None:
        src.drain()  # whole body read: archive_bytes is exact (and a fixture is complete)
        res.archive_bytes = src.count
    else:  # no TeX source: stop downloading after the first bytes
        res.archive_bytes = size_hint if size_hint is not None else src.count
        res.bytes_before = res.archive_bytes
        if res.archive_bytes:
            res.drop(f"({res.kind})", res.archive_bytes)
    res.files.sort()
    res.has_tex = any(f.lower().endswith(".tex") for f in res.files)
    return res


def extract_version(
    archive: Path,
    out_dir: Path,
    keep_extensions: Iterable[str] = config.KEEP_EXTENSIONS,
    filename_hint: str | None = None,
) -> ExtractResult:
    """``extract_stream`` on a file already on disk (tests, saved fixtures)."""
    with open(archive, "rb") as fh:
        return extract_stream(fh, out_dir, keep_extensions, filename_hint, archive.stat().st_size)
