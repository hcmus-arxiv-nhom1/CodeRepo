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


def _gzip_original_name(path: Path) -> str | None:
    """Original file name stored in the gzip header (FNAME field), if any."""
    with open(path, "rb") as fh:
        header = fh.read(10)
        if len(header) < 10 or header[:2] != b"\x1f\x8b":
            return None
        flags = header[3]
        if flags & 0x04:  # FEXTRA
            fh.read(int.from_bytes(fh.read(2), "little"))
        if not flags & 0x08:  # FNAME
            return None
        name = bytearray()
        while (byte := fh.read(1)) not in (b"", b"\x00") and len(name) < 1024:
            name += byte
    return name.decode("latin-1") or None


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


def _extract_tar(archive: Path, out_dir: Path, keep: frozenset[str], res: ExtractResult) -> None:
    try:
        with tarfile.open(archive, "r:*") as tf:
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
        raise CorruptArchiveError(f"{archive.name}: {exc}") from exc


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
    if _sniff(data[:HEAD_BYTES]) not in ("tex", "text"):
        return  # PDF, PostScript, HTML, binary...: empty record
    named_tex = name is not None and name.lower().endswith(".tex")
    if not (named_tex or _looks_like_tex(data)):
        return  # plain text that is not TeX: empty record
    rel = _single_file_name(name, keep)
    (out_dir / rel).write_bytes(data)
    res.bytes_after = len(data)
    res.files.append(rel.as_posix())


def extract_version(
    archive: Path,
    out_dir: Path,
    keep_extensions: Iterable[str] = config.KEEP_EXTENSIONS,
    filename_hint: str | None = None,
) -> ExtractResult:
    """Unpack ``archive`` into a fresh ``out_dir`` keeping text files; return size/kind stats.

    ``filename_hint`` is the Content-Disposition file name, used to name a single
    uncompressed file. Raises ``CorruptArchiveError`` for unreadable archives.
    """
    keep = frozenset(e.lower() for e in keep_extensions)
    if out_dir.exists():
        shutil.rmtree(out_dir)  # leftovers of an interrupted extraction
    out_dir.mkdir(parents=True)

    res = ExtractResult(kind=detect_kind(archive), archive_bytes=archive.stat().st_size)
    if res.kind in ("tar.gz", "tar"):
        _extract_tar(archive, out_dir, keep, res)
    elif res.kind == "zip":
        _extract_zip(archive, out_dir, keep, res)
    elif res.kind == "gz":
        try:
            with gzip.open(archive, "rb") as gz:
                data = gz.read()
        except _GZIP_ERRORS as exc:
            raise CorruptArchiveError(f"{archive.name}: {exc}") from exc
        _extract_single(data, _gzip_original_name(archive), out_dir, keep, res)
    elif res.kind == "tex":
        _extract_single(archive.read_bytes(), filename_hint, out_dir, keep, res)
    else:  # pdf, ps, html, text, empty, unknown: no TeX source
        res.bytes_before = res.archive_bytes

    res.files.sort()
    res.has_tex = any(f.lower().endswith(".tex") for f in res.files)
    return res
