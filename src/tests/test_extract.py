# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
import io
import tarfile

import pytest

from src.crawler.extract import CorruptArchiveError, detect_kind, extract_version, safe_relpath
from src.tests.helpers import MAIN_TEX, PNG, gzip_bytes, tar_bytes, zip_bytes

INTRO = b"\\section{Introduction}\nHello.\n"
BIB = b"@article{a, title={T}}\n"


def write(tmp_path, name, data):
    path = tmp_path / name
    path.write_bytes(data)
    return path


def test_targz_keeps_tex_and_bib_only_and_figure_commands(tmp_path):
    files = {
        "main.tex": MAIN_TEX,
        "sections/intro.tex": INTRO,
        "refs.bib": BIB,
        "refs.bbl": b"\\begin{thebibliography}{1}\\bibitem{a} A.\\end{thebibliography}",
        "figs/plot.png": PNG,
        "style.sty": b"% style",
    }
    archive = write(tmp_path, "a.src", tar_bytes(files))
    out = tmp_path / "v1"
    res = extract_version(archive, out)

    assert res.kind == "tar.gz" and res.has_tex
    assert res.files == ["main.tex", "refs.bib", "sections/intro.tex"]
    assert not (out / "figs").exists() and not (out / "refs.bbl").exists()
    tex = (out / "main.tex").read_bytes()
    assert b"\\begin{figure}" in tex and b"\\includegraphics{figs/plot.png}" in tex
    assert res.bytes_before == sum(len(d) for d in files.values())
    assert res.bytes_after == len(MAIN_TEX) + len(INTRO) + len(BIB)
    assert res.archive_bytes == archive.stat().st_size
    # what the whitelist left out, for the M1 report
    assert res.dropped == {
        ".bbl": {"files": 1, "bytes": len(files["refs.bbl"])},
        ".png": {"files": 1, "bytes": len(PNG)},
        ".sty": {"files": 1, "bytes": len(files["style.sty"])},
    }


def test_keep_extensions_can_include_bbl(tmp_path):
    archive = write(
        tmp_path, "a.src", tar_bytes({"main.tex": MAIN_TEX, "main.bbl": b"\\bibitem{a}"})
    )
    res = extract_version(archive, tmp_path / "v1", keep_extensions={".tex", ".bib", ".bbl"})
    assert res.files == ["main.bbl", "main.tex"]


def test_unsafe_members_are_skipped(tmp_path):
    link = tarfile.TarInfo("link.tex")
    link.type, link.linkname = tarfile.SYMTYPE, "/etc/passwd"
    extra = [(link, None)]
    for name in ("../evil.tex", "/abs.tex"):
        info = tarfile.TarInfo(name)
        info.size = 3
        extra.append((info, b"bad"))
    archive = write(tmp_path, "a.src", tar_bytes({"main.tex": MAIN_TEX}, extra=extra))
    out = tmp_path / "paper" / "v1"
    res = extract_version(archive, out)
    assert res.files == ["main.tex"]
    assert res.skipped_unsafe == 3
    assert not (tmp_path / "paper" / "evil.tex").exists()


def test_single_file_gzip_uses_name_from_header(tmp_path):
    archive = write(tmp_path, "a.src", gzip_bytes(MAIN_TEX, name="paper.tex"))
    res = extract_version(archive, tmp_path / "v1")
    assert res.kind == "gz" and res.files == ["paper.tex"]
    assert (tmp_path / "v1" / "paper.tex").read_bytes() == MAIN_TEX


def test_single_file_gzip_without_name_becomes_main_tex(tmp_path):
    archive = write(tmp_path, "a.src", gzip_bytes(MAIN_TEX))
    res = extract_version(archive, tmp_path / "v1")
    assert res.kind == "gz" and res.files == ["main.tex"] and res.has_tex


def test_pdf_only_version_is_an_empty_record(tmp_path):
    pdf = b"%PDF-1.5\n" + PNG
    archive = write(tmp_path, "a.src", pdf)
    out = tmp_path / "v1"
    res = extract_version(archive, out)
    assert res.kind == "pdf" and not res.has_tex and res.files == []
    assert out.is_dir() and not any(out.iterdir())
    assert res.bytes_before == len(pdf) and res.bytes_after == 0
    assert res.dropped == {"(pdf)": {"files": 1, "bytes": len(pdf)}}


def test_gzipped_pdf_is_detected(tmp_path):
    archive = write(tmp_path, "a.src", gzip_bytes(b"%PDF-1.4\n" + PNG))
    assert detect_kind(archive) == "pdf"


def test_gzipped_html_is_a_submission_without_tex(tmp_path):
    """2606.25881v1 (EPTCS proceedings): /src/ serves arXiv-2606.25881v1.html.gz."""
    page = b"<!DOCTYPE html>\n<html><head><title>EPTCS 446</title></head></html>\n"
    archive = write(tmp_path, "a.src", gzip_bytes(page, name="index.html"))
    res = extract_version(archive, tmp_path / "v1")
    assert res.kind == "html.gz" and not res.has_tex and res.files == []
    assert detect_kind(write(tmp_path, "raw", page)) == "html"  # unwrapped: error page


def test_plain_tar_zip_tex_and_html(tmp_path):
    assert (
        detect_kind(write(tmp_path, "t", tar_bytes({"main.tex": MAIN_TEX}, compress=False)))
        == "tar"
    )
    zipped = write(tmp_path, "z", zip_bytes({"main.tex": MAIN_TEX, "fig.png": PNG}))
    assert extract_version(zipped, tmp_path / "vz").files == ["main.tex"]
    tex = write(tmp_path, "x", MAIN_TEX)
    res = extract_version(tex, tmp_path / "vx", filename_hint="arXiv-2606.00001v1")
    assert res.kind == "tex" and res.files == ["arXiv-2606.00001v1.tex"]
    assert detect_kind(write(tmp_path, "h", b"<!DOCTYPE html><html></html>")) == "html"
    assert detect_kind(write(tmp_path, "e", b"")) == "empty"


def test_truncated_archive_raises_corrupt(tmp_path):
    data = tar_bytes({"main.tex": MAIN_TEX, "big.png": PNG * 50})
    archive = write(tmp_path, "a.src", data[: len(data) // 2])
    with pytest.raises(CorruptArchiveError):
        extract_version(archive, tmp_path / "v1")


def test_stale_output_is_replaced(tmp_path):
    out = tmp_path / "v1"
    out.mkdir()
    (out / "stale.tex").write_bytes(b"old")
    extract_version(write(tmp_path, "a.src", tar_bytes({"main.tex": MAIN_TEX})), out)
    assert sorted(p.name for p in out.iterdir()) == ["main.tex"]


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("sub/a.tex", "sub/a.tex"),
        ("./a.tex", "a.tex"),
        ("sub\\b.tex", "sub/b.tex"),
        ("../a.tex", None),
        ("/etc/a.tex", None),
        ("a/../../b.tex", None),
    ],
)
def test_safe_relpath(name, expected):
    rel = safe_relpath(name)
    assert (rel.as_posix() if rel else None) == expected


def test_old_style_tar_inside_gzip(tmp_path):
    """A gzip whose content is a tar without the ustar magic is still read as tar."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w", format=tarfile.USTAR_FORMAT) as tf:
        info = tarfile.TarInfo("main.tex")
        info.size = len(MAIN_TEX)
        tf.addfile(info, io.BytesIO(MAIN_TEX))
    raw = bytearray(buf.getvalue())
    raw[257:263] = b"\x00" * 6  # drop the magic, keep a valid checksum below
    chksum = sum(raw[:148]) + 8 * 32 + sum(raw[156:512])
    raw[148:156] = b"%06o\x00 " % chksum
    archive = write(tmp_path, "a.src", gzip_bytes(bytes(raw)))
    assert detect_kind(archive) == "tar.gz"
