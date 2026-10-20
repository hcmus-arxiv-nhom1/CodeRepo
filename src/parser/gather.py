"""Multi-file gathering (M2): find the root ``.tex`` of a version and the files it pulls in.

Rules (statement M2):
- the root is a ``.tex`` file containing ``\\documentclass``;
- ``\\input{}`` / ``\\include{}`` are followed transitively (the ``.tex`` suffix may be
  omitted, cycles are cut);
- a command inside a ``%`` comment is plain text and is not followed;
- ``.tex`` files that cannot be reached from the root are redundant and ignored.

Several files declare ``\\documentclass`` in about 2.5% of versions (main paper +
supplementary, unedited conference templates, rebuttal...). The root is then the
candidate that no other candidate includes, has ``\\begin{document}``, is not a
``standalone`` / ``subfiles`` piece, is not named like a side document, has a
``\\title`` matching the arXiv title (from ``metadata.json``), sits highest in the tree
and reaches the most text, in that order. The other candidates are reported in
``other_roots`` so the choice can be audited.

Paths are resolved from the root's folder (as LaTeX does), then from the including
file's folder. ``\\verb`` / verbatim content is not special-cased: a ``%`` there is
read as a comment.

    python -m src.parser.gather data/corpus            # whole corpus: root/ambiguity stats
    python -m src.parser.gather data/corpus/2606-30762/v1

Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from pathlib import Path, PurePosixPath

# Line by line: code before the first unescaped "%" (group 1) and the comment (group 2).
# "\\." consumes escaped characters, so "\%" is text and "\\%" starts a comment.
_COMMENT_RE = re.compile(r"(?m)^((?:[^\\%\n]|\\.)*)(%[^\n]*)")
_INCLUDE_RE = re.compile(
    r"\\(input|include)(?![A-Za-z@])\s*(?:\{([^{}]*)\}|([^\s{}%\\]+))"  # {name} or bare name
)
_DOCCLASS_RE = re.compile(
    r"\\document(?:class|style)(?![A-Za-z@])\s*(?:\[[^\]]*\])?\s*(?:\{([^{}]*)\})?"
)
_BEGIN_DOC_RE = re.compile(r"\\begin\s*\{document\}")
_TITLE_RE = re.compile(r"\\title\s*(?:\[[^\]]*\])?\s*\{")
_PIECE_CLASSES = frozenset({"standalone", "subfiles"})
_SIDE_DOC_WORDS = frozenset(
    {
        "si",
        "sm",
        "appendix",
        "appendices",
        "rebuttal",
        "response",
        "reply",
        "cover",
        "letter",
        "checklist",
        "lualatex",
        "template",
        "example",
        "sample",
        "backup",
        "old",
    }
)
TITLE_MATCH = 0.6  # share of the arXiv title's words found in the candidate's \title


@dataclass
class GatherResult:
    root: str | None  # posix path relative to the version folder; None if no \documentclass
    files: list[str] = field(default_factory=list)  # root first, then in first-include order
    unused: list[str] = field(default_factory=list)  # .tex files not reachable from the root
    missing: list[str] = field(default_factory=list)  # include targets not found (e.g. .pgf)
    other_roots: list[str] = field(default_factory=list)  # other \documentclass files


@dataclass
class _TexFile:
    text: str
    code: str  # text without comments
    doc_class: str | None  # None when the file has no \documentclass
    has_begin_document: bool
    title_words: set[str]  # words of the \title argument, empty when there is none
    includes: list[tuple[str, str | None, str | None]]  # (command, {name}, bare name)


def read_tex(path: Path) -> str:
    data = path.read_bytes()
    try:
        return data.decode("utf-8")
    except UnicodeDecodeError:
        return data.decode("latin-1")


def strip_comments(text: str) -> str:
    return _COMMENT_RE.sub(r"\1", text)


def _segments(text: str) -> Iterator[tuple[str, str]]:
    """``(code, comment)`` pairs covering ``text``; comments exclude the newline."""
    last = 0
    for m in _COMMENT_RE.finditer(text):
        yield text[last : m.start(2)], m.group(2)
        last = m.end(2)
    yield text[last:], ""


def _is_side_document(rel: str) -> bool:
    parts = PurePosixPath(rel).with_suffix("").parts
    words = {
        w
        for p in parts
        for w in re.split(r"[^a-z0-9]+", re.sub(r"([a-z])([A-Z])", r"\1 \2", p).lower())
        if w
    }
    return bool(words & _SIDE_DOC_WORDS) or any(w.startswith("supp") for w in words)


def _words(text: str) -> set[str]:
    text = re.sub(r"\\[A-Za-z@]+\*?", " ", text)  # drop commands, keep their arguments
    return {w for w in re.split(r"[^a-z0-9]+", text.lower()) if w}


def _title_words(code: str) -> set[str]:
    """Words of the first ``\\title{...}`` argument (balanced braces, at most 1,000 chars)."""
    m = _TITLE_RE.search(code)
    if m is None:
        return set()
    depth, end = 1, m.end()
    while end < len(code) and depth and end - m.end() < 1000:
        depth += {"{": 1, "}": -1}.get(code[end], 0)
        end += 1
    return _words(code[m.end() : end - 1])


class TexVersion:
    """The ``.tex`` files of one ``vN/`` folder; ``title`` is the arXiv title, if known."""

    def __init__(self, version_dir: Path, title: str | None = None) -> None:
        self.dir = Path(version_dir)
        self.title_words = _words(title or "")
        self.tex: dict[str, _TexFile] = {}
        for path in sorted(self.dir.rglob("*")):
            if path.is_file() and path.suffix.lower() == ".tex":
                self.tex[path.relative_to(self.dir).as_posix()] = self._parse(read_tex(path))
        self._lower = {rel.lower(): rel for rel in self.tex}

    @staticmethod
    def _parse(text: str) -> _TexFile:
        code = strip_comments(text)
        dc = _DOCCLASS_RE.search(code)
        return _TexFile(
            text=text,
            code=code,
            doc_class=(dc.group(1) or "").strip() if dc else None,
            has_begin_document=_BEGIN_DOC_RE.search(code) is not None,
            title_words=_title_words(code),
            includes=[m.groups() for m in _INCLUDE_RE.finditer(code)],
        )

    def _title_matches(self, rel: str) -> bool:
        if not self.title_words:
            return False
        shared = self.title_words & self.tex[rel].title_words
        return len(shared) >= TITLE_MATCH * len(self.title_words)

    # ----------------------------------------------------------------- resolution

    def resolve(self, name: str, including: str) -> str | None:
        """Include target -> relative path of an existing .tex file, or None."""
        name = name.strip().strip('"').strip()
        bases = [PurePosixPath(".")]
        if (parent := PurePosixPath(including).parent) != PurePosixPath("."):
            bases.append(parent)
        for base in bases:
            path = _normalize(base / name)
            if path is None:
                continue
            tries = (
                [path] if path.suffix.lower() == ".tex" else [path.with_name(path.name + ".tex")]
            )
            for p in tries:
                rel = p.as_posix()
                if rel in self.tex:
                    return rel
                if rel.lower() in self._lower:  # authors on case-insensitive file systems
                    return self._lower[rel.lower()]
        return None

    def _targets(self, rel: str) -> Iterator[tuple[str, str | None]]:
        """``(name, resolved)`` for each followable include of ``rel``, in order."""
        for cmd, braced, bare in self.tex[rel].includes:
            if cmd == "include" and braced is None:
                continue  # \include always takes a braced argument
            name = braced if braced is not None else bare
            if not name.strip() or "#" in name or "\\" in name:
                continue  # macro parameter or computed name: not a file reference
            yield name.strip(), self.resolve(name, rel)

    def walk(self, start: str) -> tuple[list[str], list[str]]:
        """Files reachable from ``start`` (first-include order) and unresolved targets."""
        order: list[str] = []
        missing: list[str] = []
        seen: set[str] = set()

        def visit(rel: str) -> None:
            seen.add(rel)
            order.append(rel)
            for name, target in self._targets(rel):
                if target is None:
                    if name not in missing:
                        missing.append(name)
                elif target not in seen:
                    visit(target)

        visit(start)
        return order, missing

    # ----------------------------------------------------------------- root

    def gather(self) -> GatherResult:
        candidates = [rel for rel, f in self.tex.items() if f.doc_class is not None]
        if not candidates:
            return GatherResult(root=None, unused=sorted(self.tex))
        walks = {c: self.walk(c) for c in candidates}
        included = {f for c in candidates for f in walks[c][0][1:]}

        def rank(c: str) -> tuple:
            f = self.tex[c]
            reach_chars = sum(len(self.tex[r].text) for r in walks[c][0])
            return (
                c in included,
                not f.has_begin_document,
                f.doc_class in _PIECE_CLASSES,
                _is_side_document(c),
                not self._title_matches(c),
                c.count("/"),
                -reach_chars,
                c,
            )

        root = min(candidates, key=rank)
        files, missing = walks[root]
        reached = set(files)
        return GatherResult(
            root=root,
            files=files,
            unused=[rel for rel in self.tex if rel not in reached],
            missing=missing,
            other_roots=[c for c in candidates if c != root],
        )

    def expand(self, root: str | None = None) -> str | None:
        """Root text with every followed ``\\input``/``\\include`` replaced by the file content.

        Comments are kept verbatim (a commented ``\\input`` stays as text); unresolved
        includes and includes that would close a cycle are left as written.
        """
        root = root or self.gather().root
        if root is None:
            return None

        def render(rel: str, stack: frozenset[str]) -> str:
            def substitute(m: re.Match) -> str:
                cmd, braced, bare = m.groups()
                if cmd == "include" and braced is None:
                    return m.group(0)
                name = braced if braced is not None else bare
                if "#" in name or "\\" in name:
                    return m.group(0)
                target = self.resolve(name, rel)
                if target is None or target in stack:
                    return m.group(0)
                return render(target, stack | {target})

            return "".join(
                _INCLUDE_RE.sub(substitute, code) + comment
                for code, comment in _segments(self.tex[rel].text)
            )

        return render(root, frozenset({root}))


def _normalize(path: PurePosixPath) -> PurePosixPath | None:
    """Collapse ``.``/``..``; None when the path is absolute or leaves the version folder."""
    if path.is_absolute():
        return None
    parts: list[str] = []
    for part in path.parts:
        if part in ("", "."):
            continue
        if part == "..":
            if not parts:
                return None
            parts.pop()
        else:
            parts.append(part)
    return PurePosixPath(*parts) if parts else None


def paper_title(version_dir: Path) -> str | None:
    """Title from the paper's ``metadata.json`` (``<yymm-id>/vN/../metadata.json``)."""
    try:
        meta = json.loads((Path(version_dir).parent / "metadata.json").read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    return meta.get("title")


def gather(version_dir: Path) -> GatherResult:
    return TexVersion(version_dir, paper_title(version_dir)).gather()


def expand(version_dir: Path) -> str | None:
    return TexVersion(version_dir, paper_title(version_dir)).expand()


# --------------------------------------------------------------------------- CLI


def iter_version_dirs(path: Path) -> Iterator[Path]:
    """``path`` is a version folder (``vN``), a paper folder or a corpus root."""
    if re.fullmatch(r"v\d+", path.name):
        yield path
        return
    papers = [path] if (path / "metadata.json").exists() else sorted(path.iterdir())
    for paper in papers:
        if paper.is_dir():
            yield from sorted(
                (v for v in paper.iterdir() if v.is_dir() and re.fullmatch(r"v\d+", v.name)),
                key=lambda v: int(v.name[1:]),
            )


SUMMARY_KEYS = (
    "versions",
    "with_root",
    "multi_file",
    "ambiguous_root",
    "with_unused_tex",
    "with_missing_tex",
    "no_root_with_tex",
    "no_tex",
)


def summarize(results: dict[str, GatherResult]) -> dict[str, int]:
    stats: Counter = Counter()
    for res in results.values():
        stats["versions"] += 1
        if res.root is None:
            stats["no_root_with_tex" if res.unused else "no_tex"] += 1
            continue
        stats["with_root"] += 1
        stats["multi_file"] += len(res.files) > 1
        stats["ambiguous_root"] += bool(res.other_roots)
        stats["with_unused_tex"] += bool(res.unused)
        stats["with_missing_tex"] += any(
            PurePosixPath(m).suffix in ("", ".tex") for m in res.missing
        )
    return {k: stats[k] for k in SUMMARY_KEYS}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Find the root .tex of each version.")
    parser.add_argument("path", type=Path, help="corpus root, paper folder or vN folder")
    parser.add_argument("--jsonl", type=Path, help="write one result per version to this file")
    args = parser.parse_args(argv)

    results = {v.as_posix(): gather(v) for v in iter_version_dirs(args.path)}
    if args.jsonl:
        with open(args.jsonl, "w", encoding="utf-8") as fh:
            for vdir, res in results.items():
                fh.write(
                    json.dumps({"version_dir": vdir, **asdict(res)}, ensure_ascii=False) + "\n"
                )
    print(json.dumps(summarize(results), indent=2))
    for vdir, res in results.items():
        if res.other_roots:
            print(f"{vdir}: root={res.root}  other={res.other_roots}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
