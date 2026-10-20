"""Pick a small, varied sample of crawled papers so M2 work can start before the full crawl.

    python -m src.parser.sample_set --data-root data/trial \\
        --work-dir work/trial-thien --work-dir work/trial-tien \\
        --work-dir work/trial-hieu --work-dir work/trial-dang --out data/m2-sample

Each paper is described by features over all its versions; the sample is filled
feature by feature (rarest first), alternating between the four ID ranges, then
topped up at random. The selected paper folders are copied to ``--out`` together
with ``manifest.json`` (why each paper was picked, coverage per feature).

Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""

from __future__ import annotations

import argparse
import json
import random
import re
import shutil
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from src import config
from src.parser.gather import TexVersion, paper_title
from src.run_pipeline import load_version_state
from src.utils.ids import NEW_ID_RE, from_folder_name

# Minimum number of sampled papers per feature, filled in this order.
QUOTAS = {
    "single_gz": 4,  # /src/ was one gzipped .tex, not a tar
    "ambiguous_root": 4,  # several files with \documentclass
    "no_tex_version": 4,  # at least one PDF-only / unavailable version (empty vN/)
    "bibitem": 10,  # \bibitem inside the .tex files (M2 bibitem -> BibTeX)
    "multi_version": 10,  # 2+ versions (M2 dedup across versions)
    "bib_file": 10,  # .bib file present
    "multi_file": 12,  # root \input/\include other .tex files
}
_BIBITEM_RE = re.compile(r"\\bibitem(?![A-Za-z@])")


@dataclass
class Paper:
    folder: Path
    arxiv_id: str
    range_owner: str | None
    features: set[str] = field(default_factory=set)


def range_owner(arxiv_id: str) -> str | None:
    def key(i: str) -> tuple[str, int]:
        m = NEW_ID_RE.match(i)
        return m.group("yymm"), int(m.group("num"))

    for name, (start, end) in config.RANGES.items():
        if key(start) <= key(arxiv_id) <= key(end):
            return name
    return None


def load_kinds(work_dirs: list[Path]) -> dict[tuple[str, int], str]:
    """(arxiv_id, version) -> source kind (tar.gz, gz, pdf...) from versions.jsonl files."""
    kinds = {}
    for work_dir in work_dirs:
        for key, rec in load_version_state(work_dir / "versions.jsonl").items():
            if rec.get("kind"):
                kinds[key] = rec["kind"]
    return kinds


def scan_paper(folder: Path, kinds: dict[tuple[str, int], str]) -> Paper:
    arxiv_id = from_folder_name(folder.name)
    paper = Paper(folder=folder, arxiv_id=arxiv_id, range_owner=range_owner(arxiv_id))
    title = paper_title(folder / "v1")
    versions = sorted(v for v in folder.iterdir() if v.is_dir() and re.fullmatch(r"v\d+", v.name))
    if len(versions) >= 2:
        paper.features.add("multi_version")
    for vdir in versions:
        if kinds.get((arxiv_id, int(vdir.name[1:]))) == "gz":
            paper.features.add("single_gz")
        if any(vdir.rglob("*.bib")):
            paper.features.add("bib_file")
        tex = TexVersion(vdir, title)
        if not tex.tex:
            paper.features.add("no_tex_version")
            continue
        res = tex.gather()
        if len(res.files) > 1:
            paper.features.add("multi_file")
        if res.other_roots:
            paper.features.add("ambiguous_root")
        if any(_BIBITEM_RE.search(tex.tex[f].code) for f in res.files):
            paper.features.add("bibitem")
    return paper


def select(papers: list[Paper], n: int, quotas: dict[str, int], seed: int) -> dict[str, str]:
    """arxiv_id -> reason ("feature name" or "random"), at most ``n`` papers."""
    rng = random.Random(seed)
    pool = sorted(papers, key=lambda p: p.arxiv_id)
    rng.shuffle(pool)
    picked: dict[str, str] = {}

    def round_robin(candidates: list[Paper]):
        by_range = defaultdict(list)
        for p in candidates:
            by_range[p.range_owner or ""].append(p)
        queues = [by_range[k] for k in sorted(by_range)]
        while any(queues):
            for q in queues:
                if q:
                    yield q.pop(0)

    for feature, quota in quotas.items():
        have = sum(feature in p.features for p in pool if p.arxiv_id in picked)
        for p in round_robin([p for p in pool if feature in p.features]):
            if have >= quota or len(picked) >= n:
                break
            if p.arxiv_id not in picked:
                picked[p.arxiv_id] = feature
                have += 1
    for p in round_robin([p for p in pool if p.arxiv_id not in picked]):
        if len(picked) >= n:
            break
        picked[p.arxiv_id] = "random"
    return picked


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Copy a varied sample of papers for M2.")
    parser.add_argument("--data-root", type=Path, required=True, help="crawled corpus")
    parser.add_argument("--work-dir", type=Path, action="append", default=[], help="repeatable")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("-n", type=int, default=50)
    parser.add_argument("--seed", type=int, default=2026)
    parser.add_argument("--force", action="store_true", help="replace an existing --out")
    args = parser.parse_args(argv)

    if args.out.exists() and any(args.out.iterdir()):
        if not args.force:
            raise SystemExit(f"{args.out} is not empty (use --force to replace it)")
        shutil.rmtree(args.out)
    kinds = load_kinds(args.work_dir)
    papers = [
        scan_paper(folder, kinds)
        for folder in sorted(args.data_root.iterdir())
        if folder.is_dir() and (folder / "metadata.json").exists()
    ]
    picked = select(papers, args.n, QUOTAS, args.seed)
    by_id = {p.arxiv_id: p for p in papers}
    args.out.mkdir(parents=True, exist_ok=True)
    for arxiv_id in picked:
        shutil.copytree(by_id[arxiv_id].folder, args.out / by_id[arxiv_id].folder.name)

    chosen = [by_id[i] for i in sorted(picked)]
    coverage = {f: sum(f in p.features for p in chosen) for f in QUOTAS}
    per_range = {r: sum(p.range_owner == r for p in chosen) for r in config.RANGES}
    manifest = {
        "source": args.data_root.as_posix(),
        "seed": args.seed,
        "coverage": coverage,
        "per_range": per_range,
        "papers": [
            {
                "arxiv_id": p.arxiv_id,
                "range": p.range_owner,
                "picked_for": picked[p.arxiv_id],
                "features": sorted(p.features),
            }
            for p in chosen
        ],
    }
    (args.out / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(f"{len(chosen)} papers -> {args.out}")
    print("coverage:", json.dumps(coverage), "\nper range:", json.dumps(per_range))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
