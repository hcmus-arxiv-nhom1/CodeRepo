# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
import json

from src.parser.sample_set import Paper, main, range_owner, select
from src.run_pipeline import JsonlWriter

DOC = b"\\documentclass{article}\\begin{document}%s\\end{document}"


def paper(corpus, folder, versions):
    """versions: list of {relative path: bytes}; an empty dict is a PDF-only version."""
    root = corpus / folder
    for n, files in enumerate(versions, 1):
        (root / f"v{n}").mkdir(parents=True)
        for rel, data in files.items():
            (root / f"v{n}" / rel).parent.mkdir(parents=True, exist_ok=True)
            (root / f"v{n}" / rel).write_bytes(data)
    (root / "metadata.json").write_text(json.dumps({"title": "T"}), encoding="utf-8")


def test_range_owner():
    assert range_owner("2606.32040") == "thien" and range_owner("2607.00001") == "thien"
    assert range_owner("2604.20535") == "dang" and range_owner("2601.00001") is None


def test_select_fills_quotas_then_tops_up():
    papers = [Paper(folder=None, arxiv_id=f"2606.0000{i}", range_owner="hieu") for i in range(6)]
    papers[3].features = {"single_gz"}
    papers[4].features = {"bibitem"}
    picked = select(papers, n=3, quotas={"single_gz": 1, "bibitem": 1}, seed=0)
    assert picked["2606.00003"] == "single_gz" and picked["2606.00004"] == "bibitem"
    assert len(picked) == 3 and list(picked.values()).count("random") == 1


def test_cli_copies_sample_with_manifest(tmp_path):
    corpus, work = tmp_path / "corpus", tmp_path / "work"
    paper(corpus, "2606-05734", [{"main.tex": DOC % b"\\input{a}", "a.tex": b"A", "r.bib": b""}])
    paper(corpus, "2606-25734", [{"main.tex": DOC % b"\\bibitem{x} X"}, {}])
    paper(corpus, "2606-30734", [{"arXiv-2606.30734v1.tex": DOC % b"single"}])
    work.mkdir()
    log = JsonlWriter(work / "versions.jsonl")
    log.write({"arxiv_id": "2606.30734", "version": 1, "status": "ok", "kind": "gz"})
    log.close()

    out = tmp_path / "sample"
    assert main(["--data-root", str(corpus), "--work-dir", str(work), "--out", str(out)]) == 0
    manifest = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    features = {p["arxiv_id"]: set(p["features"]) for p in manifest["papers"]}
    assert features == {
        "2606.05734": {"multi_file", "bib_file"},
        "2606.25734": {"bibitem", "multi_version", "no_tex_version"},
        "2606.30734": {"single_gz"},
    }
    assert (out / "2606-05734" / "v1" / "a.tex").exists()
    assert manifest["per_range"] == {"thien": 1, "tien": 1, "hieu": 1, "dang": 0}
