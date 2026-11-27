# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""End-to-end checks of the M3 labeling tools on a tiny synthetic corpus."""

import csv
import json

import pyarrow.parquet as pq
import pytest

from src.m3 import analysis, crosscheck, hf_dataset, label_tool, select_papers
from src.m3.labels import (
    label_path,
    load_bibtex,
    load_candidates,
    range_owner,
    read_matrix,
    write_matrix,
)
from src.parser.writer import to_table

PID = "2606.25801"  # tien's range
OTHER = "2604.20600"  # dang's range


def _corpus(tmp_path, n_entries=22):
    """Two papers; entry i of each paper cites candidate i (titles equal), odd keys also exist."""
    m2, corpus = tmp_path / "m2", tmp_path / "corpus"
    rows = []
    for pid in (PID, OTHER):
        refs = []
        for i in range(n_entries):
            title = f"Quantum widget study number {i} of lattices"
            refs.append(
                {
                    "title": title,
                    "authors": [f"Ann Author{i}"],
                    "submission_date": "2020-01-02",
                    "arxiv_id": f"2001.{i:05d}",
                    "s2_id": f"s{i}",
                }
            )
            # entry 0 has no candidate: a book
            rows.append(
                {
                    "arxiv_id": pid,
                    "citation_key": f"k{i:02d}",
                    "entry_type": "article",
                    "title": "A book on cooking" if i == 0 else title,
                    "authors": [f"Author{i}, Ann"],
                    "year": 2020,
                    "doi": None,
                    "journal": None,
                    "url": f"https://arxiv.org/abs/2001.{i:05d}" if i == 5 else None,
                }
            )
        refs.append({"title": "no arxiv id", "authors": [], "arxiv_id": None})
        refs.append(dict(refs[1]))  # duplicate arXiv ID, must be dropped
        folder = corpus / pid.replace(".", "-")
        folder.mkdir(parents=True)
        (folder / "references.json").write_text(json.dumps({"references": refs}), encoding="utf-8")
    (m2 / "shard-00000").mkdir(parents=True)
    pq.write_table(to_table("bibtex", rows), m2 / "shard-00000" / "bibtex.parquet")
    return m2, corpus


def _label_by_hand(m2, corpus, labels):
    """Simulates a person: picks the candidate whose title equals the entry title, else 0."""
    for pid in (PID, OTHER):
        entries = sorted(load_bibtex(m2, [pid]), key=lambda e: e["citation_key"])
        cands = load_candidates(corpus, pid)

        it = iter(entries)

        def ask(_prompt, it=it, cands=cands):
            e = next(it)
            ordered = label_tool.order_candidates(e, cands)[:15]
            hit = [n for n, c in enumerate(ordered, 1) if c["ref_title"] == e["title"]]
            return str(hit[0]) if hit else "0"

        label_tool.label_paper(entries, cands, label_path(labels, "manual", pid), ask=ask)


def test_loaders_and_ranges(tmp_path):
    m2, corpus = _corpus(tmp_path)
    assert len(load_bibtex(m2, [PID])) == 22
    cands = load_candidates(corpus, PID)
    assert len(cands) == 22 and len({c["ref_arxiv_id"] for c in cands}) == 22
    assert load_candidates(corpus, "2606.99999") == []
    assert (
        range_owner(PID) == "tien"
        and range_owner(OTHER) == "dang"
        and range_owner("2101.00001") is None
    )


def test_matrix_roundtrip_and_validation(tmp_path):
    path = tmp_path / "m.csv"
    write_matrix(path, ["a", "b"], {"k1": "b", "k2": None})
    assert read_matrix(path) == (["a", "b"], {"k1": "b", "k2": None})
    with pytest.raises(ValueError):
        write_matrix(path, ["a"], {"k1": "zz"})
    path.write_text("citation_key,a,b\nk1,yes,yes\n", encoding="utf-8")
    with pytest.raises(ValueError, match="at most one"):
        read_matrix(path)
    path.write_text("citation_key,a\nk1,maybe\n", encoding="utf-8")
    with pytest.raises(ValueError, match="yes/no"):
        read_matrix(path)


def test_label_tool_saves_resumes_and_goes_back(tmp_path):
    m2, corpus = _corpus(tmp_path)
    entries = sorted(load_bibtex(m2, [PID]), key=lambda e: e["citation_key"])
    cands = load_candidates(corpus, PID)
    path = label_path(tmp_path / "labels", "manual", PID)
    answers = iter(["0", "1", "b", "/number 1 of", "1", "q"])
    label_tool.label_paper(entries[:3], cands, path, ask=lambda _: next(answers))
    _, saved = read_matrix(path)
    assert saved == {"k00": None, "k01": "2001.00001"}
    # resume starts at k02
    seen = []
    label_tool.label_paper(entries[:3], cands, path, ask=lambda _: seen.append(1) or "0")
    assert len(seen) == 1 and read_matrix(path)[1]["k02"] is None


def test_select_analysis_crosscheck_hf(tmp_path, capsys):
    m2, corpus = _corpus(tmp_path)
    out = tmp_path / "sel.csv"
    select_papers.main(["--m2-dir", str(m2), "--corpus-dir", str(corpus), "--out", str(out)])
    chosen = list(csv.DictReader(out.open(encoding="utf-8")))
    assert {r["arxiv_id"] for r in chosen} == {PID, OTHER}
    assert all(r["n_title_hits"] == "21" for r in chosen)

    labels = tmp_path / "labels"
    _label_by_hand(m2, corpus, labels)
    adir = tmp_path / "an"
    analysis.main(
        [
            "--m2-dir",
            str(m2),
            "--corpus-dir",
            str(corpus),
            "--labels-dir",
            str(labels),
            "--out-dir",
            str(adir),
        ]
    )
    text = (adir / "summary.txt").read_text(encoding="utf-8")
    assert "entries with no correct ref: 2 (4.5%)" in text
    assert "title_sim baseline MRR@5  : 1.0000 on 42" in text
    pairs = list(csv.DictReader((adir / "pairs.csv").open(encoding="utf-8")))
    assert len(pairs) == 2 * 22 * 22
    assert any(p["id_in_entry"] == "1" and p["label"] == "yes" for p in pairs)

    sheet, key = tmp_path / "sheet.csv", tmp_path / "key.csv"
    crosscheck.main(
        [
            "sample",
            "--m2-dir",
            str(m2),
            "--corpus-dir",
            str(corpus),
            "--labels-dir",
            str(labels),
            "--sheet",
            str(sheet),
            "--key",
            str(key),
        ]
    )
    rows = list(csv.DictReader(sheet.open(encoding="utf-8-sig")))
    assert len(rows) == 20 and "first_label" not in rows[0]
    keys = {r["item"]: r for r in csv.DictReader(key.open(encoding="utf-8-sig"))}
    for r in rows:  # reviewer agrees everywhere except item 1
        k = keys[r["item"]]
        r["reviewer_choice"] = (
            str(k["choices"].split("|").index(k["first_label"]) + 1) if k["first_label"] else "0"
        )
    rows[0]["reviewer_choice"] = "0" if keys["1"]["first_label"] else "1"
    with sheet.open("w", newline="", encoding="utf-8-sig") as f:
        w = csv.DictWriter(f, fieldnames=crosscheck.SHEET)
        w.writeheader()
        w.writerows(rows)
    crosscheck.main(["score", "--sheet", str(sheet), "--key", str(key)])
    assert "overall  19/20 = 95%" in capsys.readouterr().out

    pkg = tmp_path / "pkg"
    for t in hf_dataset.TABLES:
        (pkg / t).mkdir(parents=True)
        pq.write_table(to_table("bibtex", load_bibtex(m2, [PID])), pkg / t / "part-0.parquet")
    (pkg / "README.md").write_text('load_dataset("<hf-user>/arxiv-nhom1-tien")', encoding="utf-8")
    hf_dataset.main(
        [
            "--package-dir",
            str(pkg),
            "--repo-id",
            "u/d",
            "--range",
            "tien",
            "--labels-dir",
            str(labels),
            "--dry-run",
        ]
    )
    assert (pkg / "labels" / "manual" / "2606-25801.csv").exists()
    assert not (pkg / "labels" / "manual" / "2604-20600.csv").exists()
    card = (pkg / "README.md").read_text(encoding="utf-8")
    assert "config_name: predictions" in card and "path: bibtex/*.parquet" in card
    assert 'load_dataset("u/d"' in card and "<hf-user>" not in card


def test_restore_inputs_copies_only_verified_references(tmp_path):
    import hashlib

    from src.m3.restore_inputs import restore_bibtex, restore_references

    m1, m2run = tmp_path / "m1", tmp_path / "m2run"
    remote = m1 / "runs" / "r1"
    good = b'{"references": []}'
    sha = hashlib.sha256(good).hexdigest()
    (remote / "data/corpus/2606-25801").mkdir(parents=True)
    (remote / "data/corpus/2606-25801/references.json").write_bytes(b"newer, changed")
    (remote / ".checkpoint-objects" / sha[:2]).mkdir(parents=True)
    (remote / ".checkpoint-objects" / sha[:2] / sha).write_bytes(good)  # verified fallback
    files = {
        "data/corpus/2606-25801/references.json": sha,
        "data/corpus/2606-25801/v1/main.tex": "0" * 64,  # not copied
        "work/versions.jsonl": "0" * 64,
    }
    (remote / "INDEX.json").write_text(json.dumps({"files": files}), encoding="utf-8")
    (m2run / "out/shard-00000").mkdir(parents=True)
    (m2run / "out/shard-00000/bibtex.parquet").write_bytes(b"pq")
    (m2run / "M2_MANIFEST.json").write_text(
        json.dumps({"runs": {"r1": "runs/r1"}}), encoding="utf-8"
    )

    corpus = tmp_path / "corpus"
    assert restore_references(m2run, m1, corpus) == {"copied": 1, "reused": 0, "total": 1}
    assert (corpus / "2606-25801/references.json").read_bytes() == good
    assert not (corpus / "2606-25801/v1").exists()
    assert restore_references(m2run, m1, corpus)["reused"] == 1
    assert restore_bibtex(m2run, tmp_path / "out") == {"copied": 1, "reused": 0, "total": 1}

    (remote / ".checkpoint-objects" / sha[:2] / sha).write_bytes(b"bad")
    (corpus / "2606-25801/references.json").unlink()
    with pytest.raises(ValueError, match="indexed hash"):
        restore_references(m2run, m1, corpus)


def test_crosscheck_auto_audit_groups_and_overlap_guard(tmp_path, capsys):
    m2, corpus = _corpus(tmp_path)
    labels = tmp_path / "labels"
    cand_ids = [c["ref_arxiv_id"] for c in load_candidates(corpus, OTHER)]
    auto = {f"k{i:02d}": (None if i < 5 else f"2001.{i:05d}") for i in range(22)}
    write_matrix(label_path(labels, "auto", OTHER), cand_ids, auto)
    sel = tmp_path / "selection.csv"
    sel.write_text(f"range_owner,arxiv_id\ntien,{PID}\n", encoding="utf-8")
    sheet, key = tmp_path / "sheet.csv", tmp_path / "key.csv"
    base = ["sample", "--method", "auto", "--m2-dir", str(m2), "--corpus-dir", str(corpus),
            "--labels-dir", str(labels), "--selection-csv", str(sel), "--per-labeler", "4",
            "--sheet", str(sheet), "--key", str(key)]  # fmt: skip
    crosscheck.main(base)
    rows = list(csv.DictReader(sheet.open(encoding="utf-8-sig")))
    assert sorted(r["labeler"] for r in rows) == ["auto-match"] * 4 + ["auto-none"] * 4
    assert "none in the manual set" in capsys.readouterr().out

    write_matrix(label_path(labels, "auto", PID), cand_ids, {})  # PID is in selection.csv
    with pytest.raises(SystemExit, match="both auto and manual"):
        crosscheck.main(base)
