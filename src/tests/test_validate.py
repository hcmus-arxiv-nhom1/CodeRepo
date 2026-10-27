# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
import csv
import json

from src.qa.validate import (
    check_metadata,
    load_not_found,
    load_versions_log,
    main,
    validate_paper,
    validate_range,
)

ID = "2606.25734"


def meta(**over):
    base = {
        "arxiv_id": ID,
        "title": "A title",
        "authors": ["Ada Lovelace"],
        "submission_date": "2026-06-25T10:00:00Z",
        "revised_dates": ["2026-06-27T10:00:00Z"],
        "venue": None,
        "num_versions": 2,
        "categories": ["cs.LG"],
        "doi": None,
    }
    return {**base, **over}


def make_paper(root, arxiv_id=ID, metadata=None, versions=("v1", "v2")):
    folder = root / arxiv_id.replace(".", "-")
    folder.mkdir(parents=True)
    if metadata is not False:
        (folder / "metadata.json").write_text(json.dumps(metadata or meta(arxiv_id=arxiv_id)))
    for v in versions:
        (folder / v).mkdir()
        (folder / v / "main.tex").write_text("\\documentclass{article}\n")
    return folder


def checks(issues):
    return sorted({i.check for i in issues})


def test_clean_paper_has_no_issue(tmp_path):
    make_paper(tmp_path)
    assert validate_paper(ID, tmp_path, owner="tien") == []


def test_missing_folder(tmp_path):
    assert checks(validate_paper(ID, tmp_path, owner="tien")) == ["missing_dir"]


def test_metadata_missing_and_invalid_json(tmp_path):
    folder = make_paper(tmp_path, metadata=False)
    assert "metadata_missing" in checks(validate_paper(ID, tmp_path, owner="tien"))
    (folder / "metadata.json").write_text("{not json")
    assert "metadata_schema" in checks(validate_paper(ID, tmp_path, owner="tien"))


def test_metadata_schema_problems():
    assert check_metadata(meta(), ID) == []
    assert check_metadata(meta(arxiv_id="2606.00001"), ID)
    assert check_metadata(meta(title=" "), ID)
    assert check_metadata(meta(authors=[]), ID)
    assert check_metadata(meta(submission_date="2026-06-25"), ID)
    assert check_metadata(meta(revised_dates=["nope"]), ID)
    assert check_metadata(meta(num_versions=3), ID)  # 1 + len(revised_dates) == 2
    assert check_metadata(meta(venue=5), ID)
    assert check_metadata([], ID) == ["not a JSON object"]


def test_missing_and_extra_version_dirs(tmp_path):
    make_paper(tmp_path, versions=("v1", "v3"))
    found = validate_paper(ID, tmp_path, owner="tien")
    assert {(i.check, i.version) for i in found} == {
        ("version_missing", 2),
        ("version_extra", 3),
    }


def test_binary_and_foreign_files_are_caught(tmp_path):
    folder = make_paper(tmp_path)
    (folder / "v1" / "fig.png").write_bytes(b"\x89PNG\r\n\x1a\n")
    (folder / "v2" / "evil.tex").write_bytes(b"\\input{a}\x00\x01")
    expected = 2
    try:
        (folder / "v2" / "link.tex").symlink_to(folder / "v2" / "main.tex")
        expected += 1
    except OSError:  # Windows without the symlink privilege
        pass
    found = validate_paper(ID, tmp_path, owner="tien")
    assert [i.check for i in found].count("binary_file") == expected


def test_empty_tex_is_only_a_warning(tmp_path):
    folder = make_paper(tmp_path)
    (folder / "v1" / "main.tex").write_text("  \n")
    (found,) = validate_paper(ID, tmp_path, owner="tien")
    assert (found.check, found.severity) == ("tex_empty", "warning")


def test_status_checks_with_versions_log(tmp_path):
    folder = make_paper(tmp_path)
    (folder / "v2" / "main.tex").unlink()  # v2 claims ok but is empty
    log = {
        (ID, 1): {"arxiv_id": ID, "version": 1, "status": "no_tex"},  # but has a .tex file
        (ID, 2): {"arxiv_id": ID, "version": 2, "status": "ok"},
    }
    found = validate_paper(ID, tmp_path, owner="tien", versions_log=log)
    assert [(i.check, i.version) for i in found] == [
        ("status_mismatch", 1),
        ("status_mismatch", 2),
    ]
    log[(ID, 2)]["status"] = "failed"
    log.pop((ID, 1))
    found = validate_paper(ID, tmp_path, owner="tien", versions_log=log)
    assert {(i.check, i.version) for i in found} == {("status_missing", 1), ("status_failed", 2)}


def test_references_json(tmp_path):
    folder = make_paper(tmp_path)
    assert checks(validate_paper(ID, tmp_path, owner="t", require_references=True)) == [
        "references_missing"
    ]
    (folder / "references.json").write_text("[1, 2")
    assert checks(validate_paper(ID, tmp_path, owner="t")) == ["references_schema"]
    ok = {"status": "ok", "num_references": 0, "num_arxiv_references": 0, "references": []}
    (folder / "references.json").write_text(json.dumps(ok))  # spec 0001 shape
    assert validate_paper(ID, tmp_path, owner="t", require_references=True) == []


def test_unexpected_folder_is_reported(tmp_path):
    make_paper(tmp_path)
    make_paper(tmp_path, arxiv_id="2606.99999")
    issues, checked = validate_range([ID], tmp_path, owner="tien", scan_extra=True)
    assert checked == 1
    assert [(i.check, i.arxiv_id) for i in issues] == [("unexpected_dir", "2606.99999")]


def test_load_versions_log_keeps_latest_and_skips_torn_line(tmp_path):
    path = tmp_path / "versions.jsonl"
    path.write_text(
        json.dumps({"arxiv_id": ID, "version": 1, "status": "failed"})
        + "\n"
        + json.dumps({"arxiv_id": ID, "version": 1, "status": "ok"})
        + '\n{"arxiv_id": "2606.'
    )
    assert load_versions_log(path)[(ID, 1)]["status"] == "ok"


def test_cli_writes_report_and_exit_code(tmp_path, capsys):
    data, work = tmp_path / "corpus", tmp_path / "work"
    make_paper(data)
    (data / "2606-25734" / "v1" / "x.png").write_bytes(b"png")
    argv = [
        "--start", ID, "--end", "2606.25735", "--no-require-versions-log",
        "--data-root", str(data), "--work-dir", str(work),
    ]  # fmt: skip
    assert main(argv) == 1  # x.png + 2606.25735 missing
    rows = list(csv.DictReader(open(work / "qa_report.csv", encoding="utf-8")))
    assert {(r["arxiv_id"], r["check"]) for r in rows} == {
        (ID, "binary_file"),
        ("2606.25735", "missing_dir"),
    }
    assert rows[0]["range_owner"] == "custom"
    assert json.loads(capsys.readouterr().out)["papers_with_errors"] == 2


def test_references_follow_spec_0001(tmp_path):
    folder = make_paper(tmp_path)
    ref = {"s2_id": "x", "title": "T", "authors": ["A"], "submission_date": "2020", "arxiv_id": "1"}
    rec = {"status": "ok", "num_references": 2, "num_arxiv_references": 1, "references": [ref]}
    (folder / "references.json").write_text(json.dumps(rec))
    found = [i.detail for i in validate_paper(ID, tmp_path, owner="t")]
    assert found == ["num_references != len(references)"]
    rec |= {"num_references": 1, "references": [{"title": "no other field"}]}
    (folder / "references.json").write_text(json.dumps(rec))
    details = [i.detail for i in validate_paper(ID, tmp_path, owner="t")]
    assert any("miss one of" in d for d in details)


def test_per_version_metadata_is_checked():
    dates = [meta()["submission_date"], *meta()["revised_dates"]]
    good = [
        {"version": k, "date": d, "title": "T", "authors": ["A"]} for k, d in enumerate(dates, 1)
    ]
    assert check_metadata(meta(versions=good), ID) == []
    assert check_metadata(meta(), ID) == []  # metadata written before the field existed
    bad = [dict(v) for v in good]
    bad[0]["date"] = "2020-01-01T00:00:00Z"
    assert check_metadata(meta(versions=bad), ID)
    assert check_metadata(meta(versions=good[:1]), ID)  # one entry missing


def test_id_unknown_to_arxiv_is_only_a_warning(tmp_path):
    found = validate_paper(ID, tmp_path, owner="t", not_on_arxiv=frozenset({ID}))
    assert [(i.check, i.severity) for i in found] == [("not_on_arxiv", "warning")]
    log = tmp_path / "discovery.jsonl"
    records = [
        {"arxiv_id": ID, "status": "missing", "reason": "not_found"},
        {"arxiv_id": "2606.25735", "status": "found"},
    ]
    log.write_text("".join(json.dumps(r) + "\n" for r in records))
    assert load_not_found(log) == frozenset({ID})
