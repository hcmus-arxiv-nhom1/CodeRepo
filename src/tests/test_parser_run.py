# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
import csv
import json
import signal

import pytest

from src.crawler.worklock import WorkDirLock, WorkDirLockedError
from src.parser.run import (
    STAGES,
    ChangedCorpusError,
    Options,
    StageError,
    main,
    process_paper,
    run,
    select_papers,
)
from src.utils.ids import to_folder_name

DOC = "\\documentclass{article}\\begin{document}%s\\end{document}"
BIBITEM = "\\begin{thebibliography}{9}\\bibitem{k1} A. One. A title. 2001.\\end{thebibliography}"
FAKE = Options(stages={name: f"src.tests.m2_stages:{name}" for name in STAGES})


def make_paper(root, arxiv_id, versions, title="A title"):
    """``versions``: one entry per version; a dict of files, ``{}`` (empty record) or None."""
    folder = root / to_folder_name(arxiv_id)
    folder.mkdir(parents=True)
    meta = {"arxiv_id": arxiv_id, "title": title, "num_versions": len(versions)}
    (folder / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")
    for number, files in enumerate(versions, 1):
        if files is None:
            continue
        (folder / f"v{number}").mkdir()
        for rel, text in files.items():
            (folder / f"v{number}" / rel).write_text(text, encoding="utf-8")


@pytest.fixture
def corpus(tmp_path):
    root = tmp_path / "corpus"
    make_paper(
        root,
        "2606.00001",
        [{"main.tex": DOC % "First. Second."}, {"main.tex": DOC % ("First. Third." + BIBITEM)}],
    )
    make_paper(root, "2606.00002", [{}, {"main.tex": DOC % "Only v2."}])  # v1: empty record
    make_paper(root, "2606.00003", [{"notes.tex": "no document class"}])
    make_paper(root, "2606.00004", [{"main.tex": DOC % "Present."}, None])  # v2 never crawled
    make_paper(root, "2606.00005", [{"main.tex": DOC % "BOOM."}, {"main.tex": DOC % "Fine."}])
    return root


def go(corpus, tmp_path, name="a", options=FAKE, **kwargs):
    kwargs.setdefault("progress", False)
    return run(
        corpus, tmp_path / f"out-{name}", tmp_path / f"work-{name}", options=options, **kwargs
    )


def read_rows(out_dir, table):
    rows = []
    for shard in sorted(out_dir.glob("shard-*")):
        text = (shard / f"{table}.jsonl").read_text(encoding="utf-8")
        rows += [json.loads(line) for line in text.splitlines()]
    return rows


def read_csv(path):
    with open(path, encoding="utf-8", newline="") as fh:
        return list(csv.DictReader(fh))


def statuses(work_dir):
    rows = read_csv(work_dir / "m2_versions.csv")
    return {(r["arxiv_id"], int(r["version"])): r["status"] for r in rows}


def journal(work_dir):
    lines = (work_dir / "shards.jsonl").read_text(encoding="utf-8").splitlines()
    return [json.loads(line) for line in lines]


# ------------------------------------------------------------------ outcome of each version


def test_every_version_gets_a_status_and_none_is_skipped(corpus, tmp_path):
    summary = go(corpus, tmp_path)
    assert statuses(tmp_path / "work-a") == {
        ("2606.00001", 1): "parsed",
        ("2606.00001", 2): "parsed",
        ("2606.00002", 1): "empty",  # the M1 empty record is recorded, not skipped
        ("2606.00002", 2): "parsed",
        ("2606.00003", 1): "no_root",
        ("2606.00004", 1): "parsed",
        ("2606.00004", 2): "missing",  # no v2/ folder at all
        ("2606.00005", 1): "error",  # BOOM: that version fails, its sibling does not
        ("2606.00005", 2): "parsed",
    }
    assert summary["papers_this_run"] == 5 and summary["papers_failed_this_run"] == 0
    assert summary["version_status_this_run"]["error"] == 1


def test_dedup_sees_empty_versions_but_not_missing_ones(corpus, tmp_path):
    go(corpus, tmp_path)
    hierarchy = read_rows(tmp_path / "out-a", "hierarchy")
    roots = {(r["arxiv_id"], r["version"]) for r in hierarchy if "#root" in r["element_id"]}
    assert ("2606.00002", 1) in roots  # empty: a root and nothing under it
    assert ("2606.00003", 1) in roots and ("2606.00005", 1) in roots
    assert ("2606.00004", 2) not in roots
    assert not [r for r in hierarchy if r["arxiv_id"] == "2606.00002" and r["version"] == 1][1:]


def test_sentences_shared_between_versions_get_one_element_and_references_come_through(
    corpus, tmp_path
):
    go(corpus, tmp_path)
    elements = read_rows(tmp_path / "out-a", "elements")
    first = [e for e in elements if e["arxiv_id"] == "2606.00001" and e["content"] == "First"]
    assert len(first) == 1
    bibtex = read_rows(tmp_path / "out-a", "bibtex")
    assert [(b["arxiv_id"], b["citation_key"], b["year"]) for b in bibtex] == [
        ("2606.00001", "k1", 2001)
    ]


def test_crawler_status_explains_an_empty_version(corpus, tmp_path):
    crawl = tmp_path / "crawl"
    crawl.mkdir()
    record = {"arxiv_id": "2606.00002", "version": 1, "status": "unavailable", "kind": None}
    (crawl / "versions.jsonl").write_text(json.dumps(record) + "\n", encoding="utf-8")
    go(corpus, tmp_path, crawl_work_dir=crawl)
    rows = read_csv(tmp_path / "work-a" / "m2_versions.csv")
    empty = next(r for r in rows if r["status"] == "empty")
    assert (empty["crawl_status"], empty["reason"]) == ("unavailable", "unavailable")


def test_a_failing_dedup_loses_that_paper_only_and_retry_errors_recovers_it(
    corpus, tmp_path, monkeypatch
):
    make_paper(corpus, "2606.00099", [{"main.tex": DOC % "Late one."}])
    monkeypatch.setenv("M2_FAKE_DEDUP_FAIL", "1")
    summary = go(corpus, tmp_path)
    assert summary["papers_failed_this_run"] == 1
    papers = {r["arxiv_id"]: r for r in read_csv(tmp_path / "work-a" / "m2_papers.csv")}
    assert (
        "dedup failed" in papers["2606.00099"]["error"] and papers["2606.00099"]["elements"] == "0"
    )
    assert not [
        e for e in read_rows(tmp_path / "out-a", "elements") if e["arxiv_id"] == "2606.00099"
    ]

    monkeypatch.delenv("M2_FAKE_DEDUP_FAIL")
    assert go(corpus, tmp_path)["papers_this_run"] == 0  # committed, even if it failed
    assert go(corpus, tmp_path, retry_errors=True)["papers_this_run"] == 1
    papers = {r["arxiv_id"]: r for r in read_csv(tmp_path / "work-a" / "m2_papers.csv")}
    assert papers["2606.00099"]["error"] == "" and papers["2606.00099"]["elements"] == "2"
    late = [e for e in read_rows(tmp_path / "out-a", "elements") if e["arxiv_id"] == "2606.00099"]
    assert len(late) == 2  # root + sentence, once


# ------------------------------------------------------------------ stop and run again


def snapshot(out_dir):
    return {t: read_rows(out_dir, t) for t in ("elements", "hierarchy", "bibtex")}


def test_resume_after_a_crash_gives_the_same_tables_as_an_uninterrupted_run(
    corpus, tmp_path, monkeypatch
):
    go(corpus, tmp_path, name="ref", shard_papers=2)

    monkeypatch.setenv("M2_FAKE_FAIL_SHARD", "shard-00001")
    with pytest.raises(OSError, match="disk full"):
        go(corpus, tmp_path, shard_papers=2)
    assert len(journal(tmp_path / "work-a")) == 1  # shard 0 committed, shard 1 not
    monkeypatch.delenv("M2_FAKE_FAIL_SHARD")

    summary = go(corpus, tmp_path, shard_papers=2)
    assert summary["papers_this_run"] == 3  # the 2 committed papers are not parsed again
    assert snapshot(tmp_path / "out-a") == snapshot(tmp_path / "out-ref")
    assert not (tmp_path / "out-a" / ".staging").exists()
    ids = [r["arxiv_id"] for line in journal(tmp_path / "work-a") for r in line["papers"]]
    assert sorted(ids) == sorted(set(ids)) == [f"2606.0000{n}" for n in range(1, 6)]


def test_a_shard_folder_without_a_journal_line_and_a_torn_line_are_discarded(corpus, tmp_path):
    go(corpus, tmp_path, name="ref", shard_papers=2)
    go(corpus, tmp_path, shard_papers=2)

    work, out = tmp_path / "work-a", tmp_path / "out-a"
    lines = (work / "shards.jsonl").read_text(encoding="utf-8").splitlines(keepends=True)
    torn = lines[-1][: len(lines[-1]) // 2]  # killed halfway through the last commit
    (work / "shards.jsonl").write_text("".join(lines[:-1]) + torn, encoding="utf-8")
    assert (out / "shard-00002").exists()  # renamed, but never journaled

    summary = go(corpus, tmp_path, shard_papers=2)
    assert summary["papers_this_run"] == 1
    assert snapshot(out) == snapshot(tmp_path / "out-ref")


def test_shard_size_follows_papers_and_rows(corpus, tmp_path):
    go(corpus, tmp_path, name="p", shard_papers=2)
    assert [len(s["papers"]) for s in journal(tmp_path / "work-p")] == [2, 2, 1]
    go(corpus, tmp_path, name="r", shard_rows=1)
    assert [len(s["papers"]) for s in journal(tmp_path / "work-r")] == [1] * 5


def test_ctrl_c_keeps_the_papers_already_parsed(corpus, tmp_path, monkeypatch):
    import src.parser.run as run_module

    real = run_module.process_paper
    calls = []

    def interrupting(path, options, crawl=None):
        calls.append(path.name)
        if len(calls) == 3:
            raise KeyboardInterrupt
        return real(path, options, crawl)

    monkeypatch.setattr(run_module, "process_paper", interrupting)
    with pytest.raises(KeyboardInterrupt):
        go(corpus, tmp_path, shard_papers=10)
    assert [len(s["papers"]) for s in journal(tmp_path / "work-a")] == [2]
    monkeypatch.setattr(run_module, "process_paper", real)
    assert go(corpus, tmp_path)["papers_this_run"] == 3


def test_verify_done_refuses_a_corpus_that_changed(corpus, tmp_path):
    go(corpus, tmp_path)
    (corpus / "2606-00004" / "v2").mkdir()
    (corpus / "2606-00004" / "v2" / "main.tex").write_text(DOC % "Arrived late.", encoding="utf-8")
    assert go(corpus, tmp_path)["papers_this_run"] == 0  # not noticed unless asked
    with pytest.raises(ChangedCorpusError, match="2606-00004"):
        go(corpus, tmp_path, verify_done=True)


# ------------------------------------------------------------------ workers, lock, stages


def test_two_workers_give_the_same_shards_as_one(corpus, tmp_path):
    go(corpus, tmp_path, name="one", shard_papers=2)
    go(corpus, tmp_path, name="two", shard_papers=2, workers=2)
    assert snapshot(tmp_path / "out-one") == snapshot(tmp_path / "out-two")
    assert [s["papers"][0]["arxiv_id"] for s in journal(tmp_path / "work-two")] == [
        "2606.00001",
        "2606.00003",
        "2606.00005",
    ]


def test_second_run_on_the_same_work_dir_is_refused(corpus, tmp_path):
    lock = WorkDirLock(tmp_path / "work-a")
    lock.acquire()
    try:
        with pytest.raises(WorkDirLockedError):
            go(corpus, tmp_path)
        argv = ["--data-root", str(corpus), "--out-dir", str(tmp_path / "out-a")]
        for name, spec in FAKE.stages.items():
            argv += ["--stage", f"{name}={spec}"]
        assert main([*argv, "--work-dir", str(tmp_path / "work-a"), "--no-progress"]) == 2
    finally:
        lock.release()


def test_a_missing_stage_stops_before_anything_is_written(corpus, tmp_path, capsys):
    argv = [
        "--data-root", str(corpus),
        "--out-dir", str(tmp_path / "out"),
        "--work-dir", str(tmp_path / "work"),
        "--stage", "dedup=src.parser.not_written_yet:dedup",
        "--no-progress",
    ]  # fmt: skip
    assert main(argv) == 3
    assert "not_written_yet" in capsys.readouterr().err
    assert not (tmp_path / "out").exists() and not (tmp_path / "work" / "shards.jsonl").exists()
    with pytest.raises(StageError):
        go(corpus, tmp_path, options=Options(stages={**FAKE.stages, "dedup": "no.such:fn"}))


def test_scan_runs_gather_and_references_only(corpus, tmp_path):
    summary = go(corpus, tmp_path, options=Options(scan=True))
    assert summary["scan_only"] and not (tmp_path / "out-a").exists()
    rows = read_csv(tmp_path / "work-a" / "m2_versions.csv")
    parsed = next(r for r in rows if r["arxiv_id"] == "2606.00001" and r["version"] == "2")
    assert (parsed["root"], parsed["n_bibitems"]) == ("main.tex", "1")
    assert statuses(tmp_path / "work-a")[("2606.00005", 1)] == "parsed"  # BOOM is a tree error


def test_selection_by_ids_offset_and_limit(corpus):
    papers = sorted(p for p in corpus.iterdir())
    names = lambda ps: [p.name for p in ps]  # noqa: E731
    assert names(select_papers(papers, ["2606.00003", "2606.00001"], 0, None)) == [
        "2606-00001",
        "2606-00003",
    ]
    assert names(select_papers(papers, None, 1, 2)) == ["2606-00002", "2606-00003"]


def test_empty_corpus_and_folders_without_metadata(tmp_path):
    (tmp_path / "corpus" / "2606-00001").mkdir(parents=True)  # no metadata.json: not a paper
    summary = go(tmp_path / "corpus", tmp_path)
    assert summary["papers_this_run"] == 0 and summary["shards_written_this_run"] == 0


def test_unreadable_metadata_is_an_error_record_not_a_crash(tmp_path):
    root = tmp_path / "corpus"
    make_paper(root, "2606.00001", [{"main.tex": DOC % "Fine."}])
    (root / "2606-00001" / "metadata.json").write_text("{not json", encoding="utf-8")
    outcome = process_paper(root / "2606-00001", FAKE)
    assert outcome.error and outcome.versions == []


@pytest.mark.skipif(not hasattr(signal, "SIGALRM"), reason="paper timeout needs POSIX signals")
def test_paper_timeout_turns_a_stuck_paper_into_an_error(tmp_path):
    root = tmp_path / "corpus"
    make_paper(root, "2606.00001", [{"main.tex": DOC % "SLOW."}])
    make_paper(root, "2606.00002", [{"main.tex": DOC % "Quick."}])
    summary = go(root, tmp_path, options=Options(stages=FAKE.stages, paper_timeout_s=1))
    assert summary["papers_failed_this_run"] == 1  # only the stuck paper; the run goes on
    papers = {r["arxiv_id"]: r for r in read_csv(tmp_path / "work-a" / "m2_papers.csv")}
    assert "timeout" in papers["2606.00001"]["error"] and papers["2606.00002"]["error"] == ""


def test_runner_uses_bbl_for_matching_version_only(corpus, tmp_path):
    bbl = tmp_path / "recovered.bbl"
    bbl.write_text(r"\bibitem{recovered} A. Author. Title. 2020.", encoding="utf8")
    options = Options(stages=FAKE.stages, bbl_index={("2606.00001", 1): [bbl]})
    go(corpus, tmp_path, options=options)
    rows = read_rows(tmp_path / "out-a", "bibtex")
    assert {r["citation_key"] for r in rows} == {"recovered", "k1"}


def test_cli_accepts_external_recovery_option():
    from src.parser.run import build_parser

    args = build_parser().parse_args(["--bbl-recovery-dir", "recovery"])
    assert args.bbl_recovery_dir.name == "recovery"


def test_cli_runs_with_verified_external_bbl(corpus, tmp_path):
    import hashlib

    recovery = tmp_path / "recovery"
    file = recovery / "files/2606-00001/v1/main.bbl"
    file.parent.mkdir(parents=True)
    content = b"\\bibitem{outside} A. Author. Title. 2020."
    file.write_bytes(content)
    record = {
        "arxiv_id": "2606.00001",
        "version": 1,
        "status": "recovered",
        "files": [
            {
                "path": "files/2606-00001/v1/main.bbl",
                "bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        ],
    }
    (recovery / "versions.jsonl").write_text(json.dumps(record) + "\n", encoding="utf8")
    result = main(
        [
            "--data-root",
            str(corpus),
            "--out-dir",
            str(tmp_path / "out"),
            "--work-dir",
            str(tmp_path / "work"),
            "--scan",
            "--no-progress",
            "--bbl-recovery-dir",
            str(recovery),
        ]
    )
    assert result == 0
    rows = read_csv(tmp_path / "work/m2_versions.csv")
    assert (
        next(r for r in rows if r["arxiv_id"] == "2606.00001" and r["version"] == "1")["n_bbl"]
        == "1"
    )


def test_scan_refuses_to_resume_recovery_run_without_bbl_pointer(corpus, tmp_path):
    work = tmp_path / "work"
    work.mkdir()
    (work / "bbl-input-identity.json").write_text('{"journal_sha256":"abc"}', encoding="utf8")
    with pytest.raises(ValueError, match="BBL input"):
        main(["--data-root", str(corpus), "--work-dir", str(work), "--scan", "--no-progress"])


def test_external_scan_detects_corpus_edit_on_resume(corpus, tmp_path):
    import hashlib

    recovery = tmp_path / "recovery"
    file = recovery / "files/2606-00001/v1/main.bbl"
    file.parent.mkdir(parents=True)
    content = b"\\bibitem{outside} A. Author. Title. 2020."
    file.write_bytes(content)
    record = {
        "arxiv_id": "2606.00001",
        "version": 1,
        "status": "recovered",
        "files": [
            {
                "path": "files/2606-00001/v1/main.bbl",
                "bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        ],
    }
    (recovery / "versions.jsonl").write_text(json.dumps(record) + "\n", encoding="utf8")
    args = [
        "--data-root",
        str(corpus),
        "--out-dir",
        str(tmp_path / "out"),
        "--work-dir",
        str(tmp_path / "work"),
        "--scan",
        "--no-progress",
        "--bbl-recovery-dir",
        str(recovery),
    ]
    assert main(args) == 0
    tex = corpus / "2606-00001/v1/main.tex"
    data = tex.read_bytes()
    tex.write_bytes(data.replace(b"First", b"Other"))
    with pytest.raises(ValueError, match="corpus input changed"):
        main(args)


def test_workers_receive_only_their_papers_external_bbl(corpus, tmp_path, monkeypatch):
    import src.parser.run as runner

    file = tmp_path / "external.bbl"
    file.write_text(r"\bibitem{k} Author. Title. 2020.")
    index = {("2606.00001", 1): [file], ("2606.00002", 2): [file]}
    original = runner.process_paper
    seen = []

    def observe(path, options, crawl=None):
        seen.append((path.name, set(options.bbl_index)))
        return original(path, options, crawl)

    monkeypatch.setattr(runner, "process_paper", observe)
    go(corpus, tmp_path, options=Options(stages=FAKE.stages, bbl_index=index))
    assert dict(seen)["2606-00001"] == {("2606.00001", 1)}
    assert dict(seen)["2606-00002"] == {("2606.00002", 2)}
    assert dict(seen)["2606-00003"] == set()


def test_committed_paper_rows_are_released_during_run(corpus, tmp_path, monkeypatch):
    import weakref

    import src.parser.run as runner

    original = runner.process_paper
    first = None
    released = []

    def observe(*args, **kwargs):
        nonlocal first
        if first is not None:
            released.append(first() is None)
        outcome = original(*args, **kwargs)
        if first is None:
            first = weakref.ref(outcome)
        return outcome

    monkeypatch.setattr(runner, "process_paper", observe)
    summary = go(corpus, tmp_path, shard_papers=1)
    assert any(released), "committed outcomes remained alive until the run ended"
    assert summary["papers_this_run"] == 5
    assert summary["version_status_this_run"]["error"] == 1


def test_cli_parses_external_bbl_into_parquet(corpus, tmp_path):
    import hashlib

    import pyarrow.parquet as pq

    recovery = tmp_path / "recovery"
    file = recovery / "files/2606-00001/v1/main.bbl"
    file.parent.mkdir(parents=True)
    content = b"\\bibitem{outside} A. Author. Title. 2020."
    file.write_bytes(content)
    record = {
        "arxiv_id": "2606.00001",
        "version": 1,
        "status": "recovered",
        "files": [
            {
                "path": "files/2606-00001/v1/main.bbl",
                "bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        ],
    }
    (recovery / "versions.jsonl").write_text(json.dumps(record) + "\n", encoding="utf8")
    result = main(
        [
            "--data-root",
            str(corpus),
            "--out-dir",
            str(tmp_path / "out"),
            "--work-dir",
            str(tmp_path / "work"),
            "--no-progress",
            "--bbl-recovery-dir",
            str(recovery),
        ]
    )
    assert result == 0
    rows = pq.read_table(tmp_path / "out/shard-00000/bibtex.parquet").to_pylist()
    assert any(row["citation_key"] == "outside" for row in rows)


def test_external_input_rejects_mismatched_paper_metadata(corpus, tmp_path):
    import hashlib

    metadata = corpus / "2606-00002/metadata.json"
    doc = json.loads(metadata.read_text(encoding="utf8"))
    doc["arxiv_id"] = "2606.00001"
    metadata.write_text(json.dumps(doc), encoding="utf8")
    recovery = tmp_path / "recovery"
    file = recovery / "files/2606-00001/v1/main.bbl"
    file.parent.mkdir(parents=True)
    content = b"example"
    file.write_bytes(content)
    record = {
        "arxiv_id": "2606.00001",
        "version": 1,
        "status": "recovered",
        "files": [
            {
                "path": "files/2606-00001/v1/main.bbl",
                "bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        ],
    }
    (recovery / "versions.jsonl").write_text(json.dumps(record) + "\n", encoding="utf8")
    with pytest.raises(ValueError, match="metadata"):
        main(
            [
                "--data-root",
                str(corpus),
                "--work-dir",
                str(tmp_path / "work"),
                "--scan",
                "--no-progress",
                "--bbl-recovery-dir",
                str(recovery),
            ]
        )


def test_external_run_cannot_resume_scan_as_full_parse(corpus, tmp_path):
    import hashlib

    recovery = tmp_path / "recovery"
    file = recovery / "files/2606-00001/v1/main.bbl"
    file.parent.mkdir(parents=True)
    content = b"\\bibitem{outside} A. Author. Title. 2020."
    file.write_bytes(content)
    record = {
        "arxiv_id": "2606.00001",
        "version": 1,
        "status": "recovered",
        "files": [
            {
                "path": "files/2606-00001/v1/main.bbl",
                "bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
        ],
    }
    (recovery / "versions.jsonl").write_text(json.dumps(record) + "\n", encoding="utf8")
    args = [
        "--data-root",
        str(corpus),
        "--out-dir",
        str(tmp_path / "out"),
        "--work-dir",
        str(tmp_path / "work"),
        "--no-progress",
        "--bbl-recovery-dir",
        str(recovery),
    ]
    assert main([*args, "--scan"]) == 0
    with pytest.raises(ValueError, match="configuration changed"):
        main(args)


def test_parser_main_restores_root_logging_configuration(corpus, tmp_path):
    import logging

    root = logging.getLogger()
    level, handlers = root.level, list(root.handlers)
    assert (
        main(
            [
                "--data-root",
                str(corpus),
                "--work-dir",
                str(tmp_path / "work"),
                "--scan",
                "--no-progress",
                "--limit",
                "1",
            ]
        )
        == 0
    )
    assert root.level == level
    assert root.handlers == handlers


def test_runner_stops_if_external_bbl_changes_after_preflight(corpus, tmp_path):
    import hashlib

    from src.parser.bibitem import BBLIntegrityError

    file = tmp_path / "external.bbl"
    original = b"\\bibitem{x} A. Author. Title. 2020."
    file.write_bytes(original)
    expected = hashlib.sha256(original).hexdigest()
    file.write_bytes(b"X" * len(original))
    options = Options(
        stages=FAKE.stages, bbl_index={("2606.00001", 1): [file]}, bbl_hashes={file: expected}
    )
    with pytest.raises(BBLIntegrityError):
        go(corpus, tmp_path, options=options)


def test_progress_reports_every_25_papers_and_finishes(tmp_path, capsys):
    root = tmp_path / "corpus-progress"
    for i in range(1, 28):
        make_paper(root, f"2606.{i:05}", [{"main.tex": DOC % "Text."}])
    run(root, tmp_path / "out-progress", tmp_path / "work-progress", options=FAKE, progress=True)
    output = capsys.readouterr().err
    assert "25/27" in output
    assert "27/27" in output


def test_resume_progress_includes_already_committed_papers(tmp_path, capsys):
    root = tmp_path / "corpus-resume-progress"
    for i in range(1, 28):
        make_paper(root, f"2606.{i:05}", [{"main.tex": DOC % "Text."}])
    run(
        root,
        tmp_path / "out-progress",
        tmp_path / "work-progress",
        options=FAKE,
        limit=25,
        progress=False,
    )
    run(root, tmp_path / "out-progress", tmp_path / "work-progress", options=FAKE, progress=True)
    output = capsys.readouterr().err
    assert "25/27" in output
    assert "27/27" in output
