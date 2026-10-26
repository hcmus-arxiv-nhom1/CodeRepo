# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
import csv
import json

import pytest

from src.crawler.http_client import BlockedError, HttpClient
from src.crawler.ratelimit import RateLimiter
from src.crawler.worklock import WorkDirLock, WorkDirLockedError
from src.run_pipeline import load_version_state, run
from src.tests.helpers import (
    MAIN_TEX,
    PNG,
    FakeResponse,
    FakeSession,
    atom_feed,
    gzip_bytes,
    query,
    tar_bytes,
)

VERSIONS = {"2606.00001": 1, "2606.00002": 2, "2606.00003": 1}  # 2606.00003 is PDF-only
IDS = list(VERSIONS)


class FakeArxiv:
    """Answers API and /src/ requests; can simulate a kill or a broken version."""

    def __init__(
        self, kill_after_downloads=None, src_status=None, kill_exc=KeyboardInterrupt, src_body=None
    ):
        self.downloads = []
        self.api_calls = 0
        self.kill_after = kill_after_downloads
        self.kill_exc = kill_exc
        self.src_status = src_status or {}
        self.src_body = src_body or {}  # name -> (body, filename): override a /src/ response

    def __call__(self, url):
        if "/api/query" in url:
            self.api_calls += 1
            entries = [
                {"id": i, "version": VERSIONS[i], "updated": f"2026-06-0{VERSIONS[i]}T00:00:00Z"}
                for i in query(url)["id_list"].split(",")
                if i in VERSIONS
            ]
            return FakeResponse(200, atom_feed(entries))
        assert "/src/" in url
        if self.kill_after is not None and len(self.downloads) == self.kill_after:
            raise self.kill_exc  # the Colab session dies mid-run
        name = url.rsplit("/", 1)[1]
        self.downloads.append(name)
        if name in self.src_status:
            return FakeResponse(self.src_status[name])
        if name in self.src_body:
            body, filename = self.src_body[name]
        elif name.startswith("2606.00003"):
            body, filename = b"%PDF-1.5\n" + PNG, f"arXiv-{name}.pdf"
        else:
            body, filename = (
                tar_bytes({"main.tex": MAIN_TEX, "fig.png": PNG}),
                f"arXiv-{name}.tar.gz",
            )
        headers = {
            "Content-Type": "application/gzip",
            "Content-Length": str(len(body)),
            "Content-Disposition": f'attachment; filename="{filename}"',
        }
        return FakeResponse(200, body, headers)


def client_for(fake):
    return HttpClient(
        RateLimiter(0.0),
        "test-agent",
        session=FakeSession(fake),
        backoff_base_s=0.0,
        rng=lambda: 0.0,
    )


def crawl(tmp_path, fake):
    return run(
        IDS,
        data_root=tmp_path / "corpus",
        work_dir=tmp_path / "work",
        client=client_for(fake),
        references=False,
        progress=False,
    )


def test_end_to_end_layout_and_summary(tmp_path):
    summary = crawl(tmp_path, FakeArxiv())
    corpus = tmp_path / "corpus"
    assert sorted(p.name for p in corpus.iterdir()) == ["2606-00001", "2606-00002", "2606-00003"]
    assert sorted(p.name for p in (corpus / "2606-00002").iterdir()) == [
        "metadata.json",
        "v1",
        "v2",
    ]
    assert [p.name for p in (corpus / "2606-00001" / "v1").iterdir()] == ["main.tex"]
    assert list((corpus / "2606-00003" / "v1").iterdir()) == []  # PDF-only: empty record
    assert summary["papers_complete"] == 3 and summary["paper_success_rate"] == 1.0
    assert summary["version_status"] == {"ok": 3, "no_tex": 1}
    assert summary["failed_rows"] == 0
    assert not (tmp_path / "work" / "tmp").exists()
    state = load_version_state(tmp_path / "work" / "versions.jsonl")
    assert state[("2606.00001", 1)]["dropped"] == {".png": {"files": 1, "bytes": len(PNG)}}


def test_second_run_on_same_work_dir_is_refused(tmp_path):
    with WorkDirLock(tmp_path / "work"):
        with pytest.raises(WorkDirLockedError):
            crawl(tmp_path, FakeArxiv())
    crawl(tmp_path, FakeArxiv())  # free again once the first run is gone


def test_lock_is_released_when_a_run_stops_on_an_error(tmp_path):
    with pytest.raises(BlockedError):
        crawl(tmp_path, FakeArxiv(kill_after_downloads=1, kill_exc=BlockedError("u", "403", 403)))
    assert crawl(tmp_path, FakeArxiv())["papers_complete"] == 3


def test_resume_after_kill_does_not_download_finished_versions(tmp_path):
    first = FakeArxiv(kill_after_downloads=2)
    with pytest.raises(KeyboardInterrupt):
        crawl(tmp_path, first)
    assert first.downloads == ["2606.00001v1", "2606.00002v1"]
    state = load_version_state(tmp_path / "work" / "versions.jsonl")
    assert len(state) == 2
    interrupted = json.loads((tmp_path / "work" / "run_summary.json").read_text())
    assert interrupted["interrupted"] is True

    second = FakeArxiv()
    summary = crawl(tmp_path, second)
    assert second.downloads == ["2606.00002v2", "2606.00003v1"]
    assert second.api_calls == 0  # metadata.json reused
    assert summary["papers_complete"] == 3

    third = FakeArxiv()
    crawl(tmp_path, third)
    assert third.downloads == [] and third.api_calls == 0


def test_run_stopped_by_blocked_error_is_marked_interrupted(tmp_path):
    fake = FakeArxiv(kill_after_downloads=1, kill_exc=BlockedError("u", "HTTP 403", 403))
    with pytest.raises(BlockedError):
        crawl(tmp_path, fake)
    summary = json.loads((tmp_path / "work" / "run_summary.json").read_text())
    assert summary["interrupted"] is True


def test_failed_version_is_reported_and_retried(tmp_path):
    summary = crawl(tmp_path, FakeArxiv(src_status={"2606.00002v2": 503}))
    assert summary["version_status"]["failed"] == 1 and summary["papers_complete"] == 2
    with open(tmp_path / "work" / "failed.csv", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    assert [(r["arxiv_id"], r["version"], r["stage"]) for r in rows] == [
        ("2606.00002", "2", "download")
    ]
    assert not (tmp_path / "corpus" / "2606-00002" / "v2").exists()

    retry = FakeArxiv()
    summary = crawl(tmp_path, retry)
    assert retry.downloads == ["2606.00002v2"] and summary["papers_complete"] == 3


def test_gzipped_html_submission_is_an_empty_record_not_a_failure(tmp_path):
    """2606.25881v1 (EPTCS proceedings) was failing on every run before the html.gz kind."""
    page = gzip_bytes(b"<!DOCTYPE html>\n<html><title>EPTCS 446</title></html>\n")
    fake = FakeArxiv(src_body={"2606.00001v1": (page, "arXiv-2606.00001v1.html.gz")})
    summary = crawl(tmp_path, fake)
    assert summary["version_status"] == {"ok": 2, "no_tex": 2} and summary["failed_rows"] == 0
    assert summary["version_kinds"]["html.gz"] == 1
    assert list((tmp_path / "corpus" / "2606-00001" / "v1").iterdir()) == []


def test_missing_source_is_an_empty_record(tmp_path):
    summary = crawl(tmp_path, FakeArxiv(src_status={"2606.00001v1": 404}))
    assert summary["version_status"]["unavailable"] == 1
    assert list((tmp_path / "corpus" / "2606-00001" / "v1").iterdir()) == []
    assert summary["papers_complete"] == 3
