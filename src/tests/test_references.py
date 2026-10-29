# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
import json

import pytest

from src.crawler import references as refs_mod
from src.crawler.ratelimit import RateLimiter
from src.crawler.references import S2Client, crawl, normalize_reference

REF_ARXIV = {
    "paperId": "p-arxiv",
    "externalIds": {"ArXiv": "1705.04304", "CorpusId": 1},
    "title": "A Deep Reinforced Model",
    "venue": "ICLR",
    "year": 2018,
    "publicationDate": "2017-05-11",
    "authors": [{"authorId": "1", "name": "Romain Paulus"}],
}
REF_JOURNAL = {  # no arXiv ID, no publicationDate: still kept
    "paperId": None,
    "externalIds": {"DOI": "10.1/x"},
    "title": "A journal paper",
    "venue": "",
    "year": 1999,
    "publicationDate": None,
    "authors": [{"authorId": None, "name": "Ada Lovelace"}],
}


class FakeResponse:
    def __init__(self, status, body=None, headers=None):
        self.status_code = status
        self._body = body
        self.headers = headers or {}

    def json(self):
        return self._body


class FakeS2:
    """Answers /paper/batch and /paper/{id}/references like the real API (checked 27/10)."""

    def __init__(self, papers, fail_first=0):
        self.papers = papers  # arxiv_id -> list of references (None: unknown to S2)
        self.fail_first = fail_first
        self.calls = []
        self.headers = {}

    def request(self, method, url, params=None, json=None, timeout=None):
        self.calls.append((method, url))
        if self.fail_first:
            self.fail_first -= 1
            return FakeResponse(429, headers={"Retry-After": "2"})
        if method == "POST":
            out = []
            for sid in json["ids"]:
                refs = self.papers.get(sid.removeprefix("arXiv:"))
                out.append(None if refs is None else {"paperId": f"s2-{sid}", "references": refs})
            return FakeResponse(200, out)
        refs = self.papers["big"]  # paginated endpoint
        offset, limit = params["offset"], params["limit"]
        page = {
            "offset": offset,
            "data": [{"citedPaper": r} for r in refs[offset : offset + limit]],
        }
        if offset + limit < len(refs):
            page["next"] = offset + limit
        return FakeResponse(200, page)


def client_for(fake, sleeps=None):
    return S2Client(session=fake, limiter=RateLimiter(0.0), sleep=(sleeps or []).append)


def test_normalize_reference_keeps_refs_without_arxiv_id():
    ref = normalize_reference(REF_JOURNAL)
    assert ref["arxiv_id"] is None and ref["s2_id"] is None
    assert ref["submission_date"] == "1999" and ref["venue"] is None
    assert ref["authors"] == ["Ada Lovelace"]
    assert normalize_reference(REF_ARXIV)["submission_date"] == "2017-05-11"


def test_crawl_writes_every_reference_and_not_found(tmp_path):
    fake = FakeS2({"2606.30762": [REF_ARXIV, REF_JOURNAL], "2607.00001": []})
    counts = crawl(["2606.30762", "2607.00001", "2606.99999"], tmp_path, client=client_for(fake))
    assert counts["ok"] == 2 and counts["not_found"] == 1 and counts["failed"] == 0
    assert counts["requests"] == 1  # one batch call for the three papers

    rec = json.loads((tmp_path / "2606-30762" / "references.json").read_text(encoding="utf-8"))
    assert rec["status"] == "ok" and rec["num_references"] == 2
    assert rec["num_arxiv_references"] == 1
    assert [r["arxiv_id"] for r in rec["references"]] == ["1705.04304", None]
    missing = json.loads((tmp_path / "2606-99999" / "references.json").read_text(encoding="utf-8"))
    assert missing["status"] == "not_found" and missing["references"] == []


def test_done_papers_are_skipped(tmp_path):
    fake = FakeS2({"2606.30762": [REF_ARXIV]})
    crawl(["2606.30762"], tmp_path, client=client_for(fake))
    fake.calls.clear()
    counts = crawl(["2606.30762"], tmp_path, client=client_for(fake))
    assert fake.calls == [] and counts["skipped"] == 1


def test_429_is_retried_with_retry_after(tmp_path):
    sleeps = []
    fake = FakeS2({"2606.30762": [REF_ARXIV]}, fail_first=2)
    counts = crawl(["2606.30762"], tmp_path, client=client_for(fake, sleeps))
    assert counts["ok"] == 1 and len(fake.calls) == 3
    assert all(s >= 2 for s in sleeps)  # Retry-After honoured


def test_failed_batch_leaves_no_file_and_is_retried_next_run(tmp_path):
    fake = FakeS2({"2606.30762": [REF_ARXIV]}, fail_first=99)
    counts = crawl(["2606.30762"], tmp_path, client=client_for(fake))
    assert counts["failed"] == 1 and not (tmp_path / "2606-30762").exists()
    fake.fail_first = 0
    assert crawl(["2606.30762"], tmp_path, client=client_for(fake))["ok"] == 1


def test_failed_batch_is_retried_once_in_the_same_run(tmp_path):
    fake = FakeS2({"2606.30762": [REF_ARXIV]}, fail_first=5)  # the first batch call gives up
    counts = crawl(["2606.30762"], tmp_path, client=client_for(fake))
    assert counts["ok"] == 1 and counts["failed"] == 0


def test_s2_down_stops_after_three_failed_batches_in_a_row(tmp_path):
    ids = [f"2606.{n:05d}" for n in range(1, 11)]
    fake = FakeS2({}, fail_first=999)
    counts = crawl(ids, tmp_path, client=client_for(fake), batch_size=1, max_batch_retries=0)
    assert counts["failed"] == 10
    assert len(fake.calls) == 3 * 5  # 3 batches x 5 attempts, the other 7 never sent


def test_long_reference_list_uses_the_paginated_endpoint(tmp_path, monkeypatch):
    monkeypatch.setattr(refs_mod, "NESTED_REFERENCE_CAP", 3)
    monkeypatch.setattr(refs_mod, "PAGE_LIMIT", 2)
    big = [dict(REF_ARXIV, paperId=f"p{i}") for i in range(5)]
    fake = FakeS2({"2606.30762": big[:3], "big": big})
    counts = crawl(["2606.30762"], tmp_path, client=client_for(fake))
    assert counts["references"] == 5
    assert [m for m, _ in fake.calls] == ["POST", "GET", "GET", "GET"]


def test_batch_answer_of_wrong_length_is_an_error():
    class Broken(FakeS2):
        def request(self, *a, **k):
            return FakeResponse(200, [])

    with pytest.raises(refs_mod.S2Error):
        client_for(Broken({})).batch(["2606.30762"])
