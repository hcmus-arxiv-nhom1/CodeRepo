# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
import json

from src.crawler.discovery import (
    discover,
    iso_utc,
    parse_api_feed,
    parse_oai_record,
    parse_snapshot_line,
)
from src.tests.helpers import FakeResponse, atom_feed, make_client, query

OAI_RECORD = b"""<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/"><GetRecord><record><metadata>
<arXivRaw xmlns="http://arxiv.org/OAI/arXivRaw/">
  <id>2606.30762</id>
  <version version="v1"><date>Mon, 29 Jun 2026 18:01:23 GMT</date></version>
  <version version="v2"><date>Wed, 08 Jul 2026 17:35:09 GMT</date></version>
  <version version="v3"><date>Wed, 02 Sep 2026 08:16:03 GMT</date></version>
  <title>Athermality of generalized
    Gibbs ensembles</title>
  <authors>Riccardo Senese, Bruno Bertini, Katja Klobas and Pasquale Calabrese</authors>
  <categories>cond-mat.stat-mech quant-ph</categories>
</arXivRaw></metadata></record></GetRecord></OAI-PMH>"""

OAI_MISSING = b"""<?xml version="1.0" encoding="UTF-8"?>
<OAI-PMH xmlns="http://www.openarchives.org/OAI/2.0/">
<error code="idDoesNotExist">unknown identifier</error></OAI-PMH>"""


def test_iso_utc_accepts_atom_and_rfc2822():
    assert iso_utc("2026-06-30T17:59:57Z") == "2026-06-30T17:59:57Z"
    assert iso_utc("Mon, 29 Jun 2026 18:01:23 GMT") == "2026-06-29T18:01:23Z"


def test_parse_api_feed():
    feed = atom_feed(
        [
            {
                "id": "2606.30762",
                "version": 3,
                "published": "2026-06-29T18:01:23Z",
                "updated": "2026-09-02T08:16:03Z",
                "title": "Athermality of\n   generalized Gibbs ensembles",
                "authors": ["Riccardo Senese", "Bruno Bertini"],
                "journal_ref": "Phys. Rev. X 1 (2026)",
            }
        ]
    )
    (e,) = parse_api_feed(feed)
    assert (e.arxiv_id, e.version) == ("2606.30762", 3)
    assert e.title == "Athermality of generalized Gibbs ensembles"
    assert e.authors == ["Riccardo Senese", "Bruno Bertini"]
    assert e.published == "2026-06-29T18:01:23Z" and e.updated == "2026-09-02T08:16:03Z"
    assert e.journal_ref == "Phys. Rev. X 1 (2026)" and e.categories == ["cs.LG"]


def test_parse_oai_record():
    rec = parse_oai_record(OAI_RECORD)
    assert rec["versions"] == {
        1: "2026-06-29T18:01:23Z",
        2: "2026-07-08T17:35:09Z",
        3: "2026-09-02T08:16:03Z",
    }
    assert rec["title"] == "Athermality of generalized Gibbs ensembles"
    assert rec["authors"] == [
        "Riccardo Senese",
        "Bruno Bertini",
        "Katja Klobas",
        "Pasquale Calabrese",
    ]
    assert parse_oai_record(OAI_MISSING) is None


def test_parse_snapshot_line():
    line = json.dumps(
        {
            "id": "0704.0001",
            "authors": "C. Bal\\'azs, E. L. Berger",
            "title": "Calculation of prompt\n  diphoton production",
            "journal-ref": "Phys.Rev.D76:013009,2007",
            "categories": "hep-ph",
            "versions": [
                {"version": "v1", "created": "Mon, 2 Apr 2007 19:18:42 GMT"},
                {"version": "v2", "created": "Tue, 24 Jul 2007 20:10:27 GMT"},
            ],
            "authors_parsed": [["Balázs", "C.", ""], ["Berger", "E. L.", ""], ["Collaboration"]],
        }
    )
    rec = parse_snapshot_line(line)
    assert rec["authors"] == ["C. Balázs", "E. L. Berger", "Collaboration"]
    assert rec["versions"] == {1: "2007-04-02T19:18:42Z", 2: "2007-07-24T20:10:27Z"}
    assert rec["journal_ref"] == "Phys.Rev.D76:013009,2007"


def arxiv_handler(calls):
    """A: 1 version, B: 3 versions, C: does not exist."""

    def handler(url):
        calls.append(url)
        if "oaipmh" in url:
            return FakeResponse(200, OAI_MISSING)
        ids = query(url)["id_list"].split(",")
        entries = []
        for i in ids:
            if i == "2606.00001":
                entries.append(
                    {
                        "id": i,
                        "version": 1,
                        "published": "2026-06-01T10:00:00Z",
                        "updated": "2026-06-01T10:00:00Z",
                    }
                )
            elif i == "2606.00002":
                entries.append(
                    {
                        "id": i,
                        "version": 3,
                        "published": "2026-06-02T10:00:00Z",
                        "updated": "2026-08-01T10:00:00Z",
                    }
                )
            elif i == "2606.00002v2":
                entries.append(
                    {
                        "id": "2606.00002",
                        "version": 2,
                        "published": "2026-06-02T10:00:00Z",
                        "updated": "2026-07-01T10:00:00Z",
                    }
                )
        return FakeResponse(200, atom_feed(entries))

    return handler


def test_discover_resolves_versions_and_writes_metadata(tmp_path):
    calls = []
    client = make_client(arxiv_handler(calls))
    journal = []
    ids = ["2606.00001", "2606.00002", "2606.00003"]
    res = discover(ids, client, tmp_path, journal=journal.append)

    assert res.found == {"2606.00001": 1, "2606.00002": 3}
    assert res.missing == {"2606.00003": "not_found"}
    meta = json.loads((tmp_path / "2606-00002" / "metadata.json").read_text(encoding="utf-8"))
    assert meta["submission_date"] == "2026-06-02T10:00:00Z"
    assert meta["revised_dates"] == ["2026-07-01T10:00:00Z", "2026-08-01T10:00:00Z"]
    assert meta["num_versions"] == 3 and meta["venue"] is None
    assert not (tmp_path / "2606-00003").exists()
    # base batch, one retry for the missing ID, one versioned batch, one OAI lookup
    assert len(calls) == 4
    assert {r["status"] for r in journal} == {"found", "missing"}


def test_discover_survives_malformed_api_response(tmp_path):
    calls = []
    good = arxiv_handler(calls)

    def handler(url):
        if not calls:  # first batch: feed cut mid-body, not valid XML
            calls.append(url)
            return FakeResponse(200, b'<?xml version="1.0"?><feed><entry><id>http://arx')
        return good(url)

    res = discover(["2606.00001"], make_client(handler), tmp_path)
    assert res.found == {"2606.00001": 1}  # resolved by the retry pass


def test_discover_skips_ids_with_metadata(tmp_path):
    calls = []
    client = make_client(arxiv_handler(calls))
    discover(["2606.00001", "2606.00002"], client, tmp_path)
    calls.clear()
    res = discover(["2606.00001", "2606.00002"], client, tmp_path)
    assert res.found == {"2606.00001": 1, "2606.00002": 3}
    assert calls == []
