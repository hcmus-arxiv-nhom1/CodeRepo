import json

import pytest

from src.crawler.discovery import discover, load_snapshot
from src.tests.helpers import FakeResponse, atom_feed, make_client, query


def snapshot(tmp_path, versions=1):
    p = tmp_path / "snapshot.json"
    rec = dict(
        id="2606.00001",
        title="Current title",
        authors="Ada",
        authors_parsed=[["Lovelace", "Ada", ""]],
        versions=[
            dict(version=f"v{k}", created=f"2026-06-0{k}T00:00:00Z") for k in range(1, versions + 1)
        ],
    )
    p.write_text(json.dumps(rec) + "\n", encoding="utf-8")
    return p


def test_snapshot_single_version_never_contacts_arxiv(tmp_path):
    def forbidden(url):
        pytest.fail(f"unexpected network: {url}")

    notes = []
    result = discover(
        ["2606.00001"],
        make_client(forbidden),
        tmp_path / "corpus",
        snapshot=snapshot(tmp_path),
        journal=notes.append,
    )
    assert result.found == {"2606.00001": 1}
    meta = json.loads((tmp_path / "corpus/2606-00001/metadata.json").read_text())
    assert meta["versions"][0]["title"] == "Current title"
    assert notes[0]["source"] == "snapshot"


def test_snapshot_queries_only_historical_versions(tmp_path):
    calls = []

    def response(url):
        ids = query(url)["id_list"].split(",")
        calls.extend(ids)
        assert ids == ["2606.00001v1", "2606.00001v2"]
        return FakeResponse(
            200,
            atom_feed([dict(id="2606.00001", version=k, title=f"Historical {k}") for k in (1, 2)]),
        )

    result = discover(
        ["2606.00001"], make_client(response), tmp_path / "corpus", snapshot=snapshot(tmp_path, 3)
    )
    assert result.found == {"2606.00001": 3}
    assert calls == ["2606.00001v1", "2606.00001v2"]
    meta = json.loads((tmp_path / "corpus/2606-00001/metadata.json").read_text())
    assert [v["title"] for v in meta["versions"]] == [
        "Historical 1",
        "Historical 2",
        "Current title",
    ]
    assert [v["date"] for v in meta["versions"]] == [f"2026-06-0{k}T00:00:00Z" for k in (1, 2, 3)]


def test_snapshot_loader_accepts_standard_json_spacing(tmp_path):
    assert (
        load_snapshot(snapshot(tmp_path), ["2606.00001"])["2606.00001"]["title"] == "Current title"
    )


def test_snapshot_index_is_reused_and_bound_to_source(tmp_path):
    from src.crawler.snapshot import prepare_snapshot_index

    source = snapshot(tmp_path)
    index = tmp_path / "work/snapshot.jsonl"
    first = prepare_snapshot_index(source, ["2606.00001"], index)
    before = index.read_bytes()
    second = prepare_snapshot_index(source, ["2606.00001"], index)
    assert first["sha256"] == second["sha256"]
    assert index.read_bytes() == before and second["reused"]
    assert load_snapshot(index, ["2606.00001"]).keys() == {"2606.00001"}
    source.write_text(source.read_text().replace("Current title", "Changed title"))
    with pytest.raises(ValueError, match="snapshot"):
        prepare_snapshot_index(source, ["2606.00001"], index, expected_sha256=first["sha256"])


def test_missing_snapshot_id_falls_back_to_api(tmp_path):
    calls = []

    def response(url):
        calls.append(url)
        return FakeResponse(200, atom_feed([dict(id="2606.00002", version=1)]))

    result = discover(
        ["2606.00001", "2606.00002"],
        make_client(response),
        tmp_path / "corpus",
        snapshot=snapshot(tmp_path),
    )
    assert result.found == {"2606.00001": 1, "2606.00002": 1}
    assert len(calls) == 1 and query(calls[0])["id_list"] == "2606.00002"


def test_refused_historical_api_keeps_snapshot_dates_without_inventing_authors(tmp_path):
    result = discover(
        ["2606.00001"],
        make_client(lambda url: FakeResponse(503)),
        tmp_path / "corpus",
        snapshot=snapshot(tmp_path, 2),
    )
    assert result.found == {"2606.00001": 2} and result.api_failed_batches == 1
    meta = json.loads((tmp_path / "corpus/2606-00001/metadata.json").read_text())
    assert meta["versions"][0]["title"] is None and meta["versions"][0]["authors"] is None
    assert meta["versions"][1]["title"] == "Current title"


def test_run_child_passes_snapshot_to_pipeline(tmp_path, monkeypatch):
    from src.benchmark.runner import run_child

    seen = {}
    monkeypatch.setattr("src.run_pipeline.run", lambda ids, **kwargs: seen.update(kwargs))
    local = tmp_path / "local"
    (local / "work").mkdir(parents=True)
    run_child(
        dict(
            local=str(local),
            remote=str(tmp_path / "remote"),
            checkpoint_s=300,
            ids=["x"],
            owner="dang",
            references=False,
            snapshot=str(tmp_path / "index.jsonl"),
        )
    )
    assert seen["snapshot"] == tmp_path / "index.jsonl"


def test_benchmark_resume_rejects_changed_snapshot(tmp_path):
    from src.benchmark.runner import BenchmarkConfig, prepare_run

    p = snapshot(tmp_path)
    cfg = BenchmarkConfig("snap", "sample", ("dang",), count=1, snapshot=str(p))
    original = prepare_run(cfg, tmp_path / "runs", "code", resume=False)
    assert original["snapshot_sha256"]
    p.write_text(p.read_text().replace("Current title", "Changed title"))
    with pytest.raises(ValueError, match="revision differs"):
        prepare_run(cfg, tmp_path / "runs", "code", resume=True)


def test_discovery_measurement_includes_index_preparation(tmp_path):
    from src.analysis.stats import discovery_seconds

    (tmp_path / "snapshot_preparation").mkdir()
    (tmp_path / "snapshot_preparation/s1.json").write_text(json.dumps(dict(preparation_s=5)))
    (tmp_path / "runs.jsonl").write_text(json.dumps(dict(discovery_s=2)) + "\n")
    assert discovery_seconds([tmp_path]) == 7


def test_invalid_snapshot_record_uses_network_fallback(tmp_path):
    source = snapshot(tmp_path)
    row = json.loads(source.read_text())
    row["versions"] = []
    source.write_text(json.dumps(row))
    calls = []

    def response(url):
        calls.append(url)
        return FakeResponse(200, atom_feed([dict(id="2606.00001", version=1)]))

    result = discover(["2606.00001"], make_client(response), tmp_path / "corpus", snapshot=source)
    assert result.found == {"2606.00001": 1} and len(calls) == 1


def test_corrupt_filtered_index_is_rebuilt(tmp_path):
    from src.crawler.snapshot import prepare_snapshot_index

    source = snapshot(tmp_path)
    index = tmp_path / "index.jsonl"
    first = prepare_snapshot_index(source, ["2606.00001"], index)
    index.write_text("corrupted")
    repaired = prepare_snapshot_index(
        source, ["2606.00001"], index, expected_sha256=first["sha256"]
    )
    assert not repaired["reused"]
    assert load_snapshot(index, ["2606.00001"])["2606.00001"]["title"] == "Current title"
