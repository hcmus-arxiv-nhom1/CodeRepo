"""Per-paper lifecycle latency is distinct from source work and amortized throughput."""

import json
from types import SimpleNamespace

from src.run_pipeline import run
from src.tests.helpers import FakeClock
from src.tests.test_run_pipeline import IDS, FakeArxiv, client_for


def events(work):
    return [json.loads(s) for s in (work / "paper_events.jsonl").read_text().splitlines()]


def test_paper_completion_waits_for_references_and_retains_attempts(tmp_path, monkeypatch):
    clock = FakeClock()
    monkeypatch.setattr("src.run_pipeline.time.perf_counter", clock)

    def refs(ids, data, *, on_record):
        class Thread:
            def join(self):
                clock.sleep(10)
                for arxiv_id in ids:
                    on_record({"arxiv_id": arxiv_id, "status": "ok"})

        return Thread()

    monkeypatch.setattr("src.run_pipeline._start_references", refs)
    work = tmp_path / "work"
    for _ in range(2):
        run(
            IDS,
            data_root=tmp_path / "corpus",
            work_dir=work,
            client=client_for(FakeArxiv()),
            references=True,
            progress=False,
        )
    complete = [e for e in events(work) if e["event"] == "complete"]
    assert len(complete) == 2 * len(IDS)
    assert len({e["attempt_id"] for e in complete}) == 2
    assert all(e["elapsed_s"] == 10 for e in complete)
    assert all(e["references_status"] == "ok" for e in complete)


def test_failed_discovery_has_no_successful_paper_timing(tmp_path, monkeypatch):
    missing = "2606.99999"
    monkeypatch.setattr(
        "src.run_pipeline.discover",
        lambda *a, **kw: SimpleNamespace(found={}, missing={missing: "not found"}, elapsed_s=1),
    )
    run(
        [missing],
        data_root=tmp_path / "corpus",
        work_dir=tmp_path / "work",
        client=client_for(FakeArxiv()),
        references=False,
        progress=False,
    )
    records = events(tmp_path / "work")
    assert any(e["event"] == "discovery_failed" for e in records)
    assert not any(e["event"] == "complete" for e in records)


def test_resume_after_torn_paper_event_preserves_new_attempt_start(tmp_path):
    from src.analysis.measurement_exports import paper_attempt_rows

    work = tmp_path / "work"
    work.mkdir()
    (work / "paper_events.jsonl").write_text('{"attempt_id":"torn"')
    run(
        IDS,
        data_root=tmp_path / "corpus",
        work_dir=work,
        client=client_for(FakeArxiv()),
        references=False,
        progress=False,
    )
    rows = paper_attempt_rows([work], IDS)
    assert len(rows) == len(IDS) and all(r["complete"] for r in rows)
