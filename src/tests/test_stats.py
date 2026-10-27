# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
import csv
import json

from src.analysis import stats
from src.run_pipeline import JsonlWriter


def make_corpus(tmp_path):
    """Three papers: a clean one, a PDF-only one, and one not yet fetched by S2."""
    data, work = tmp_path / "corpus", tmp_path / "work"
    work.mkdir()
    papers = {
        "2606.30734": (["2026-06-29T18:01:23Z", "2026-07-08T17:35:09Z"], ["ok", "ok"]),
        "2606.30735": (["2026-06-29T18:05:00Z"], ["no_tex"]),
        "2606.30736": (["2026-06-29T18:06:00Z"], ["ok"]),
    }
    versions = JsonlWriter(work / "versions.jsonl")
    discovery = JsonlWriter(work / "discovery.jsonl")
    for arxiv_id, (dates, statuses) in papers.items():
        folder = data / arxiv_id.replace(".", "-")
        meta = {
            "arxiv_id": arxiv_id,
            "title": "T",
            "authors": ["A"],
            "submission_date": dates[0],
            "revised_dates": dates[1:],
            "venue": None,
            "num_versions": len(dates),
            "versions": [
                {"version": k, "date": d, "title": "T", "authors": ["A"]}
                for k, d in enumerate(dates, 1)
            ],
        }
        folder.mkdir(parents=True)
        (folder / "metadata.json").write_text(json.dumps(meta), encoding="utf-8")
        discovery.write({"arxiv_id": arxiv_id, "status": "found"})
        for v, status in enumerate(statuses, 1):
            vdir = folder / f"v{v}"
            vdir.mkdir()
            if status == "ok":
                (vdir / "main.tex").write_text("\\documentclass{article}")
            versions.write(
                {
                    "arxiv_id": arxiv_id,
                    "version": v,
                    "status": status,
                    "bytes_before": 1000,
                    "bytes_after": 100 if status == "ok" else 0,
                    "total_s": 3.5,
                }
            )
    refs = [
        {"s2_id": "a", "title": "x", "authors": [], "submission_date": None, "arxiv_id": "1"},
        {"s2_id": None, "title": "y", "authors": [], "submission_date": None, "arxiv_id": None},
    ]
    for arxiv_id in ("2606.30734", "2606.30735"):
        rec = {
            "arxiv_id": arxiv_id,
            "status": "ok",
            "num_references": 2,
            "num_arxiv_references": 1,
            "references": refs,
        }
        path = data / arxiv_id.replace(".", "-") / "references.json"
        path.write_text(json.dumps(rec), encoding="utf-8")
    discovery.write({"arxiv_id": "2606.30737", "status": "missing", "reason": "not_found"})
    versions.close()
    discovery.close()
    (work / "run.log").write_text(
        "x INFO src.crawler.http_client: GET https://export.arxiv.org/src/2606.30734v1 -> 200 (\n"
        "x INFO src.crawler.http_client: GET https://export.arxiv.org/src/2606.30734v2 -> 429 (\n"
        "x WARNING src.crawler.http_client: GET https://export.arxiv.org/src/x failed (Chunk\n"
        "x INFO src.crawler.references: S2 POST https://api.semanticscholar.org/graph/v1/"
        "paper/batch -> 200 (attempt 1)\n",
        encoding="utf-8",
    )
    return data, work


def read_csv(path):
    with open(path, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def test_stats_export_matches_the_book_schema(tmp_path):
    data, work = make_corpus(tmp_path)
    out = tmp_path / "book"
    assert stats.main(["--data-root", str(data), "--work-dir", str(work), "--out", str(out)]) == 0

    papers = {r["arxiv_id"]: r for r in read_csv(out / "papers_stats.csv")}
    assert list(papers) == ["2606.30734", "2606.30735", "2606.30736", "2606.30737"]
    p = papers["2606.30734"]
    assert (p["n_versions"], p["n_versions_ok"], p["has_source"]) == ("2", "2", "1")
    assert (p["size_before_bytes"], p["size_after_bytes"]) == ("2000", "200")
    assert (p["n_refs_total"], p["n_refs_arxiv"], p["n_refs_meta_ok"]) == ("2", "1", "1")
    assert p["range_owner"] == "thien" and float(p["process_sec"]) == 7.0
    assert papers["2606.30735"]["has_source"] == "0"  # PDF-only, still complete
    assert papers["2606.30736"]["ref_status"] == "missing"
    assert papers["2606.30737"]["n_versions"] == "0"  # not on arXiv: not a success

    sources = {r["source"]: r for r in read_csv(out / "references_stats.csv")}
    assert sources["arxiv"]["n_requests"] == "3" and sources["arxiv"]["n_http_429"] == "1"
    assert (sources["arxiv"]["n_hit"], sources["arxiv"]["n_miss"]) == ("3", "1")
    assert sources["semantic_scholar"]["n_requests"] == "1"
    assert sources["semantic_scholar"]["n_hit"] == "2"


def test_stats_benchmark_from_profile_and_time_v(tmp_path):
    data, work = make_corpus(tmp_path)
    prof = tmp_path / "profile"
    prof.mkdir()
    summary = {"wall_s": 12.5, "rss_max_mb": 80.0, "rss_mean_mb": 60.0}
    summary |= {"disk_max_mb": 5.0, "output_mb": 3.0}
    (prof / "profile_summary.json").write_text(json.dumps(summary))
    (prof / "profile_samples.csv").write_text(
        "t_sec,rss_total_mb,n_procs,disk_mb\n0.0,50.0,1,1.0\n1.0,80.0,2,5.0\n"
    )
    (work / "run_summary.json").write_text(json.dumps({"discovery_s": 1.2, "avg_paper_s": 4.0}))
    time_v = tmp_path / "time_v.log"
    time_v.write_text(
        "\tElapsed (wall clock) time (h:mm:ss or m:ss): 0:12.61\n"
        "\tMaximum resident set size (kbytes): 51200\n"
    )
    out = tmp_path / "book"
    args = ["--data-root", str(data), "--work-dir", str(work), "--out", str(out)]
    stats.main([*args, "--profile-dir", str(prof), "--time-log", str(time_v)])
    bench = {r["metric"]: r for r in read_csv(out / "benchmark_summary.csv")}
    assert bench["ram_max"]["value"] == "80.0" and bench["discovery_time"]["value"] == "1.2"
    assert bench["wall_time_time_v"]["value"] == "12.61"
    assert bench["ram_max_main_process"]["value"] == "52.4"
    assert len(read_csv(out / "resource_timeseries.csv")) == 2


def write_chunk(root, name, started, wall, rss_max, rss_mean, disk, output, samples):
    d = root / name
    d.mkdir(parents=True)
    summary = {"started_at": started, "wall_s": wall, "rss_max_mb": rss_max}
    summary |= {"rss_mean_mb": rss_mean, "disk_max_mb": disk, "output_mb": output}
    (d / "profile_summary.json").write_text(json.dumps(summary))
    rows = "".join(f"{t},{r},1,{dk}\n" for t, r, dk in samples)
    (d / "profile_samples.csv").write_text("t_sec,rss_total_mb,n_procs,disk_mb\n" + rows)
    (d / "time_v.log").write_text(
        f"\tElapsed (wall clock) time (h:mm:ss or m:ss): 0:{wall:05.2f}\n"
        f"\tMaximum resident set size (kbytes): {round(rss_max * 1_000_000 / 1024)}\n"
    )


def test_stats_aggregates_the_chunks_of_the_official_crawl(tmp_path):
    """The full crawl is the benchmark: chunks across Colab sessions are summed / maxed."""
    data, work = make_corpus(tmp_path)
    prof = work / "profile"  # default location, no --profile-dir needed
    # written out of order on purpose: chunks are sorted by started_at
    write_chunk(
        prof, "b", "2026-10-28T10:00:00Z", 30.0, 90.0, 70.0, 8.0, 6.0, [(0, 60, 6), (29, 90, 8)]
    )
    write_chunk(
        prof, "a", "2026-10-28T09:00:00Z", 10.0, 50.0, 40.0, 4.0, 3.0, [(0, 40, 3), (9, 50, 4)]
    )
    with open(work / "runs.jsonl", "w", encoding="utf-8") as fh:
        fh.write(json.dumps({"discovery_s": 2.0}) + "\n" + json.dumps({"discovery_s": 3.5}) + "\n")
    out = tmp_path / "book"
    stats.main(["--data-root", str(data), "--work-dir", str(work), "--out", str(out)])

    bench = {r["metric"]: float(r["value"]) for r in read_csv(out / "benchmark_summary.csv")}
    assert bench["wall_time_e2e"] == 40.0  # 10 + 30
    assert bench["ram_max"] == 90.0 and bench["disk_max"] == 8.0
    assert bench["ram_mean"] == 62.5  # (40*10 + 70*30) / 40
    assert bench["output_size"] == 6.0  # corpus after the last chunk
    assert bench["discovery_time"] == 5.5 and bench["avg_paper_time"] == 10.0  # 40 s / 4 IDs
    assert bench["wall_time_time_v"] == 40.0 and bench["ram_max_main_process"] == 90.0
    ts = read_csv(out / "resource_timeseries.csv")
    assert [float(r["t_sec"]) for r in ts] == [0.0, 9.0, 10.0, 39.0]  # chunk b after chunk a
