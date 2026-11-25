import json

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from src.m3.labels import label_path, write_matrix
from src.m3.model import FEATURE_VERSION, FEATURES, pair_features, rank_paper
from src.m3.train_model import evaluate, main, select_threshold
from src.utils.ids import to_folder_name


def test_title_containment_and_author_evidence():
    entry = {"title": r"{\emph{Spin Waves}}", "authors": ["A. Lee"], "year": 2024}
    candidate = {
        "ref_title": "Spin waves in magnetic nanotubes",
        "ref_authors": ["J. Smith"],
    }
    values = dict(zip(FEATURES, pair_features(entry, [candidate])[0], strict=True))
    assert values["entry_title_coverage"] == 1
    assert 0 < values["reference_title_coverage"] < 1
    assert values["author_overlap"] == 0
    assert values["year_missing"] == 1


def test_title_math_notation_preserves_distinct_symbols():
    entry = {"title": r"Dispersion of $\alpha$ modes"}
    candidates = [{"ref_title": "Dispersion of α modes"}, {"ref_title": "Dispersion of β modes"}]
    similarity = pair_features(entry, candidates)[:, FEATURES.index("title_similarity")]
    assert similarity[0] == 1
    assert similarity[1] < 1

    entry = {"title": "Dispersion of 𝛼 and W⁺ modes"}
    candidates = [
        {"ref_title": r"Dispersion of $\alpha$ and $W^{+}$ modes"},
        {"ref_title": "Dispersion of 𝛼 and W⁻ modes"},
    ]
    similarity = pair_features(entry, candidates)[:, FEATURES.index("title_similarity")]
    assert similarity[0] == 1
    assert similarity[1] < 1


def test_ranker_abstains_and_breaks_ties(tmp_path, monkeypatch):
    artifact = tmp_path / "model.json"
    coefficients = [0.0] * len(FEATURES)
    coefficients[FEATURES.index("title_similarity")] = 1.0
    artifact.write_text(
        json.dumps(
            dict(
                feature_version=FEATURE_VERSION,
                features=list(FEATURES),
                coefficients=coefficients,
                intercept=0.0,
                threshold=0.5,
            )
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("M3_MODEL_ARTIFACT", str(artifact))
    candidates = [
        {"ref_arxiv_id": f"2401.{i:05d}", "ref_title": "Spin waves"} for i in range(6, 0, -1)
    ]
    entries = [{"citation_key": "a", "title": "Spin waves"}, {"citation_key": "b"}]
    assert rank_paper(entries, candidates) == {
        "a": [f"2401.{i:05d}" for i in range(1, 6)],
        "b": [],
    }
    assert rank_paper(entries, []) == {"a": [], "b": []}


def test_threshold_reports_rank_and_abstention_separately():
    scores = np.array([0.2, 0.8, 0.9])
    rr = np.array([0.0, 0.5, 1.0])
    matched = np.array([False, True, True])
    threshold = select_threshold(scores, rr, matched, minimum_mrr=0.75)["threshold"]
    assert threshold == 0.8
    report = evaluate(scores, rr, matched, threshold)
    assert report["no_match_f1"] == 1.0
    assert report["mrr5_matched_only"] == 0.75
    assert report["mrr5_all_entries"] == 0.5


def test_threshold_preserves_best_attainable_mrr_when_floor_is_unmet():
    result = select_threshold(
        np.array([0.2, 0.8]), np.array([0.5, 0.0]), np.array([True, False]), minimum_mrr=1.0
    )
    assert result["meets_mrr_floor"] is False
    assert result["mrr5_matched_only"] == 0.5


def test_train_cli_uses_train_and_valid_and_exports_artifact(tmp_path, monkeypatch):
    train_manual, train_auto, valid, test = [f"2604.{i}" for i in range(20535, 20539)]
    partitions = tmp_path / "partitions.csv"
    partitions.write_text(
        f"arxiv_id,partition\n{train_manual},train\n{train_auto},train\n{valid},valid\n{test},test\n",
        encoding="utf-8",
    )
    labels = tmp_path / "labels"
    corpus = tmp_path / "corpus"
    m2 = tmp_path / "m2"
    shard = m2 / "shard-00000"
    shard.mkdir(parents=True)
    candidate_ids = ["2401.12345", "2401.12346"]
    bibtex = []
    for paper, method in [(train_manual, "manual"), (train_auto, "auto"), (valid, "manual")]:
        write_matrix(
            label_path(labels, method, paper), candidate_ids, {"a": candidate_ids[0], "b": None}
        )
        folder = corpus / to_folder_name(paper)
        folder.mkdir(parents=True)
        (folder / "references.json").write_text(
            json.dumps(
                {
                    "references": [
                        {
                            "arxiv_id": candidate_ids[0],
                            "title": "Spin waves",
                            "authors": ["A. Lee"],
                        },
                        {
                            "arxiv_id": candidate_ids[1],
                            "title": "Particle spectra",
                            "authors": ["J. Smith"],
                        },
                    ]
                }
            ),
            encoding="utf-8",
        )
        for key, title in [("a", "Spin waves"), ("b", "Oblique camera geometry")]:
            bibtex.append(
                dict(
                    arxiv_id=paper,
                    citation_key=key,
                    title=title,
                    authors=["A. Lee"],
                    year=2024,
                    doi=None,
                    journal=None,
                    url=None,
                )
            )
    label_path(labels, "manual", test).write_text("unreadable Test matrix", encoding="utf-8")
    pq.write_table(pa.Table.from_pylist(bibtex), shard / "bibtex.parquet")
    output = tmp_path / "out"
    main(
        [
            "--m2-dir",
            str(m2),
            "--corpus-dir",
            str(corpus),
            "--labels-dir",
            str(labels),
            "--partitions",
            str(partitions),
            "--out-dir",
            str(output),
        ]
    )
    metrics = json.loads((output / "valid_metrics.json").read_text(encoding="utf-8"))
    assert metrics["train_pairs"] == 8
    assert metrics["valid_papers"] == 1
    assert metrics["models"][metrics["selected_model"]]["manual"]["entries"] == 2
    assert (output / "train_features.png").stat().st_size > 0
    monkeypatch.setenv("M3_MODEL_ARTIFACT", str(output / "model.json"))
    result = rank_paper(
        bibtex[:2],
        [
            {
                "ref_arxiv_id": candidate_ids[1],
                "ref_title": "Particle spectra",
                "ref_authors": ["J. Smith"],
            },
            {
                "ref_arxiv_id": candidate_ids[0],
                "ref_title": "Spin waves",
                "ref_authors": ["A. Lee"],
            },
        ],
    )
    assert result["a"][0] == candidate_ids[0]
    assert result["b"] == []
