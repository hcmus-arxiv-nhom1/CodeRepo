# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
from src import config
from src.spike import analyse, find_last_id, sample_ids


def exists_up_to(last, calls):
    def exists(ids):
        calls.append(len(ids))
        return {i for i in ids if int(i.split(".")[1]) <= last}

    return exists


def test_find_last_id_from_a_guess_below_and_above():
    for guess in (31000, 32040, 33500):
        calls = []
        assert find_last_id("2606", exists_up_to(32040, calls), guess) == 32040
        assert all(n == 100 for n in calls)


def test_sample_ids_include_the_month_border():
    samples = sample_ids(per_range=10, seed=1)
    assert set(samples) == set(config.RANGES)
    assert all(len(ids) == 10 for ids in samples.values())
    assert {"2606.32040", "2607.00001"} <= set(samples["thien"])


def test_analyse_estimate_uses_pacing_floor():
    records = [
        {
            "status": "ok",
            "download_s": 0.5,
            "archive_bytes": 10,
            "bytes_before": 8,
            "bytes_after": 4,
        },
        {
            "status": "ok",
            "download_s": 6.0,
            "archive_bytes": 30,
            "bytes_before": 20,
            "bytes_after": 6,
        },
    ]
    report = analyse(records, papers=2, range_size=100)
    interval = config.ARXIV_MIN_INTERVAL_S + config.ARXIV_JITTER_S / 2
    expected_s = 1 * interval + 100 * 1.0 * ((interval + 6.0) / 2)
    assert report["estimate_hours_per_range"] == round(expected_s / 3600, 2)
    assert report["versions_per_paper"] == 1.0
