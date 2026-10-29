# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
import pytest

from src import config
from src.utils.ids import (
    from_folder_name,
    is_new_style,
    iter_id_range,
    split_version,
    to_folder_name,
)


def test_folder_round_trip():
    assert to_folder_name("2606.30734") == "2606-30734"
    assert to_folder_name("2606.30734v3") == "2606-30734"
    assert from_folder_name("2606-30734") == "2606.30734"


def test_old_style_id_kept():
    assert not is_new_style("hep-th/9901001")
    assert to_folder_name("hep-th/9901001") == "hep-th/9901001"


def test_split_version():
    assert split_version("2607.00001v2") == ("2607.00001", 2)
    assert split_version("2607.00001") == ("2607.00001", None)


def test_range_within_one_month():
    ids = list(iter_id_range("2606.25734", "2606.30733", {}))
    assert len(ids) == 5000
    assert ids[0] == "2606.25734"
    assert ids[-1] == "2606.30733"


def test_range_crossing_month_border():
    # Thiên's range; 2606.32040 is the last June ID (confirmed with the arXiv API, 2026-10-18).
    ids = list(iter_id_range("2606.30734", "2607.03693", {"2606": 32040}))
    assert len(ids) == 5000
    assert "2606.32040" in ids
    assert "2606.32041" not in ids
    assert ids[ids.index("2606.32040") + 1] == "2607.00001"
    assert ids[-1] == "2607.03693"


def test_range_crossing_year_border():
    ids = list(iter_id_range("2612.00009", "2701.00002", {"2612": 10}))
    assert ids == ["2612.00009", "2612.00010", "2701.00001", "2701.00002"]


def test_range_missing_month_end_raises():
    with pytest.raises(KeyError):
        list(iter_id_range("2606.30734", "2607.03693", {}))


def test_assigned_ranges_hold_5000_ids_each_and_never_overlap():
    seen: set[str] = set()
    for start, end in config.RANGES.values():
        ids = set(iter_id_range(start, end, config.MONTH_END))
        assert len(ids) == 5000
        assert not ids & seen  # an ID in two ranges would be crawled twice
        seen |= ids
