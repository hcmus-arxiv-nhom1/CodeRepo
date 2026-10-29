# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
from __future__ import annotations

import pytest

from src.crawler.ladder import DiscoveryLadder


@pytest.mark.parametrize(
    ("backlog", "size"),
    [
        (0, 5),
        (9, 5),
        (10, 10),
        (29, 10),
        (30, 20),
        (59, 20),
        (60, 50),
        (99, 50),
        (100, 100),
        (5000, 100),
    ],
)
def test_backlog_picks_the_tier(backlog, size):
    assert DiscoveryLadder().next_size(backlog) == size


def test_rate_limit_caps_the_tier_for_the_cooldown_then_lets_it_grow_again():
    ladder = DiscoveryLadder()
    assert ladder.next_size(1000) == 100
    ladder.on_rate_limited()
    assert [ladder.next_size(1000) for _ in range(3)] == [50, 50, 50]
    assert ladder.next_size(1000) == 100


def test_consecutive_rate_limits_step_down_further():
    ladder = DiscoveryLadder()
    assert ladder.next_size(1000) == 100
    ladder.on_rate_limited()
    assert ladder.next_size(1000) == 50
    ladder.on_rate_limited()
    assert ladder.next_size(1000) == 20


def test_rate_limit_at_the_first_tier_stays_there():
    ladder = DiscoveryLadder()
    assert ladder.next_size(0) == 5
    ladder.on_rate_limited()
    assert ladder.next_size(1000) == 5


def test_negative_backlog_is_rejected():
    with pytest.raises(ValueError):
        DiscoveryLadder().next_size(-1)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"tiers": (10, 5), "backlog_steps": (10,)},  # not increasing
        {"tiers": (0, 5), "backlog_steps": (10,)},  # not positive
        {"tiers": (5, 10, 20), "backlog_steps": (10,)},  # one step missing
        {"tiers": (5, 10, 20), "backlog_steps": (30, 10)},  # steps not increasing
        {"cooldown_rounds": -1},
    ],
)
def test_invalid_configuration_is_rejected(kwargs):
    with pytest.raises(ValueError):
        DiscoveryLadder(**kwargs)


def test_from_env_reads_the_three_variables():
    ladder = DiscoveryLadder.from_env(
        {
            "DISCOVERY_TIERS": "2, 4,8",
            "DISCOVERY_BACKLOG_STEPS": "5,9",
            "DISCOVERY_COOLDOWN_ROUNDS": "1",
        }
    )
    assert ladder.tiers == (2, 4, 8)
    assert ladder.next_size(9) == 8
    ladder.on_rate_limited()
    assert ladder.next_size(9) == 4
    assert ladder.next_size(9) == 8  # cooldown of one round is over


def test_from_env_defaults_when_unset():
    assert DiscoveryLadder.from_env({}).tiers == (5, 10, 20, 50, 100)
