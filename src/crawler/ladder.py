# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
"""Batch size for metadata discovery, chosen from how much work is already queued.

Early on nothing is queued, so discovery asks the API for only a few IDs: the first download
jobs appear within seconds and a small request is unlikely to be rejected. Once the queues
hold enough jobs to keep the workers busy during one more request, batches grow so fewer
requests are spent on metadata. After an arXiv ``429`` the ladder steps down one tier and
holds it for a few rounds before it may grow again.
"""

from __future__ import annotations

import os
from bisect import bisect_right
from collections.abc import Mapping
from dataclasses import dataclass, field

DEFAULT_TIERS = (5, 10, 20, 50, 100)
DEFAULT_BACKLOG_STEPS = (10, 30, 60, 100)
DEFAULT_COOLDOWN_ROUNDS = 3


def _ints(raw: str) -> tuple[int, ...]:
    return tuple(int(part) for part in raw.split(",") if part.strip())


def _increasing(values: tuple[int, ...]) -> bool:
    return all(b > a for a, b in zip(values, values[1:], strict=False))


@dataclass
class DiscoveryLadder:
    tiers: tuple[int, ...] = DEFAULT_TIERS
    backlog_steps: tuple[int, ...] = DEFAULT_BACKLOG_STEPS
    cooldown_rounds: int = DEFAULT_COOLDOWN_ROUNDS
    _cap: int = field(default=0, init=False, repr=False)  # highest tier index while cooling down
    _cooldown: int = field(default=0, init=False, repr=False)
    _last: int = field(default=0, init=False, repr=False)  # tier index used by the last round

    def __post_init__(self) -> None:
        tiers, steps = self.tiers, self.backlog_steps
        if not tiers or tiers[0] <= 0 or not _increasing(tiers):
            raise ValueError("tiers must be positive and strictly increasing")
        if len(steps) != len(tiers) - 1:
            raise ValueError("need exactly one backlog step for every tier after the first")
        if (steps and steps[0] <= 0) or not _increasing(steps):
            raise ValueError("backlog steps must be positive and strictly increasing")
        if self.cooldown_rounds < 0:
            raise ValueError("cooldown_rounds cannot be negative")

    @classmethod
    def from_env(cls, environ: Mapping[str, str] | None = None) -> DiscoveryLadder:
        env = os.environ if environ is None else environ
        return cls(
            tiers=_ints(env["DISCOVERY_TIERS"]) if env.get("DISCOVERY_TIERS") else DEFAULT_TIERS,
            backlog_steps=_ints(env["DISCOVERY_BACKLOG_STEPS"])
            if env.get("DISCOVERY_BACKLOG_STEPS")
            else DEFAULT_BACKLOG_STEPS,
            cooldown_rounds=int(env.get("DISCOVERY_COOLDOWN_ROUNDS") or DEFAULT_COOLDOWN_ROUNDS),
        )

    def next_size(self, backlog: int) -> int:
        if backlog < 0:
            raise ValueError("backlog cannot be negative")
        index = bisect_right(self.backlog_steps, backlog)
        if self._cooldown > 0:
            index = min(index, self._cap)
            self._cooldown -= 1
        self._last = index
        return self.tiers[index]

    def on_rate_limited(self) -> None:
        """The chunk just discovered was rejected: use a lower tier for the next few rounds."""
        self._cap = max(self._last - 1, 0)
        self._cooldown = self.cooldown_rounds
