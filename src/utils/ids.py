"""arXiv ID helpers shared by the crawler (M1), parser (M2) and dataset export (M3).

Conventions (see the M1 statement):
- New-style ID ``2606.30734`` is stored in folder ``2606-30734`` (dot -> dash).
- Version IDs append ``v<N>``: ``2606.30734v2``.
- Old-style IDs (before 2007, e.g. ``hep-th/9901001``) are kept unchanged as keys.
"""

from __future__ import annotations

import re
from collections.abc import Iterator, Mapping

NEW_ID_RE = re.compile(r"^(?P<yymm>\d{4})\.(?P<num>\d{4,5})(?:v(?P<version>\d+))?$")
FOLDER_RE = re.compile(r"^(?P<yymm>\d{4})-(?P<num>\d{4,5})$")
OLD_ID_RE = re.compile(r"^[a-z\-]+(?:\.[A-Z]{2})?/\d{7}(?:v\d+)?$")


def is_new_style(arxiv_id: str) -> bool:
    return NEW_ID_RE.match(arxiv_id) is not None


def split_version(arxiv_id: str) -> tuple[str, int | None]:
    """``'2606.30734v2'`` -> ``('2606.30734', 2)``; no suffix -> ``(id, None)``."""
    m = re.match(r"^(?P<base>.+?)v(?P<version>\d+)$", arxiv_id)
    if m is None:
        return arxiv_id, None
    return m.group("base"), int(m.group("version"))


def to_folder_name(arxiv_id: str) -> str:
    """``'2606.30734'`` -> ``'2606-30734'``. Old-style IDs are returned unchanged."""
    base, _ = split_version(arxiv_id)
    m = NEW_ID_RE.match(base)
    if m is None:
        return base
    return f"{m.group('yymm')}-{m.group('num')}"


def from_folder_name(folder: str) -> str:
    """``'2606-30734'`` -> ``'2606.30734'``."""
    m = FOLDER_RE.match(folder)
    if m is None:
        raise ValueError(f"not an arXiv folder name: {folder!r}")
    return f"{m.group('yymm')}.{m.group('num')}"


def _num_width(yymm: str) -> int:
    # Since 1501 the sequence number has 5 digits; before that, 4.
    return 5 if yymm >= "1501" else 4


def _next_month(yymm: str) -> str:
    yy, mm = int(yymm[:2]), int(yymm[2:])
    if mm == 12:
        return f"{yy + 1:02d}01"
    return f"{yy:02d}{mm + 1:02d}"


def iter_id_range(start: str, end: str, last_num_by_month: Mapping[str, int]) -> Iterator[str]:
    """Yield every new-style ID from ``start`` to ``end`` inclusive, crossing month borders.

    ``last_num_by_month`` gives the last sequence number of each month that the range
    leaves before reaching ``end``'s month, e.g. ``{"2606": 32040}`` for
    ``2606.30734 -> 2607.03693``. It must come from the arXiv API, never be guessed.
    """
    ms, me = NEW_ID_RE.match(start), NEW_ID_RE.match(end)
    if ms is None or me is None:
        raise ValueError(f"range bounds must be new-style IDs: {start!r}, {end!r}")
    if (ms.group("yymm"), int(ms.group("num"))) > (me.group("yymm"), int(me.group("num"))):
        raise ValueError(f"start {start!r} is after end {end!r}")

    yymm, num = ms.group("yymm"), int(ms.group("num"))
    end_yymm, end_num = me.group("yymm"), int(me.group("num"))
    while True:
        last = end_num if yymm == end_yymm else last_num_by_month.get(yymm)
        if last is None:
            raise KeyError(f"missing last sequence number for month {yymm}")
        width = _num_width(yymm)
        for n in range(num, last + 1):
            yield f"{yymm}.{n:0{width}d}"
        if yymm == end_yymm:
            return
        yymm, num = _next_month(yymm), 1
