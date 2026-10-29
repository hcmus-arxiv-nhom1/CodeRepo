from __future__ import annotations

from dataclasses import dataclass


@dataclass
class AdaptiveBatchScheduler:
    seed: int = 10
    max_batch: int = 100
    step: int = 10
    low_watermark: int = 20
    high_watermark: int = 100
    max_prefetch: int = 200
    _next_size: int = 0

    def __post_init__(self) -> None:
        if min(self.seed, self.max_batch, self.step, self.max_prefetch) <= 0:
            raise ValueError("batch and prefetch values must be positive")
        if self.low_watermark < 0 or self.high_watermark <= self.low_watermark:
            raise ValueError("high_watermark must exceed nonnegative low_watermark")
        self._next_size = min(self.seed, self.max_batch)

    def next_batch(self, queue_depth: int, worker_progress: bool) -> int:
        if queue_depth < 0:
            raise ValueError("queue_depth cannot be negative")
        available = self.max_prefetch - queue_depth
        if available <= 0 or queue_depth >= self.high_watermark or not worker_progress:
            return 0

        batch = min(self._next_size, self.max_batch, available)
        if queue_depth < self.low_watermark:
            self._next_size = min(self.max_batch, self._next_size + self.step)
        else:
            self._next_size = min(self._next_size, self.step)
        return batch
