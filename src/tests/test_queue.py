from __future__ import annotations

from src.crawler.queue import DownloadJob, ParserJob, RedisJobQueue


class FakeRedis:
    def __init__(self) -> None:
        self.stream: list[tuple[str, dict[str, str]]] = []
        self.acked: set[str] = set()
        self.pending: dict[str, int] = {}
        self.dead: list[dict[str, str]] = []
        self.keys: dict[str, str] = {}
        self.attempts: dict[str, int] = {}
        self.counter = 0
        self.reclaimable: list[tuple[str, dict[str, str]]] = []
        self.delivered: set[str] = set()

    def xadd(self, stream: str, fields: dict[str, str], id: str = "*") -> str:
        self.counter += 1
        message_id = str(self.counter)
        if stream.endswith(":dead"):
            self.dead.append(fields)
        else:
            self.stream.append((message_id, fields))
        return message_id

    def xgroup_create(self, *args, **kwargs) -> None:
        pass

    def xreadgroup(self, group: str, consumer: str, streams: dict, count: int, block: int):
        stream = next(iter(streams))
        rows = [
            (mid, fields)
            for mid, fields in self.stream
            if mid not in self.acked and mid not in self.delivered
        ]
        if not rows:
            return []
        mid, fields = rows[0]
        self.delivered.add(mid)
        self.pending[mid] = self.pending.get(mid, 0) + 1
        return [(stream, [(mid.encode(), {k.encode(): v.encode() for k, v in fields.items()})])]

    def xack(self, stream: str, group: str, message_id: str) -> int:
        self.acked.add(message_id)
        self.pending.pop(message_id, None)
        return 1

    def eval(self, _script: str, _numkeys: int, dedupe_key: str, stream: str, payload: str):
        if dedupe_key in self.keys:
            return self.keys[dedupe_key]
        self.counter += 1
        message_id = str(self.counter)
        self.stream.append((message_id, {"payload": payload}))
        self.keys[dedupe_key] = message_id
        return message_id

    def xinfo_groups(self, stream: str) -> list[dict]:
        outstanding = len([mid for mid, _ in self.stream if mid not in self.acked])
        return [
            {
                "name": "workers",
                "pending": len(self.pending),
                "lag": outstanding - len(self.pending),
            }
        ]

    def xpending_range(self, *args, **kwargs) -> list:
        return [{"message_id": mid.encode()} for mid, _ in self.reclaimable]

    def xrange(self, stream: str, start: str, end: str, count: int):
        return [
            (mid.encode(), {k.encode(): v.encode() for k, v in fields.items()})
            for mid, fields in self.stream
            if mid == start
        ]

    def incr(self, key: str) -> int:
        self.attempts[key] = self.attempts.get(key, 0) + 1
        return self.attempts[key]

    def xclaim(self, stream: str, group: str, consumer: str, min_idle_time: int, message_ids: list):
        reclaimed = [
            (mid.encode(), {k.encode(): v.encode() for k, v in fields.items()})
            for mid, fields in self.reclaimable
            if mid.encode() in message_ids
        ]
        for mid, _ in self.reclaimable:
            if mid.encode() in message_ids:
                self.delivered.add(mid)
        return reclaimed


def test_job_id_is_stable_for_same_parser_input():
    first = ParserJob.create("2606.30762", 1, "2606-30762/v1", {}, schema_version=1)
    second = ParserJob.create("2606.30762", 1, "2606-30762/v1", {}, schema_version=1)

    assert first.job_id == second.job_id


def test_publish_deduplicates_repeated_job():
    redis = FakeRedis()
    queue = RedisJobQueue("redis://unused", "parse", "workers", "w1", client=redis)
    job = ParserJob.create("2606.30762", 1, "2606-30762/v1", {})

    queue.publish(job)
    queue.publish(job)

    assert len(redis.stream) == 1


def test_claim_returns_job_and_ack_removes_it_from_depth():
    redis = FakeRedis()
    queue = RedisJobQueue("redis://unused", "parse", "workers", "w1", client=redis)
    job = ParserJob.create("2606.30762", 1, "2606-30762/v1", {"title": "A title"})
    queue.publish(job)

    claimed = queue.claim(block_ms=0)
    assert claimed == job
    assert queue.depth() == 1

    queue.ack(claimed.job_id)
    assert queue.depth() == 0
    assert redis.pending == {}


def test_retry_keeps_job_pending_until_reclaim_or_dead_letter():
    redis = FakeRedis()
    queue = RedisJobQueue("redis://unused", "parse", "workers", "w1", client=redis)
    job = ParserJob.create("2606.30762", 1, "2606-30762/v1", {})
    message_id = queue.publish(job)
    claimed = queue.claim(block_ms=0)
    redis.reclaimable = [(message_id, {"payload": job.to_fields()["payload"]})]

    queue.retry_or_dead_letter(claimed.job_id, "temporary", max_attempts=3)

    assert message_id not in redis.acked
    assert queue.reclaim_idle(min_idle_ms=10) == 1
    assert queue.claim(block_ms=0) == job


def test_retry_moves_job_to_dead_letter_after_attempt_limit():
    redis = FakeRedis()
    queue = RedisJobQueue("redis://unused", "parse", "workers", "w1", client=redis)
    job = ParserJob.create("2606.30762", 1, "2606-30762/v1", {})
    queue.publish(job)
    claimed = queue.claim(block_ms=0)

    queue.retry_or_dead_letter(claimed.job_id, "bad input", max_attempts=1)

    assert redis.dead[0]["job_id"] == job.job_id
    assert queue.depth() == 0


def test_reclaimed_message_is_returned_by_next_claim():
    redis = FakeRedis()
    queue = RedisJobQueue("redis://unused", "parse", "workers", "w1", client=redis)
    job = ParserJob.create("2606.30762", 1, "2606-30762/v1", {})
    message_id = queue.publish(job)
    redis.reclaimable = [(message_id, {"payload": job.to_fields()["payload"]})]

    assert queue.reclaim_idle(min_idle_ms=10) == 1
    assert queue.claim(block_ms=0) == job


def test_download_job_id_is_stable_and_the_queue_roundtrips_it():
    a = DownloadJob.create("2606.30734", 2)
    b = DownloadJob.create("2606.30734", 2)
    assert a.job_id == b.job_id
    assert a.job_id != DownloadJob.create("2606.30734", 1).job_id

    queue = RedisJobQueue(
        "redis://unused",
        "download-jobs",
        "crawler-workers",
        "c1",
        client=FakeRedis(),
        job_cls=DownloadJob,
    )
    queue.publish(a)
    queue.publish(b)  # same version: ignored
    job = queue.claim(block_ms=0)
    assert job == a and isinstance(job, DownloadJob)
    assert queue.claim(block_ms=0) is None
