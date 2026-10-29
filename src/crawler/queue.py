from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from typing import Any


class _Payload:
    """Stream payload shared by every job type: one JSON field."""

    def to_fields(self) -> dict[str, str]:
        return {"payload": json.dumps(asdict(self), ensure_ascii=False)}

    @classmethod
    def from_fields(cls, fields: dict[bytes | str, bytes | str]):
        raw = fields.get(b"payload", fields.get("payload", "{}"))
        if isinstance(raw, bytes):
            raw = raw.decode()
        return cls(**json.loads(raw))


@dataclass(frozen=True)
class DownloadJob(_Payload):
    job_id: str
    arxiv_id: str
    version: int
    schema_version: int = 1

    @classmethod
    def create(cls, arxiv_id: str, version: int, schema_version: int = 1) -> DownloadJob:
        key = f"{arxiv_id}:v{version}:download-v{schema_version}"
        return cls(hashlib.sha256(key.encode()).hexdigest(), arxiv_id, version, schema_version)


@dataclass(frozen=True)
class ParserJob(_Payload):
    job_id: str
    arxiv_id: str
    version: int
    corpus_relpath: str
    metadata: dict[str, Any]
    schema_version: int = 1

    @classmethod
    def create(
        cls,
        arxiv_id: str,
        version: int,
        corpus_relpath: str,
        metadata: dict[str, Any],
        schema_version: int = 1,
    ) -> ParserJob:
        key = f"{arxiv_id}:v{version}:parser-v{schema_version}"
        job_id = hashlib.sha256(key.encode()).hexdigest()
        return cls(job_id, arxiv_id, version, corpus_relpath, metadata, schema_version)


class RedisJobQueue:
    def __init__(
        self,
        redis_url: str,
        stream: str,
        group: str,
        consumer: str,
        *,
        client: Any | None = None,
        job_cls: type = ParserJob,
    ) -> None:
        if client is None:
            from redis import Redis

            client = Redis.from_url(redis_url, decode_responses=False)
        self.client = client
        self.job_cls = job_cls
        self.stream = stream
        self.group = group
        self.consumer = consumer
        self.dead_letter_stream = f"{stream}:dead"
        self._claimed: dict[str, str] = {}
        self._reclaimed: list[tuple[str, dict]] = []
        try:
            self.client.xgroup_create(stream, group, id="0", mkstream=True)
        except Exception as exc:
            if "BUSYGROUP" not in str(exc):
                raise

    def publish(self, job: ParserJob) -> str:
        dedupe_key = f"{self.stream}:job:{job.job_id}"
        script = """
        local existing = redis.call('GET', KEYS[1])
        if existing then return existing end
        local message_id = redis.call('XADD', KEYS[2], '*', 'payload', ARGV[1])
        redis.call('SET', KEYS[1], message_id)
        return message_id
        """
        result = self.client.eval(script, 2, dedupe_key, self.stream, job.to_fields()["payload"])
        return result.decode() if isinstance(result, bytes) else result

    def claim(self, *, block_ms: int = 5000) -> ParserJob | None:
        if self._reclaimed:
            message_id, fields = self._reclaimed.pop(0)
            return self.claim_message(message_id, fields)
        rows = self.client.xreadgroup(
            self.group,
            self.consumer,
            {self.stream: ">"},
            count=1,
            block=block_ms,
        )
        if not rows:
            return None
        _, messages = rows[0]
        message_id, fields = messages[0]
        if isinstance(message_id, bytes):
            message_id = message_id.decode()
        job = self.job_cls.from_fields(fields)
        self._claimed[job.job_id] = message_id
        return job

    def ack(self, job_id: str) -> None:
        message_id = self._claimed.pop(job_id)
        self.client.xack(self.stream, self.group, message_id)
        self.client.incr(f"{self.stream}:completed")

    def retry_or_dead_letter(self, job_id: str, error: str, max_attempts: int) -> None:
        message_id = self._claimed[job_id]
        attempts_key = f"{self.stream}:attempts:{job_id}"
        attempts = self.client.incr(attempts_key)
        if attempts >= max_attempts:
            raw_fields = self.client.xrange(self.stream, message_id, message_id, count=1)[0][1]
            fields = {
                (key.decode() if isinstance(key, bytes) else key): (
                    value.decode() if isinstance(value, bytes) else value
                )
                for key, value in raw_fields.items()
            }
            fields.update(job_id=job_id, error=error, attempts=str(attempts))
            self.client.xadd(self.dead_letter_stream, fields)
            self.client.xack(self.stream, self.group, message_id)
            self._claimed.pop(job_id, None)
        else:
            self._claimed.pop(job_id, None)

    def claim_message(self, message_id: str | bytes, fields: dict) -> ParserJob:
        job = self.job_cls.from_fields(fields)
        self._claimed[job.job_id] = (
            message_id.decode() if isinstance(message_id, bytes) else message_id
        )
        return job

    def completed_count(self) -> int:
        value = self.client.get(f"{self.stream}:completed")
        return int(value or 0)

    def published_count(self) -> int:
        info = self.client.xinfo_groups(self.stream)
        group = next(item for item in info if item["name"] in (self.group, self.group.encode()))
        return int(group.get("entries-read") or 0) + int(group.get("lag") or 0)

    def depth(self) -> int:
        info = self.client.xinfo_groups(self.stream)
        group = next(item for item in info if item["name"] in (self.group, self.group.encode()))
        return int(group.get("pending", 0)) + int(group.get("lag") or 0)

    def reclaim_idle(self, min_idle_ms: int, count: int = 10) -> int:
        pending = self.client.xpending_range(
            self.stream, self.group, min="-", max="+", count=count, idle=min_idle_ms
        )
        ids = [item["message_id"] for item in pending]
        if not ids:
            return 0
        reclaimed = self.client.xclaim(self.stream, self.group, self.consumer, min_idle_ms, ids)
        for message_id, fields in reclaimed:
            message_id = message_id.decode() if isinstance(message_id, bytes) else message_id
            self._reclaimed.append((message_id, fields))
        return len(reclaimed)
