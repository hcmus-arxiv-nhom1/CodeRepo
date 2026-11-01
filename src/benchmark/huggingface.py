"""Versioned Hugging Face checkpoint transport. Credentials stay in HF_TOKEN."""

# Drafted with assistance from OpenAI Codex, disclosed per course rules.
from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor
from email.utils import parsedate_to_datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from filelock import FileLock

from src.benchmark.checkpoints import Checkpoints, write_json


def remote_name(name):
    if not Checkpoints.safe_name(name):
        raise ValueError("Unsafe HF checkpoint path")
    parts = name.split("/")
    if parts[:2] == ["data", "corpus"] and len(parts) > 2:
        paper = parts[2]
        return "/".join(parts[:2] + [paper[:4], paper[5:8]] + parts[2:])
    return name


class CommitBudget:
    """Shared local sliding-window budget for parent/child commit attempts.

    This cannot account for other runtimes or earlier server-side usage. A 429
    adds a shared cooldown instead of repeatedly attempting checkpoint commits.
    """

    def __init__(self, path, *, limit=90, window_s=3600):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = FileLock(str(self.path) + ".lock")
        self.limit, self.window_s = limit, window_s

    def _state(self):
        return (
            json.loads(self.path.read_text())
            if self.path.exists()
            else {"attempts": [], "blocked_until": 0}
        )

    def defer(self, seconds):
        with self.lock:
            state = self._state()
            state["blocked_until"] = max(state["blocked_until"], time.time() + seconds)
            write_json(self.path, state)

    def reserve(self):
        while True:
            with self.lock:
                now = time.time()
                state = self._state()
                attempts = [t for t in state["attempts"] if t > now - self.window_s]
                until = state["blocked_until"]
                if len(attempts) >= self.limit:
                    until = max(until, attempts[0] + self.window_s)
                if until <= now:
                    state["attempts"] = attempts + [now]
                    write_json(self.path, state)
                    return
                delay = until - now
            logging.warning(
                "HF commit budget/cooldown: %.0f s remaining; retaining local data", delay
            )
            time.sleep(min(delay, 30))


def commit_retry_delay(exc):
    headers = exc.response.headers
    value = headers.get("Retry-After")
    if value:
        try:
            return max(1, float(value))
        except ValueError:
            try:
                return max(1, parsedate_to_datetime(value).timestamp() - time.time())
            except (TypeError, ValueError, OverflowError):
                pass
    match = re.search(r"(?:^|[;,\s])t=(\d+)", headers.get("RateLimit", ""))
    return max(1, int(match[1])) if match else 3600


class HubStore:
    def __init__(self, repo_id, cache):
        os.environ.setdefault("HF_XET_CACHE", str(Path(cache).parent / ".hf-xet-cache"))
        from huggingface_hub import HfApi

        token = os.environ.get("HF_TOKEN", "").strip()
        if not token:
            raise ValueError("HF_TOKEN missing; enable the Colab Secret")
        self.repo_id, self.cache = repo_id, Path(cache)
        self.api = HfApi(token=token)
        self.budget = CommitBudget(self.cache / "commit-budget.json")
        info = self.api.repo_info(repo_id, repo_type="dataset")
        if not info.private:
            raise ValueError("Expected a private Hugging Face Dataset")

    def read(self, path, revision="main", missing_ok=False):
        from huggingface_hub import hf_hub_download
        from huggingface_hub.errors import EntryNotFoundError

        if missing_ok and self.api.repo_info(self.repo_id, repo_type="dataset").sha is None:
            return None
        try:
            self.cache.mkdir(parents=True, exist_ok=True)
            with TemporaryDirectory(dir=self.cache, prefix="hf-read-") as temporary:
                file = hf_hub_download(
                    self.repo_id,
                    path,
                    repo_type="dataset",
                    revision=revision,
                    token=self.api.token,
                    local_dir=temporary,
                )
                return Path(file).read_bytes()
        except EntryNotFoundError:
            if missing_ok:
                return None
            raise

    def files(self):
        return self.api.list_repo_files(self.repo_id, repo_type="dataset")

    def commit(self, files, message="M1 checkpoint", parent=None):
        from huggingface_hub import CommitOperationAdd
        from huggingface_hub.errors import HfHubHTTPError

        revision = None
        items = list(files.items())
        for start in range(0, len(items), 100):
            batch = items[start : start + 100]
            for attempt in range(3):
                self.budget.reserve()
                head = parent or self.api.repo_info(self.repo_id, repo_type="dataset").sha
                try:
                    result = self.api.create_commit(
                        self.repo_id,
                        repo_type="dataset",
                        operations=[
                            CommitOperationAdd(path_in_repo=k, path_or_fileobj=v) for k, v in batch
                        ],
                        commit_message=message,
                        parent_commit=head,
                    )
                    revision = result.oid
                    break
                except HfHubHTTPError as exc:
                    if exc.response is not None and exc.response.status_code == 429:
                        self.budget.defer(commit_retry_delay(exc))
                        if attempt == 2:
                            raise
                        continue
                    if (
                        parent
                        or exc.response is None
                        or exc.response.status_code != 409
                        or attempt == 2
                    ):
                        raise
                    time.sleep(0.3)
            parent = None
        return revision or self.api.repo_info(self.repo_id, repo_type="dataset").sha


class HFCheckpoints(Checkpoints):
    def __init__(self, local, remote, store, prefix):
        super().__init__(local, remote)
        if not self.safe_name(prefix):
            raise ValueError("Unsafe HF run prefix")
        self.store, self.prefix = store, prefix.rstrip("/")

    def save(self, ids, **kwargs):
        started = time.perf_counter()
        self.check_owner()
        record = super().save(ids, **kwargs)
        index = json.loads((self.remote / "INDEX.json").read_text(encoding="utf-8"))
        previous = self.remote / ".hf-published.json"
        committed = json.loads(previous.read_text()) if previous.exists() else {}
        changes = {
            f"{self.prefix}/{remote_name(name)}": self.remote / name
            for name, sha in index["files"].items()
            if committed.get(name) != sha
        }
        for folder in ("checkpoint-events", "sessions"):
            for path in (self.remote / folder).glob("*.json"):
                changes[f"{self.prefix}/{folder}/{path.name}"] = path
        index_bytes = (self.remote / "INDEX.json").read_bytes()
        changes[f"{self.prefix}/INDEX.json"] = index_bytes
        if (self.remote / "MANIFEST.json").is_file():
            changes[f"{self.prefix}/MANIFEST.json"] = self.remote / "MANIFEST.json"
        for path in (self.local / "book_csv").glob("*"):
            if path.is_file():
                changes[f"{self.prefix}/book_csv/{path.name}"] = path
        revision = self.store.commit(changes)
        if self.store.read(f"{self.prefix}/INDEX.json", revision) != index_bytes:
            raise OSError("HF checkpoint index read-back failed")
        point = dict(
            schema=1, revision=revision, index_sha256=hashlib.sha256(index_bytes).hexdigest()
        )
        self.check_owner()
        self.store.commit(
            {f"{self.prefix}/LATEST.json": json.dumps(point).encode()}, "M1 checkpoint complete"
        )
        write_json(previous, index["files"])
        events_path = self.local / "work/checkpoint_events.jsonl"
        lines = events_path.read_text(encoding="utf-8").splitlines()
        event = json.loads(lines[-1])
        event["seconds"] = time.perf_counter() - started
        lines[-1] = json.dumps(event)
        events_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
        event_path = self.remote / "checkpoint-events" / f"{record['generation']}.json"
        write_json(event_path, event)
        self.store.commit(
            {f"{self.prefix}/checkpoint-events/{event_path.name}": event_path},
            "M1 checkpoint timing",
        )
        return record

    def check_owner(self):
        token = getattr(self.store, "owner_token", None)
        if token and json.loads(self.store.read("SHIFT.json")).get("token") != token:
            raise RuntimeError("HF shift ownership changed; checkpoint not published")

    def restore(self):
        point = json.loads(self.store.read(f"{self.prefix}/LATEST.json"))
        revision = point["revision"]
        data = self.store.read(f"{self.prefix}/INDEX.json", revision)
        if hashlib.sha256(data).hexdigest() != point["index_sha256"]:
            raise ValueError("HF checkpoint index checksum mismatch")
        index = json.loads(data)

        def restore_file(item):
            name, sha = item
            if not self.safe_name(name):
                raise ValueError("Unsafe checkpoint member")
            content = self.store.read(f"{self.prefix}/{remote_name(name)}", revision)
            if hashlib.sha256(content).hexdigest() != sha:
                raise ValueError(f"HF restored file mismatch: {name}")
            path = self.remote / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(content)

        items = list(index["files"].items())
        with ThreadPoolExecutor(max_workers=4) as workers:
            for start in range(0, len(items), 4):
                list(workers.map(restore_file, items[start : start + 4]))
        for name in index["directories"]:
            if not self.safe_name(name):
                raise ValueError("Unsafe checkpoint directory")
            (self.remote / name).mkdir(parents=True, exist_ok=True)
        (self.remote / "INDEX.json").write_bytes(data)
        # Independent telemetry may be newer than the committed corpus generation.
        prefixes = tuple(
            f"{self.prefix}/{folder}/"
            for folder in ("measurements", "sessions", "checkpoint-events")
        )
        for name in self.store.files():
            if name.startswith(prefixes):
                rel = name[len(self.prefix) + 1 :]
                if not self.safe_name(rel):
                    raise ValueError("Unsafe HF telemetry path")
                path = self.remote / rel
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_bytes(self.store.read(name))
        result = super().restore()
        write_json(self.remote / ".hf-published.json", index["files"])
        return result
