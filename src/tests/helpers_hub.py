"""Versioned in-memory Hub store for offline backend tests."""

from pathlib import Path


class MemoryHub:
    def __init__(self):
        self.revisions = {"0": {}}
        self.head = "0"
        self.fail = False

    def get_head(self):
        return self.head

    def commit(self, files, message="checkpoint", parent=None):
        if self.fail:
            raise OSError("upload interrupted")
        if parent is not None and parent != self.head:
            raise ValueError("parent commit mismatch")
        tree = dict(self.revisions[self.head])
        tree.update({k: (v.read_bytes() if isinstance(v, Path) else v) for k, v in files.items()})
        self.head = str(len(self.revisions))
        self.revisions[self.head] = tree
        return self.head

    def read(self, path, revision="main", missing_ok=False):
        data = self.revisions[self.head if revision == "main" else revision].get(path)
        if data is None and not missing_ok:
            raise FileNotFoundError(path)
        return data

    def files(self):
        return list(self.revisions[self.head])
