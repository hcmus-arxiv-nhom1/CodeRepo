# Drafted with the help of an AI assistant (Claude, Anthropic), cited per the course rules.
import pytest

from src.crawler.worklock import WorkDirLock, WorkDirLockedError


def test_second_lock_on_same_work_dir_is_refused(tmp_path):
    with WorkDirLock(tmp_path):
        with pytest.raises(WorkDirLockedError):
            WorkDirLock(tmp_path).acquire()


def test_lock_is_free_again_after_release(tmp_path):
    first = WorkDirLock(tmp_path)
    first.acquire()
    first.release()
    with WorkDirLock(tmp_path):  # no stale lock file to clean up
        pass


def test_different_work_dirs_do_not_block_each_other(tmp_path):
    with WorkDirLock(tmp_path / "a"), WorkDirLock(tmp_path / "b"):
        pass
