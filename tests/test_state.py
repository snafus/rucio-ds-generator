"""
Tests for state.py — StateFile lifecycle, thread safety, and atomic writes.
"""

import json
import os
import threading

import pytest

from dataset_generator.state import FileStatus, StateFile, StateError, STATE_VERSION


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def state_path(tmp_path):
    return str(tmp_path / "state_test.json")


@pytest.fixture
def state(state_path):
    return StateFile(path=state_path, run_id="abc123def456")


# ---------------------------------------------------------------------------
# Creation and loading
# ---------------------------------------------------------------------------

class TestCreation:
    def test_creates_new_file(self, state_path):
        sf = StateFile(path=state_path, run_id="run1")
        assert os.path.exists(state_path)

    def test_new_file_has_correct_schema(self, state_path):
        StateFile(path=state_path, run_id="run1")
        with open(state_path) as fh:
            data = json.load(fh)
        assert data["version"] == STATE_VERSION
        assert data["run_id"] == "run1"
        assert data["files"] == {}

    def test_loads_existing_file(self, state_path):
        # Create initial state
        sf1 = StateFile(path=state_path, run_id="run1")
        sf1.allocate("file_000000")
        sf1.update("file_000000", status=FileStatus.CREATED, lfn="s:f", lfn_name="f",
                   pfn="/p/f", bytes=100, adler32="deadbeef")

        # Load same file in new instance
        sf2 = StateFile(path=state_path, run_id="run1")
        entry = sf2.get_file("file_000000")
        assert entry is not None
        assert entry["status"] == FileStatus.CREATED
        assert entry["lfn"] == "s:f"

    def test_version_mismatch_raises(self, state_path):
        bad_state = {"version": 999, "run_id": "run1", "files": {}}
        with open(state_path, "w") as fh:
            json.dump(bad_state, fh)
        with pytest.raises(StateError, match="unsupported version"):
            StateFile(path=state_path, run_id="run1")

    def test_run_id_mismatch_raises(self, state_path):
        existing = {"version": STATE_VERSION, "run_id": "run_A", "files": {}}
        with open(state_path, "w") as fh:
            json.dump(existing, fh)
        with pytest.raises(StateError, match="run_id"):
            StateFile(path=state_path, run_id="run_B")

    def test_missing_files_key_raises(self, state_path):
        bad_state = {"version": STATE_VERSION, "run_id": "run1"}
        with open(state_path, "w") as fh:
            json.dump(bad_state, fh)
        with pytest.raises(StateError, match="'files'"):
            StateFile(path=state_path, run_id="run1")

    def test_corrupt_json_raises(self, state_path):
        with open(state_path, "w") as fh:
            fh.write("{not valid json")
        with pytest.raises(StateError):
            StateFile(path=state_path, run_id="run1")


# ---------------------------------------------------------------------------
# Allocate
# ---------------------------------------------------------------------------

class TestAllocate:
    def test_allocate_creates_pending_entry(self, state):
        state.allocate("file_000000")
        entry = state.get_file("file_000000")
        assert entry is not None
        assert entry["status"] == FileStatus.PENDING

    def test_allocate_idempotent(self, state):
        state.allocate("file_000000")
        state.update("file_000000", status=FileStatus.CREATED, adler32="aabbccdd")
        state.allocate("file_000000")   # should NOT overwrite existing entry
        entry = state.get_file("file_000000")
        assert entry["status"] == FileStatus.CREATED   # preserved

    def test_allocate_persisted_to_disk(self, state, state_path):
        state.allocate("file_000001")
        with open(state_path) as fh:
            data = json.load(fh)
        assert "file_000001" in data["files"]


# ---------------------------------------------------------------------------
# Update
# ---------------------------------------------------------------------------

class TestUpdate:
    def test_update_sets_fields(self, state):
        state.allocate("file_000000")
        state.update(
            "file_000000",
            status=FileStatus.CREATED,
            lfn="scope:name",
            lfn_name="name",
            pfn="/path/to/name",
            bytes=1024,
            adler32="1a2b3c4d",
        )
        entry = state.get_file("file_000000")
        assert entry["status"] == FileStatus.CREATED
        assert entry["adler32"] == "1a2b3c4d"
        assert entry["bytes"] == 1024

    def test_update_unknown_key_raises(self, state):
        with pytest.raises(StateError, match="not been allocated"):
            state.update("nonexistent_key", status=FileStatus.CREATED)

    def test_update_persisted_immediately(self, state, state_path):
        state.allocate("file_000000")
        state.update("file_000000", status=FileStatus.REGISTERED)
        with open(state_path) as fh:
            data = json.load(fh)
        assert data["files"]["file_000000"]["status"] == FileStatus.REGISTERED


# ---------------------------------------------------------------------------
# get_file / get_files_by_status
# ---------------------------------------------------------------------------

class TestQuery:
    def test_get_file_returns_copy(self, state):
        state.allocate("file_000000")
        entry1 = state.get_file("file_000000")
        entry1["status"] = "mutated"
        entry2 = state.get_file("file_000000")
        assert entry2["status"] == FileStatus.PENDING   # internal state unchanged

    def test_get_file_missing_returns_none(self, state):
        assert state.get_file("nonexistent") is None

    def test_get_files_by_status_filters_correctly(self, state):
        for idx in range(4):
            key = "file_{:06d}".format(idx)
            state.allocate(key)

        state.update("file_000000", status=FileStatus.CREATED)
        state.update("file_000001", status=FileStatus.REGISTERED)
        state.update("file_000002", status=FileStatus.RULED)
        state.update("file_000003", status=FileStatus.FAILED_CREATION)

        created = state.get_files_by_status(FileStatus.CREATED)
        assert len(created) == 1
        assert created[0]["key"] == "file_000000"

        failed = state.get_files_by_status(
            FileStatus.FAILED_CREATION,
            FileStatus.FAILED_REGISTRATION,
        )
        assert len(failed) == 1

        done = state.get_files_by_status(FileStatus.REGISTERED, FileStatus.RULED)
        assert len(done) == 2

    def test_get_files_by_status_includes_key(self, state):
        state.allocate("file_000000")
        state.update("file_000000", status=FileStatus.CREATED)
        results = state.get_files_by_status(FileStatus.CREATED)
        assert results[0]["key"] == "file_000000"


# ---------------------------------------------------------------------------
# Snapshot
# ---------------------------------------------------------------------------

class TestSnapshot:
    def test_snapshot_is_deep_copy(self, state):
        state.allocate("file_000000")
        snap = state.snapshot()
        snap["files"]["file_000000"]["status"] = "mutated"
        entry = state.get_file("file_000000")
        assert entry["status"] == FileStatus.PENDING   # internal unchanged


# ---------------------------------------------------------------------------
# Atomic write
# ---------------------------------------------------------------------------

class TestAtomicWrite:
    def test_no_tmp_file_left_behind(self, state, state_path, tmp_path):
        state.allocate("file_000000")
        state.update("file_000000", status=FileStatus.CREATED)
        tmp_files = [f for f in os.listdir(tmp_path) if f.endswith(".tmp")]
        assert tmp_files == []

    def test_state_file_valid_json_after_update(self, state, state_path):
        state.allocate("file_000000")
        state.update("file_000000", status=FileStatus.CREATED)
        with open(state_path) as fh:
            data = json.load(fh)
        assert data["files"]["file_000000"]["status"] == FileStatus.CREATED


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------

class TestThreadSafety:
    def test_concurrent_allocate_and_update(self, state_path):
        """
        Spin up 20 threads each allocating a unique key and updating it.
        No key should be lost and the final state file must be valid JSON.
        """
        sf = StateFile(path=state_path, run_id="threadtest")
        n = 20
        errors = []

        def worker(idx):
            key = "file_{:06d}".format(idx)
            try:
                sf.allocate(key)
                sf.update(key, status=FileStatus.CREATED, bytes=idx * 100)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(n)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == [], "Thread errors: {}".format(errors)
        with open(state_path) as fh:
            data = json.load(fh)
        assert len(data["files"]) == n

    def test_concurrent_updates_preserve_all_entries(self, state_path):
        sf = StateFile(path=state_path, run_id="threadtest2")
        keys = ["file_{:06d}".format(i) for i in range(10)]
        for k in keys:
            sf.allocate(k)

        barrier = threading.Barrier(len(keys))
        errors = []

        def worker(key):
            try:
                barrier.wait()   # all threads start updates simultaneously
                sf.update(key, status=FileStatus.REGISTERED)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker, args=(k,)) for k in keys]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        for k in keys:
            assert sf.get_file(k)["status"] == FileStatus.REGISTERED


# ---------------------------------------------------------------------------
# FileStatus constants
# ---------------------------------------------------------------------------

class TestFileStatus:
    def test_terminal_statuses(self):
        assert FileStatus.RULED in FileStatus.TERMINAL
        assert FileStatus.CREATED not in FileStatus.TERMINAL

    def test_retriable_statuses(self):
        assert FileStatus.PENDING in FileStatus.RETRIABLE
        assert FileStatus.FAILED_CREATION in FileStatus.RETRIABLE
        assert FileStatus.FAILED_REGISTRATION in FileStatus.RETRIABLE
        assert FileStatus.FAILED_RULE in FileStatus.RETRIABLE
        assert FileStatus.RULED not in FileStatus.RETRIABLE


# ---------------------------------------------------------------------------
# TestCount
# ---------------------------------------------------------------------------

class TestCount:
    def test_count_zero_on_empty_state(self, tmp_path):
        from dataset_generator.state import StateFile
        sf = StateFile(str(tmp_path / "state.json"), run_id="aabbccddeeff")
        assert sf.count() == 0

    def test_count_reflects_allocated_entries(self, tmp_path):
        from dataset_generator.state import StateFile
        sf = StateFile(str(tmp_path / "state.json"), run_id="aabbccddeeff")
        sf.allocate("file_000000")
        sf.allocate("file_000001")
        sf.allocate("file_000002")
        assert sf.count() == 3

    def test_count_unchanged_by_update(self, tmp_path):
        from dataset_generator.state import StateFile
        from dataset_generator.state import FileStatus
        sf = StateFile(str(tmp_path / "state.json"), run_id="aabbccddeeff")
        sf.allocate("file_000000")
        sf.update("file_000000", status=FileStatus.CREATED)
        assert sf.count() == 1
