"""
Tests for registry.py — DatasetRegistry persistence and upsert logic.
"""

import json
import os
import threading

import pytest

from dataset_generator.registry import DatasetRegistry, DEFAULT_REGISTRY_FILE


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_registry(tmp_path, filename="registry.json"):
    path = str(tmp_path / filename)
    return DatasetRegistry(path), path


# ---------------------------------------------------------------------------
# DEFAULT_REGISTRY_FILE
# ---------------------------------------------------------------------------

def test_default_registry_file_in_home():
    home = os.path.expanduser("~")
    assert DEFAULT_REGISTRY_FILE.startswith(home)
    assert DEFAULT_REGISTRY_FILE.endswith("registry.json")


# ---------------------------------------------------------------------------
# Fresh registry (no file on disk)
# ---------------------------------------------------------------------------

class TestFreshRegistry:
    def test_entries_empty_on_new_registry(self, tmp_path):
        reg, _ = _make_registry(tmp_path)
        assert reg.entries() == []

    def test_record_creates_file(self, tmp_path):
        reg, path = _make_registry(tmp_path)
        reg.record("cms:ds1", "T2_US_TEST", "rule-abc", 5)
        assert os.path.exists(path)

    def test_record_file_is_valid_json(self, tmp_path):
        reg, path = _make_registry(tmp_path)
        reg.record("cms:ds1", "T2_US_TEST", "rule-abc", 5)
        with open(path) as fh:
            data = json.load(fh)
        assert data["version"] == 1
        assert "datasets" in data

    def test_record_single_entry(self, tmp_path):
        reg, _ = _make_registry(tmp_path)
        reg.record("cms:ds1", "T2_US_TEST", "rule-abc", 5)
        entries = reg.entries()
        assert len(entries) == 1
        e = entries[0]
        assert e["dataset_did"] == "cms:ds1"
        assert e["rse"] == "T2_US_TEST"
        assert e["rule_id"] == "rule-abc"
        assert e["num_files"] == 5
        assert "first_seen" in e
        assert "last_updated" in e


# ---------------------------------------------------------------------------
# Upsert / cumulative logic
# ---------------------------------------------------------------------------

class TestUpsert:
    def test_num_files_is_cumulative(self, tmp_path):
        reg, _ = _make_registry(tmp_path)
        reg.record("cms:ds1", "T2_US_TEST", "rule-1", 3)
        reg.record("cms:ds1", "T2_US_TEST", "rule-2", 7)
        e = reg.entries()[0]
        assert e["num_files"] == 10

    def test_rule_id_updated_on_second_call(self, tmp_path):
        reg, _ = _make_registry(tmp_path)
        reg.record("cms:ds1", "T2_US_TEST", "rule-1", 3)
        reg.record("cms:ds1", "T2_US_TEST", "rule-2", 1)
        e = reg.entries()[0]
        assert e["rule_id"] == "rule-2"

    def test_rule_id_preserved_when_none(self, tmp_path):
        """When rule_id is None (DuplicateRule), the previous value is kept."""
        reg, _ = _make_registry(tmp_path)
        reg.record("cms:ds1", "T2_US_TEST", "rule-original", 3)
        reg.record("cms:ds1", "T2_US_TEST", None, 5)
        e = reg.entries()[0]
        assert e["rule_id"] == "rule-original"

    def test_first_seen_not_changed_on_update(self, tmp_path):
        reg, _ = _make_registry(tmp_path)
        reg.record("cms:ds1", "T2_US_TEST", "rule-1", 1)
        first = reg.entries()[0]["first_seen"]
        reg.record("cms:ds1", "T2_US_TEST", "rule-2", 1)
        assert reg.entries()[0]["first_seen"] == first

    def test_multiple_datasets(self, tmp_path):
        reg, _ = _make_registry(tmp_path)
        reg.record("cms:ds1", "T2_US_A", "rule-1", 2)
        reg.record("cms:ds2", "T2_US_B", "rule-2", 8)
        entries = {e["dataset_did"]: e for e in reg.entries()}
        assert len(entries) == 2
        assert entries["cms:ds1"]["num_files"] == 2
        assert entries["cms:ds2"]["num_files"] == 8


# ---------------------------------------------------------------------------
# Persistence across instantiations
# ---------------------------------------------------------------------------

class TestPersistence:
    def test_reload_from_disk(self, tmp_path):
        reg1, path = _make_registry(tmp_path)
        reg1.record("cms:ds1", "T2_US_TEST", "rule-abc", 5)

        reg2 = DatasetRegistry(path)
        entries = reg2.entries()
        assert len(entries) == 1
        assert entries[0]["num_files"] == 5
        assert entries[0]["rule_id"] == "rule-abc"

    def test_cumulative_across_instances(self, tmp_path):
        _, path = _make_registry(tmp_path)
        DatasetRegistry(path).record("cms:ds1", "T2", "r1", 3)
        DatasetRegistry(path).record("cms:ds1", "T2", "r2", 4)
        entries = DatasetRegistry(path).entries()
        assert entries[0]["num_files"] == 7

    def test_corrupt_file_starts_fresh(self, tmp_path):
        path = str(tmp_path / "registry.json")
        with open(path, "w") as fh:
            fh.write("not valid json{{{")
        reg = DatasetRegistry(path)
        assert reg.entries() == []

    def test_wrong_version_starts_fresh(self, tmp_path):
        path = str(tmp_path / "registry.json")
        with open(path, "w") as fh:
            json.dump({"version": 99, "datasets": {"cms:ds1": {}}}, fh)
        reg = DatasetRegistry(path)
        assert reg.entries() == []


# ---------------------------------------------------------------------------
# Atomic write — no .tmp file left behind
# ---------------------------------------------------------------------------

class TestAtomicWrite:
    def test_no_tmp_file_after_record(self, tmp_path):
        reg, path = _make_registry(tmp_path)
        reg.record("cms:ds1", "T2_US_TEST", "rule-1", 1)
        tmp = path + ".tmp"
        assert not os.path.exists(tmp)


# ---------------------------------------------------------------------------
# Thread safety
# ---------------------------------------------------------------------------

class TestThreadSafety:
    def test_concurrent_records_are_consistent(self, tmp_path):
        reg, _ = _make_registry(tmp_path)
        errors = []

        def worker():
            try:
                reg.record("cms:ds1", "T2_US_TEST", "rule-x", 1)
            except Exception as exc:
                errors.append(exc)

        threads = [threading.Thread(target=worker) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        entries = reg.entries()
        assert len(entries) == 1
        assert entries[0]["num_files"] == 20


# ---------------------------------------------------------------------------
# Parent directory creation
# ---------------------------------------------------------------------------

class TestDirectoryCreation:
    def test_creates_parent_dirs(self, tmp_path):
        path = str(tmp_path / "deep" / "nested" / "registry.json")
        reg = DatasetRegistry(path)
        reg.record("cms:ds1", "T2", "r1", 1)
        assert os.path.exists(path)
