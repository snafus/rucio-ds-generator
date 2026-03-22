"""
Tests for __main__.py pipeline helpers — focused on error paths not covered
by other test modules.
"""

import sys
import os
from types import ModuleType
from unittest.mock import MagicMock, patch

import pytest

# dotenv is a runtime dep not installed in the test venv; stub it out so
# __main__ can be imported without it.
if "dotenv" not in sys.modules:
    _stub = ModuleType("dotenv")
    _stub.load_dotenv = lambda *a, **kw: None  # type: ignore
    sys.modules["dotenv"] = _stub

from dataset_generator.__main__ import _run_full_pipeline, _run_register_only
from dataset_generator.config import Config
from dataset_generator.state import FileStatus, StateFile


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def rse_mount(tmp_path):
    mount = tmp_path / "rse_mount"
    mount.mkdir()
    return str(mount)


@pytest.fixture
def config(rse_mount, tmp_path):
    staging = str(tmp_path / "staging")
    os.makedirs(staging)
    return Config(
        scope="test",
        rse="TEST_RSE",
        rse_mount=rse_mount,
        dataset_prefix="ds",
        file_prefix="file",
        num_files=2,
        file_size_bytes=512,
        token_endpoint="https://iam.example.org/token",
        client_id="cid",
        client_secret="csec",
        rucio_host="https://rucio.example.org",
        rucio_auth_host="https://rucio-auth.example.org",
        rucio_account="acct",
        run_id="test000000",
        dry_run=False,
        staging_dir=staging,
    )


@pytest.fixture
def state(tmp_path):
    return StateFile(path=str(tmp_path / "state.json"), run_id="test000000")


@pytest.fixture
def mock_rucio(rse_mount):
    manager = MagicMock()
    def _lfns2pfn(rse, lfn):
        _, lfn_name = lfn.split(":", 1)
        return os.path.join(rse_mount, "test", lfn_name)
    manager.lfns2pfn.side_effect = _lfns2pfn
    manager.count_dataset_files.return_value = 0
    return manager


# ---------------------------------------------------------------------------
# _run_full_pipeline — add_replication_rule failure
# ---------------------------------------------------------------------------

class TestFullPipelineRuleFailure:
    def _seed_registered(self, state, count=2):
        """Pre-populate state with REGISTERED entries; return them as dicts."""
        entries = []
        for i in range(count):
            key = "file_{:06d}".format(i)
            adler = "{:08x}".format(0xAABBCC00 + i)
            state.allocate(key)
            state.update(key, status=FileStatus.REGISTERED,
                         lfn="test:file_{}".format(adler),
                         lfn_name="file_{}".format(adler),
                         bytes=512, adler32=adler)
            entry = dict(state.get_file(key))
            entry["key"] = key
            entries.append(entry)
        return entries

    def test_rule_failure_sets_failed_rule_status(self, config, state, mock_rucio):
        """If add_replication_rule raises, REGISTERED entries must transition to FAILED_RULE."""
        mock_rucio.add_replication_rule.side_effect = RuntimeError("quota exceeded")
        entries = self._seed_registered(state, count=2)

        # run_generation returns the already-registered entries as already_done.
        with patch("dataset_generator.__main__.run_generation", return_value=entries):
            failures = _run_full_pipeline(config, state, mock_rucio)

        # Rule creation counts as one failure regardless of how many files are affected.
        assert failures == 1
        assert state.get_file("file_000000")["status"] == FileStatus.FAILED_RULE
        assert state.get_file("file_000001")["status"] == FileStatus.FAILED_RULE

    def test_rule_failure_returns_nonzero(self, config, state, mock_rucio):
        """_run_full_pipeline must return a non-zero failure count when rule creation fails."""
        mock_rucio.add_replication_rule.side_effect = RuntimeError("timeout")
        entries = self._seed_registered(state, count=1)

        with patch("dataset_generator.__main__.run_generation", return_value=entries):
            failures = _run_full_pipeline(config, state, mock_rucio)

        assert failures > 0


# ---------------------------------------------------------------------------
# _run_register_only — add_replication_rule failure
# ---------------------------------------------------------------------------

class TestRegisterOnlyRuleFailure:
    def test_rule_failure_sets_failed_rule_status(self, config, state, mock_rucio):
        """If add_replication_rule raises in register-only mode, entries go to FAILED_RULE."""
        mock_rucio.add_replication_rule.side_effect = RuntimeError("quota exceeded")

        state.allocate("file_000000")
        state.update("file_000000", status=FileStatus.CREATED,
                     lfn="test:file_aabbccdd", lfn_name="file_aabbccdd",
                     bytes=512, adler32="aabbccdd")

        # _register_replicas will mark it REGISTERED; then rule creation fails.
        mock_rucio.add_replicas.return_value = None

        failures = _run_register_only(config, state, mock_rucio)

        assert failures > 0
        # Entry must be FAILED_RULE, not stuck in REGISTERED.
        entry = state.get_file("file_000000")
        assert entry["status"] == FileStatus.FAILED_RULE

    def test_rule_failure_does_not_leave_registered_entries(self, config, state, mock_rucio):
        """No entry should remain in REGISTERED after a rule failure."""
        mock_rucio.add_replication_rule.side_effect = RuntimeError("network error")
        mock_rucio.add_replicas.return_value = None

        for i in range(3):
            key = "file_{:06d}".format(i)
            state.allocate(key)
            state.update(key, status=FileStatus.CREATED,
                         lfn="test:file_{:08x}".format(i),
                         lfn_name="file_{:08x}".format(i),
                         bytes=512, adler32="{:08x}".format(i))

        _run_register_only(config, state, mock_rucio)

        registered = state.get_files_by_status(FileStatus.REGISTERED)
        assert registered == [], "entries stuck in REGISTERED: {}".format(registered)
