"""
Tests for generator.py — file generation, adler32 correctness, atomic placement,
Pool A threading, and resume behaviour.

All tests use temporary directories so no files are written outside /tmp.
The RucioManager is mocked: lfns2pfn returns a deterministic path based on
the LFN, matching how a POSIX-mounted deterministic RSE would behave.
"""

import os
import zlib
from unittest.mock import MagicMock, patch, call

import pytest

from dataset_generator.config import Config
from dataset_generator.generator import (
    CHUNK_SIZE,
    _generate_one,
    _place_file,
    _state_key,
    _write_file,
    run_generation,
)
from dataset_generator.state import FileStatus, StateFile


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def rse_mount(tmp_path):
    """A temporary directory acting as the POSIX RSE mount point."""
    mount = tmp_path / "rse_mount"
    mount.mkdir()
    return str(mount)


@pytest.fixture
def config(rse_mount):
    return Config(
        scope="test",
        rse="TEST_RSE",
        rse_mount=rse_mount,
        dataset_prefix="ds",
        file_prefix="file",
        num_files=3,
        file_size_bytes=1024,   # small for fast tests
        token_endpoint="https://iam.example.org/token",
        client_id="cid",
        client_secret="csec",
        rucio_host="https://rucio.example.org",
        rucio_auth_host="https://rucio-auth.example.org",
        rucio_account="acct",
        run_id="test000000",
        dry_run=False,
    )


@pytest.fixture
def state(tmp_path):
    return StateFile(path=str(tmp_path / "state.json"), run_id="test000000")


@pytest.fixture
def mock_rucio(rse_mount):
    """Mock RucioManager whose lfns2pfn returns a predictable POSIX path."""
    manager = MagicMock()

    def _lfns2pfn(rse, lfn):
        # Simulates deterministic POSIX PFN: {rse_mount}/{scope}/{lfn_name}
        _, lfn_name = lfn.split(":", 1)
        return os.path.join(rse_mount, "test", lfn_name)

    manager.lfns2pfn.side_effect = _lfns2pfn
    return manager


# ---------------------------------------------------------------------------
# _state_key
# ---------------------------------------------------------------------------

class TestStateKey:
    def test_format(self):
        assert _state_key(0) == "file_000000"
        assert _state_key(1) == "file_000001"
        assert _state_key(999999) == "file_999999"


# ---------------------------------------------------------------------------
# _write_file — adler32 correctness
# ---------------------------------------------------------------------------

class TestWriteFile:
    def test_creates_file_of_correct_size(self, tmp_path):
        path = str(tmp_path / "test_file")
        checksum, size = _write_file(path, 4096)
        assert size == 4096
        assert os.path.getsize(path) == 4096

    def test_adler32_mask_applied(self, tmp_path):
        """Checksum value must fit in 32 bits (result of & 0xFFFFFFFF)."""
        path = str(tmp_path / "test_file")
        checksum, _ = _write_file(path, 512)
        assert 0 <= checksum <= 0xFFFFFFFF

    def test_adler32_matches_manual_computation(self, tmp_path):
        """Write a known byte sequence and verify the checksum externally."""
        path = str(tmp_path / "known_file")
        # Patch os.urandom to return deterministic data
        fixed_data = b"A" * 512
        with patch("os.urandom", return_value=fixed_data):
            checksum, size = _write_file(path, 512)

        expected = zlib.adler32(fixed_data, 1) & 0xFFFFFFFF
        assert checksum == expected
        assert size == 512

    def test_incremental_checksum_over_multiple_chunks(self, tmp_path):
        """
        File larger than CHUNK_SIZE: checksum must equal the result of
        running zlib.adler32 over the concatenated chunks.
        """
        chunk = b"B" * CHUNK_SIZE
        second = b"C" * 512
        all_data = [chunk, second]
        call_count = [0]

        def fake_urandom(n):
            data = all_data[call_count[0]]
            call_count[0] += 1
            return data

        path = str(tmp_path / "big_file")
        with patch("os.urandom", side_effect=fake_urandom):
            checksum, size = _write_file(path, CHUNK_SIZE + 512)

        expected = zlib.adler32(chunk, 1) & 0xFFFFFFFF
        expected = zlib.adler32(second, expected) & 0xFFFFFFFF
        assert checksum == expected
        assert size == CHUNK_SIZE + 512

    def test_file_contents_readable(self, tmp_path):
        path = str(tmp_path / "readable")
        _write_file(path, 256)
        with open(path, "rb") as fh:
            content = fh.read()
        assert len(content) == 256


# ---------------------------------------------------------------------------
# _place_file
# ---------------------------------------------------------------------------

class TestPlaceFile:
    def test_creates_parent_dirs(self, tmp_path):
        tmp_file = str(tmp_path / "source.tmp")
        with open(tmp_file, "wb") as fh:
            fh.write(b"hello")

        final = str(tmp_path / "a" / "b" / "c" / "final.dat")
        _place_file(tmp_file, final)

        assert os.path.exists(final)
        assert not os.path.exists(tmp_file)

    def test_atomic_rename_on_same_filesystem(self, tmp_path):
        tmp_file = str(tmp_path / "src.tmp")
        with open(tmp_file, "wb") as fh:
            fh.write(b"data")
        final = str(tmp_path / "dst.dat")
        _place_file(tmp_file, final)
        with open(final, "rb") as fh:
            assert fh.read() == b"data"


# ---------------------------------------------------------------------------
# _generate_one
# ---------------------------------------------------------------------------

class TestGenerateOne:
    def _make_progress(self):
        import threading
        lock = threading.Lock()
        pbar = MagicMock()
        return lock, pbar

    def test_generates_file_and_updates_state(self, config, state, mock_rucio):
        state.allocate(_state_key(0))
        lock, pbar = self._make_progress()
        result = _generate_one(0, config, state, mock_rucio, lock, pbar)

        assert result["key"] == "file_000000"
        assert len(result["adler32"]) == 8
        assert result["bytes"] == config.file_size_bytes
        assert result["lfn"].startswith("test:")
        assert os.path.exists(result["pfn"])

        entry = state.get_file("file_000000")
        assert entry["status"] == FileStatus.CREATED

    def test_lfn_name_includes_checksum(self, config, state, mock_rucio):
        state.allocate(_state_key(0))
        lock, pbar = self._make_progress()
        result = _generate_one(0, config, state, mock_rucio, lock, pbar)
        # LFN name format: {prefix}_{8-char-hex}
        parts = result["lfn_name"].rsplit("_", 1)
        assert parts[0] == config.file_prefix
        assert len(parts[1]) == 8
        assert parts[1] == result["adler32"]

    def test_no_temp_file_left_after_success(self, config, state, mock_rucio, rse_mount):
        state.allocate(_state_key(0))
        lock, pbar = self._make_progress()
        _generate_one(0, config, state, mock_rucio, lock, pbar)
        tmp_dir = os.path.join(rse_mount, ".gen_tmp")
        if os.path.exists(tmp_dir):
            assert not any(f.endswith(".tmp") for f in os.listdir(tmp_dir) if ".tmp." in f)

    def test_failure_marks_state_failed_creation(self, config, state, rse_mount):
        state.allocate(_state_key(0))
        bad_rucio = MagicMock()
        bad_rucio.lfns2pfn.side_effect = RuntimeError("network down")
        lock, pbar = self._make_progress()

        with pytest.raises(RuntimeError, match="network down"):
            _generate_one(0, config, state, bad_rucio, lock, pbar)

        entry = state.get_file("file_000000")
        assert entry["status"] == FileStatus.FAILED_CREATION

    def test_dry_run_does_not_write_files(self, config, state, mock_rucio, rse_mount):
        config.dry_run = True
        state.allocate(_state_key(0))
        lock, pbar = self._make_progress()
        result = _generate_one(0, config, state, mock_rucio, lock, pbar)

        # No physical file should exist
        assert not os.path.exists(result["pfn"])
        # But state must still be updated
        entry = state.get_file("file_000000")
        assert entry["status"] == FileStatus.CREATED


# ---------------------------------------------------------------------------
# run_generation — full pool integration
# ---------------------------------------------------------------------------

class TestRunGeneration:
    def test_generates_all_files(self, config, state, mock_rucio):
        results = run_generation(config, state, mock_rucio)
        assert len(results) == config.num_files
        for r in results:
            assert os.path.exists(r["pfn"])

    def test_all_files_in_created_state(self, config, state, mock_rucio):
        run_generation(config, state, mock_rucio)
        created = state.get_files_by_status(FileStatus.CREATED)
        assert len(created) == config.num_files

    def test_resumes_skips_already_created(self, config, state, mock_rucio, rse_mount):
        """First run creates files; second run skips all of them."""
        run_generation(config, state, mock_rucio)
        first_call_count = mock_rucio.lfns2pfn.call_count

        run_generation(config, state, mock_rucio)
        # No additional lfns2pfn calls on resume
        assert mock_rucio.lfns2pfn.call_count == first_call_count

    def test_resumes_retries_failed_files(self, config, state, mock_rucio, rse_mount):
        """If a file has FAILED_CREATION, run_generation retries it."""
        # Pre-allocate and mark file_000000 as failed
        state.allocate("file_000000")
        state.update("file_000000", status=FileStatus.FAILED_CREATION)
        # Pre-allocate others as already done (CREATED)
        for idx in range(1, config.num_files):
            key = _state_key(idx)
            state.allocate(key)
            state.update(key, status=FileStatus.CREATED, lfn="test:x",
                         lfn_name="x", pfn="/p/x", bytes=512, adler32="deadbeef")

        results = run_generation(config, state, mock_rucio)
        # file_000000 should now be CREATED (retry succeeded)
        entry = state.get_file("file_000000")
        assert entry["status"] == FileStatus.CREATED

    def test_checksum_hex_is_8_char_lowercase(self, config, state, mock_rucio):
        results = run_generation(config, state, mock_rucio)
        for r in results:
            assert len(r["adler32"]) == 8
            assert r["adler32"] == r["adler32"].lower()
            assert r["adler32"] == r["lfn_name"].split("_")[-1]

    def test_multiple_files_get_unique_checksums(self, config, state, mock_rucio):
        """Distinct random files should (almost certainly) have distinct checksums."""
        results = run_generation(config, state, mock_rucio)
        checksums = [r["adler32"] for r in results]
        # Not guaranteed but extremely unlikely to collide for random data
        assert len(set(checksums)) > 1 or config.num_files == 1

    def test_dry_run_returns_correct_count(self, config, state, mock_rucio):
        config.dry_run = True
        results = run_generation(config, state, mock_rucio)
        assert len(results) == config.num_files
