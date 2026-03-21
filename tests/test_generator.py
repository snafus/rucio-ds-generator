"""
Tests for generator.py — file generation, adler32 correctness, atomic placement,
Pool A multiprocessing, and resume behaviour.

All tests use temporary directories so no files are written outside /tmp.
The RucioManager is mocked: lfns2pfn returns a deterministic path based on
the LFN, matching how a POSIX-mounted deterministic RSE would behave.
"""

import errno
import os
import zlib
from unittest.mock import MagicMock, patch, call

import pytest

import dataset_generator.generator as _gen_module
from dataset_generator.config import Config
from dataset_generator.generator import (
    _makedirs_chown,
    _pfn_to_local,
    _place_file,
    _state_key,
    _write_one_worker,
    run_generation,
)
from dataset_generator.state import FileStatus, StateFile
from dataset_generator.writers import (
    CHUNK_SIZE,
    CsprngFileWriter,
    _randbytes,  # noqa: F401 — imported to allow patch target verification
)


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
def config(rse_mount, tmp_path):
    # Each test gets its own unique staging subdir, mirroring what main() does.
    staging = str(tmp_path / "staging")
    os.makedirs(staging)
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
        staging_dir=staging,
        # rse_pfn_prefix=None: mock_rucio returns paths under rse_mount directly
    )


@pytest.fixture
def state(tmp_path):
    return StateFile(path=str(tmp_path / "state.json"), run_id="test000000")


@pytest.fixture
def writer():
    """Default CsprngFileWriter instance for use in generation tests."""
    return CsprngFileWriter()


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
    """Tests for CsprngFileWriter.write_file (the default FileWriter implementation)."""

    @pytest.fixture
    def writer(self):
        return CsprngFileWriter()

    def test_creates_file_of_correct_size(self, tmp_path, writer):
        path = str(tmp_path / "test_file")
        checksum, size = writer.write_file(path, 4096)
        assert size == 4096
        assert os.path.getsize(path) == 4096

    def test_adler32_mask_applied(self, tmp_path, writer):
        """Checksum value must fit in 32 bits (result of & 0xFFFFFFFF)."""
        path = str(tmp_path / "test_file")
        checksum, _ = writer.write_file(path, 512)
        assert 0 <= checksum <= 0xFFFFFFFF

    def test_adler32_matches_manual_computation(self, tmp_path, writer):
        """Write a known byte sequence and verify the checksum externally."""
        path = str(tmp_path / "known_file")
        # Patch the module-level _randbytes in writers so the test is independent
        # of which PRNG was selected at import time.
        fixed_data = b"A" * 512
        with patch("dataset_generator.writers._randbytes", return_value=fixed_data):
            checksum, size = writer.write_file(path, 512)

        expected = zlib.adler32(fixed_data, 1) & 0xFFFFFFFF
        assert checksum == expected
        assert size == 512

    def test_incremental_checksum_over_multiple_chunks(self, tmp_path, writer):
        """
        File larger than CHUNK_SIZE: checksum must equal the result of
        running zlib.adler32 over the concatenated chunks.
        """
        chunk = b"B" * CHUNK_SIZE
        second = b"C" * 512
        all_data = [chunk, second]
        call_count = [0]

        def fake_randbytes(n):
            data = all_data[call_count[0]]
            call_count[0] += 1
            return data

        path = str(tmp_path / "big_file")
        with patch("dataset_generator.writers._randbytes", side_effect=fake_randbytes):
            checksum, size = writer.write_file(path, CHUNK_SIZE + 512)

        expected = zlib.adler32(chunk, 1) & 0xFFFFFFFF
        expected = zlib.adler32(second, expected) & 0xFFFFFFFF
        assert checksum == expected
        assert size == CHUNK_SIZE + 512

    def test_file_contents_readable(self, tmp_path, writer):
        path = str(tmp_path / "readable")
        writer.write_file(path, 256)
        with open(path, "rb") as fh:
            content = fh.read()
        assert len(content) == 256

    def test_size_mismatch_raises(self, tmp_path, writer):
        """If the on-disk size does not match the requested size, raise RuntimeError."""
        path = str(tmp_path / "truncated")
        real_stat = os.stat
        def fake_stat(p, **kw):
            result = real_stat(p, **kw)
            return os.stat_result((
                result.st_mode, result.st_ino, result.st_dev,
                result.st_nlink, result.st_uid, result.st_gid,
                0,  # st_size — wrong on purpose
                result.st_atime, result.st_mtime, result.st_ctime,
            ))
        with patch("os.stat", side_effect=fake_stat):
            with pytest.raises(RuntimeError, match="File size mismatch"):
                writer.write_file(path, 256)


# ---------------------------------------------------------------------------
# _place_file
# ---------------------------------------------------------------------------

class TestMakedirsChown:
    def test_creates_nested_dirs(self, tmp_path):
        target = str(tmp_path / "a" / "b" / "c")
        _makedirs_chown(target, uid=None, gid=None)
        assert os.path.isdir(target)

    def test_idempotent_when_dir_exists(self, tmp_path):
        target = str(tmp_path / "exists")
        os.makedirs(target)
        _makedirs_chown(target, uid=None, gid=None)  # should not raise
        assert os.path.isdir(target)

    def test_chown_called_for_new_dirs_only(self, tmp_path):
        existing = tmp_path / "existing"
        existing.mkdir()
        target = str(existing / "new1" / "new2")

        with patch("os.chown") as mock_chown:
            _makedirs_chown(target, uid=1000, gid=2000)

        # Only the two new dirs should be chowned, not the pre-existing one
        chowned_paths = [c[0][0] for c in mock_chown.call_args_list]
        assert str(existing / "new1") in chowned_paths
        assert target in chowned_paths
        assert str(existing) not in chowned_paths

    def test_chown_not_called_when_uid_gid_none(self, tmp_path):
        target = str(tmp_path / "d1" / "d2")
        with patch("os.chown") as mock_chown:
            _makedirs_chown(target, uid=None, gid=None)
        mock_chown.assert_not_called()

    def test_uid_only_uses_minus1_for_gid(self, tmp_path):
        target = str(tmp_path / "d")
        with patch("os.chown") as mock_chown:
            _makedirs_chown(target, uid=500, gid=None)
        for c in mock_chown.call_args_list:
            assert c[0][1] == 500   # uid
            assert c[0][2] == -1    # gid unchanged


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

    def test_chown_applied_to_part_file_before_rename(self, tmp_path):
        tmp_file = str(tmp_path / "src.tmp")
        with open(tmp_file, "wb") as fh:
            fh.write(b"x")
        final = str(tmp_path / "dst.dat")
        chowned = []
        real_chown = os.chown
        def capture_chown(path, uid, gid):
            chowned.append((path, uid, gid))
        with patch("os.chown", side_effect=capture_chown):
            with patch("dataset_generator.generator._makedirs_chown"):
                _place_file(tmp_file, final, uid=42, gid=99)
        # chown must have been called on a .part path, not the final path
        assert len(chowned) == 1
        part_path, uid_arg, gid_arg = chowned[0]
        assert ".part." in part_path
        assert uid_arg == 42
        assert gid_arg == 99
        # Final file exists and staging is gone
        assert os.path.exists(final)
        assert not os.path.exists(tmp_file)

    def test_no_chown_when_uid_gid_none(self, tmp_path):
        tmp_file = str(tmp_path / "src.tmp")
        with open(tmp_file, "wb") as fh:
            fh.write(b"x")
        final = str(tmp_path / "dst.dat")
        with patch("os.chown") as mock_chown:
            _place_file(tmp_file, final, uid=None, gid=None)
        mock_chown.assert_not_called()

    def test_cross_filesystem_falls_back_to_copy(self, tmp_path):
        tmp_file = str(tmp_path / "src.tmp")
        with open(tmp_file, "wb") as fh:
            fh.write(b"crossfs")
        final = str(tmp_path / "dst.dat")
        exdev = OSError()
        exdev.errno = errno.EXDEV
        # First rename raises EXDEV (cross-fs); subsequent ones succeed normally
        rename_calls = [0]
        real_rename = os.rename
        def patched_rename(src, dst):
            if rename_calls[0] == 0:
                rename_calls[0] += 1
                raise exdev
            return real_rename(src, dst)
        with patch("os.rename", side_effect=patched_rename):
            _place_file(tmp_file, final)
        assert os.path.exists(final)
        with open(final, "rb") as fh:
            assert fh.read() == b"crossfs"

    def test_part_file_cleaned_up_on_crossfs_copy_failure(self, tmp_path):
        """If shutil.copy2 fails on a cross-filesystem move, the partial .part must be removed."""
        tmp_file = str(tmp_path / "src.tmp")
        with open(tmp_file, "wb") as fh:
            fh.write(b"data")
        final = str(tmp_path / "dst.dat")

        exdev = OSError()
        exdev.errno = errno.EXDEV
        rename_calls = [0]
        real_rename = os.rename
        def patched_rename(src, dst):
            if rename_calls[0] == 0:
                rename_calls[0] += 1
                raise exdev
            return real_rename(src, dst)

        with patch("os.rename", side_effect=patched_rename):
            with patch("shutil.copy2", side_effect=OSError("disk full")):
                with pytest.raises(OSError, match="disk full"):
                    _place_file(tmp_file, final)

        # .part file must not remain on the RSE
        leftover = [f for f in os.listdir(str(tmp_path)) if ".part." in f]
        assert leftover == [], "orphaned .part files: {}".format(leftover)

    def test_part_file_cleaned_up_on_final_rename_failure(self, tmp_path):
        """If the final rename (.part → final) fails, the .part file must be removed."""
        tmp_file = str(tmp_path / "src.tmp")
        with open(tmp_file, "wb") as fh:
            fh.write(b"data")
        final = str(tmp_path / "dst.dat")

        real_rename = os.rename
        rename_calls = [0]
        def patched_rename(src, dst):
            rename_calls[0] += 1
            if rename_calls[0] == 1:
                # First rename: staging → .part (let it succeed so the .part exists)
                return real_rename(src, dst)
            # Second rename: .part → final — simulate failure
            raise OSError("simulated rename failure")

        with patch("os.rename", side_effect=patched_rename):
            with pytest.raises(OSError, match="simulated rename failure"):
                _place_file(tmp_file, final)

        # Final file must not exist
        assert not os.path.exists(final)
        # No .part file should remain in tmp_path
        leftover = [f for f in os.listdir(str(tmp_path)) if ".part." in f]
        assert leftover == [], "orphaned .part files: {}".format(leftover)


# ---------------------------------------------------------------------------
# _pfn_to_local
# ---------------------------------------------------------------------------

class TestPfnToLocal:
    def test_no_prefix_passthrough(self):
        assert _pfn_to_local("/data/rucio/cms/ab/file", None, "/mnt/rse") == "/data/rucio/cms/ab/file"

    def test_strips_file_protocol(self):
        assert _pfn_to_local("file:///data/rucio/cms/ab/file", None, "/mnt/rse") == "/data/rucio/cms/ab/file"

    def test_prefix_replaced_with_mount(self):
        result = _pfn_to_local("/data/rucio/cms/ab/file", "/data/rucio", "/mnt/rse")
        assert result == "/mnt/rse/cms/ab/file"

    def test_file_protocol_url_as_prefix(self):
        # Prefix includes the file:// protocol — matched against raw PFN
        result = _pfn_to_local("file:///data/rucio/cms/ab/file", "file:///data/rucio", "/mnt/rse")
        assert result == "/mnt/rse/cms/ab/file"

    def test_davs_url_as_prefix(self):
        pfn = "davs://storage.example.org:443/rucio/cms/ab/file_abcd1234"
        result = _pfn_to_local(pfn, "davs://storage.example.org:443/rucio", "/mnt/rse")
        assert result == "/mnt/rse/cms/ab/file_abcd1234"

    def test_prefix_mismatch_raises(self):
        with pytest.raises(RuntimeError, match="rse_pfn_prefix"):
            _pfn_to_local("/other/path/file", "/data/rucio", "/mnt/rse")

    def test_unsupported_protocol_raises(self):
        with pytest.raises(RuntimeError, match="Unsupported PFN protocol"):
            _pfn_to_local("gsiftp://storage.example.org/data/file", None, "/mnt/rse")

    def test_unsupported_protocol_with_prefix_succeeds(self):
        # Any protocol is fine as long as rse_pfn_prefix covers it
        pfn = "gsiftp://storage.example.org:2811/rucio/cms/ab/file"
        result = _pfn_to_local(pfn, "gsiftp://storage.example.org:2811/rucio", "/mnt/rse")
        assert result == "/mnt/rse/cms/ab/file"

    def test_trailing_slash_on_prefix(self):
        # rse_pfn_prefix with trailing slash: suffix loses its leading '/';
        # _pfn_to_local must re-add it to avoid "/mnt/rsecms/..." concatenation.
        pfn = "/data/rucio/cms/ab/file"
        result = _pfn_to_local(pfn, "/data/rucio/", "/mnt/rse")
        assert result == "/mnt/rse/cms/ab/file"

    def test_trailing_slash_on_mount(self):
        # rse_mount with trailing slash must not produce double '//'
        pfn = "/data/rucio/cms/ab/file"
        result = _pfn_to_local(pfn, "/data/rucio", "/mnt/rse/")
        assert result == "/mnt/rse/cms/ab/file"

    def test_trailing_slash_on_both(self):
        pfn = "/data/rucio/cms/ab/file"
        result = _pfn_to_local(pfn, "/data/rucio/", "/mnt/rse/")
        assert result == "/mnt/rse/cms/ab/file"


# ---------------------------------------------------------------------------
# _write_one_worker — worker task unit tests
#
# _write_one_worker reads from module-level globals (_proc_config,
# _proc_writer).  Tests set these directly and reset them in teardown so
# they do not leak between tests.
# ---------------------------------------------------------------------------

class TestWriteOneWorker:
    """Tests for the Pool A worker task function."""

    @pytest.fixture(autouse=True)
    def reset_globals(self):
        """Restore module globals to None after every test."""
        yield
        _gen_module._proc_config = None
        _gen_module._proc_writer = None

    def _setup(self, config, writer):
        _gen_module._proc_config = config
        _gen_module._proc_writer = writer

    def test_success_returns_tuple(self, config, writer, tmp_path):
        self._setup(config, writer)
        result = _write_one_worker(0)
        idx, staging_path, checksum_hex, bytes_written, error_str = result
        assert idx == 0
        assert error_str is None
        assert os.path.exists(staging_path)   # staged file present for main process
        assert bytes_written == config.file_size_bytes
        assert len(checksum_hex) == 8
        assert checksum_hex == checksum_hex.lower()

    def test_lfn_name_checksum_is_8_char_hex(self, config, writer):
        self._setup(config, writer)
        _, _, checksum_hex, _, error_str = _write_one_worker(0)
        assert error_str is None
        assert len(checksum_hex) == 8
        assert all(c in "0123456789abcdef" for c in checksum_hex)

    def test_staging_file_present_after_success(self, config, writer):
        """Worker leaves the staging file for the main process to pick up."""
        self._setup(config, writer)
        _, staging_path, _, _, error_str = _write_one_worker(0)
        assert error_str is None
        assert staging_path is not None
        assert os.path.isfile(staging_path)

    def test_failure_returns_error_string(self, config, writer):
        self._setup(config, writer)
        with patch.object(writer, "write_file", side_effect=OSError("disk error")):
            result = _write_one_worker(0)
        idx, staging_path, checksum_hex, bytes_written, error_str = result
        assert idx == 0
        assert staging_path is None
        assert checksum_hex is None
        assert bytes_written is None
        assert "disk error" in error_str

    def test_failure_cleans_up_staging_file(self, config, writer):
        """After a write failure the partial staging file must not remain."""
        self._setup(config, writer)
        staging_dir = config.staging_dir

        # Patch write_file to create a partial file then raise.
        def bad_write(path, size):
            with open(path, "wb") as fh:
                fh.write(b"partial")
            raise OSError("disk full")

        with patch.object(writer, "write_file", side_effect=bad_write):
            _write_one_worker(0)

        remaining = [f for f in os.listdir(staging_dir) if f.endswith(".tmp.000000")]
        assert remaining == [], "orphaned staging file: {}".format(remaining)

    def test_dry_run_skips_write(self, config, writer):
        config.dry_run = True
        self._setup(config, writer)
        with patch.object(writer, "write_file") as mock_write:
            result = _write_one_worker(0)
        mock_write.assert_not_called()
        _, _, checksum_hex, bytes_written, error_str = result
        assert error_str is None
        assert checksum_hex == format(0xDEADBEEF & 0xFFFFFFFF, "08x")
        assert bytes_written == config.file_size_bytes

    def test_different_indices_produce_different_paths(self, config, writer):
        self._setup(config, writer)
        r0 = _write_one_worker(0)
        r1 = _write_one_worker(1)
        assert r0[1] != r1[1]   # staging paths differ


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

    def test_dry_run_does_not_write_files(self, config, state, mock_rucio, rse_mount):
        """In dry-run mode no physical files should appear on the RSE mount."""
        config.dry_run = True
        results = run_generation(config, state, mock_rucio)
        for r in results:
            assert not os.path.exists(r["pfn"])

    def test_dry_run_state_updated_to_created(self, config, state, mock_rucio):
        """dry-run must still advance state to CREATED."""
        config.dry_run = True
        run_generation(config, state, mock_rucio)
        created = state.get_files_by_status(FileStatus.CREATED)
        assert len(created) == config.num_files

    def test_new_count_additive(self, config, state, mock_rucio, rse_mount):
        """new_count generates that many files on top of the existing state."""
        # Simulate 2 files already CREATED in a previous run
        for idx in range(2):
            key = _state_key(idx)
            state.allocate(key)
            state.update(key, status=FileStatus.CREATED, lfn="test:x",
                         lfn_name="x", pfn="/p/x", bytes=512, adler32="deadbeef")

        # Generate 1 more (new_count=1) → total managed = 2+1 = 3
        results = run_generation(config, state, mock_rucio, new_count=1)
        assert len(results) == 3   # 2 already done + 1 new

    def test_new_count_zero_skips_generation(self, config, state, mock_rucio, rse_mount):
        """new_count=0 returns existing CREATED entries without generating more."""
        # Pre-populate state with CREATED entries
        run_generation(config, state, mock_rucio)
        call_count_before = mock_rucio.lfns2pfn.call_count

        results = run_generation(config, state, mock_rucio, new_count=0)
        assert len(results) == config.num_files
        assert mock_rucio.lfns2pfn.call_count == call_count_before  # no new calls

    def test_lfns2pfn_failure_marks_failed_creation(self, config, state, mock_rucio):
        """If lfns2pfn raises in the main process, the file is marked FAILED_CREATION."""
        mock_rucio.lfns2pfn.side_effect = RuntimeError("rucio down")
        results = run_generation(config, state, mock_rucio)
        assert results == []
        failed = state.get_files_by_status(FileStatus.FAILED_CREATION)
        assert len(failed) == config.num_files

    def test_lfns2pfn_failure_cleans_up_staging(self, config, state, mock_rucio):
        """Staging files must be removed when post-write processing fails."""
        mock_rucio.lfns2pfn.side_effect = RuntimeError("rucio down")
        run_generation(config, state, mock_rucio)
        staging_files = os.listdir(config.staging_dir)
        tmp_files = [f for f in staging_files if ".tmp." in f]
        assert tmp_files == [], "orphaned staging files: {}".format(tmp_files)
