"""
Tests for writers.py — FileWriter interface, CsprngFileWriter, and factory.
"""

import os
import zlib
from unittest.mock import patch

import pytest

from dataset_generator.writers import (
    CHUNK_SIZE,
    BufferReuseFileWriter,
    CsprngFileWriter,
    FileWriter,
    _DEFAULT_RING_SIZE,
    _MIN_RING_SIZE,
    _RAND_METHOD,
    _WRITER_REGISTRY,
    get_file_writer,
)


# ---------------------------------------------------------------------------
# FileWriter interface
# ---------------------------------------------------------------------------

class TestFileWriterInterface:
    def test_cannot_instantiate_abstract_class(self):
        with pytest.raises(TypeError):
            FileWriter()  # type: ignore

    def test_subclass_must_implement_description(self):
        class Incomplete(FileWriter):
            def write_file(self, path, size_bytes):
                return 1, size_bytes

        with pytest.raises(TypeError):
            Incomplete()

    def test_subclass_must_implement_write_file(self):
        class Incomplete(FileWriter):
            @property
            def description(self):
                return "incomplete"

        with pytest.raises(TypeError):
            Incomplete()

    def test_complete_subclass_instantiates(self):
        class Minimal(FileWriter):
            @property
            def description(self):
                return "minimal"

            def write_file(self, path, size_bytes):
                return 1, size_bytes

        w = Minimal()
        assert w.description == "minimal"


# ---------------------------------------------------------------------------
# CsprngFileWriter
# ---------------------------------------------------------------------------

class TestCsprngFileWriter:
    @pytest.fixture
    def writer(self):
        return CsprngFileWriter()

    def test_is_file_writer_subclass(self, writer):
        assert isinstance(writer, FileWriter)

    def test_description_contains_chunk_size(self, writer):
        assert "128" in writer.description   # CHUNK_SIZE = 128 MiB

    def test_description_contains_rand_method(self, writer):
        # _RAND_METHOD contains "MT19937" or "CSPRNG" depending on Python version
        assert any(kw in writer.description for kw in ("MT19937", "CSPRNG", "urandom", "randbytes"))

    def test_write_correct_size(self, tmp_path, writer):
        path = str(tmp_path / "f")
        _, size = writer.write_file(path, 4096)
        assert size == 4096
        assert os.path.getsize(path) == 4096

    def test_checksum_in_range(self, tmp_path, writer):
        path = str(tmp_path / "f")
        checksum, _ = writer.write_file(path, 512)
        assert 0 <= checksum <= 0xFFFFFFFF

    def test_checksum_matches_data(self, tmp_path, writer):
        path = str(tmp_path / "f")
        fixed = b"X" * 512
        with patch("dataset_generator.writers._randbytes", return_value=fixed):
            checksum, _ = writer.write_file(path, 512)
        assert checksum == zlib.adler32(fixed, 1) & 0xFFFFFFFF

    def test_size_mismatch_raises(self, tmp_path, writer):
        path = str(tmp_path / "f")
        real_stat = os.stat
        def fake_stat(p, **kw):
            r = real_stat(p, **kw)
            return os.stat_result((r.st_mode, r.st_ino, r.st_dev, r.st_nlink,
                                   r.st_uid, r.st_gid, 0,
                                   r.st_atime, r.st_mtime, r.st_ctime))
        with patch("os.stat", side_effect=fake_stat):
            with pytest.raises(RuntimeError, match="File size mismatch"):
                writer.write_file(path, 256)

    def test_partial_write_loop_completes_full_chunk(self, tmp_path, writer):
        """
        If os.write returns fewer bytes than requested (partial write), the
        inner loop must keep writing until the full chunk is flushed.

        Simulates a kernel that accepts at most 100 bytes per write() call.
        """
        real_write = os.write
        call_sizes = []

        def capped_write(fd, data):
            # Accept at most 100 bytes per call to simulate partial writes.
            chunk = bytes(data[:100])
            call_sizes.append(len(chunk))
            return real_write(fd, chunk)

        size = 512
        with patch("os.write", side_effect=capped_write):
            checksum, written = writer.write_file(str(tmp_path / "f"), size)

        assert written == size
        assert os.path.getsize(str(tmp_path / "f")) == size
        # Must have taken multiple write() calls per chunk (512 / 100 = 6 calls).
        assert len(call_sizes) >= 6
        assert 0 <= checksum <= 0xFFFFFFFF

    def test_thread_safe_no_shared_mutable_state(self, writer):
        """CsprngFileWriter has no mutable instance state — safe to share across threads."""
        assert writer.__dict__ == {}


# ---------------------------------------------------------------------------
# Registry and factory
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# BufferReuseFileWriter
# ---------------------------------------------------------------------------

class TestBufferReuseFileWriter:
    # Use the minimum ring size (= CHUNK_SIZE) to keep test memory usage low.
    _RING = _MIN_RING_SIZE

    @pytest.fixture
    def writer(self):
        return BufferReuseFileWriter(ring_size=self._RING)

    def test_is_file_writer_subclass(self, writer):
        assert isinstance(writer, FileWriter)

    def test_ring_too_small_raises(self):
        with pytest.raises(ValueError, match="must be >= CHUNK_SIZE"):
            BufferReuseFileWriter(ring_size=CHUNK_SIZE - 1)

    def test_description_contains_ring_size(self, writer):
        # ring_size == CHUNK_SIZE == 128 MiB
        assert "128" in writer.description

    def test_description_contains_chunk_size(self, writer):
        assert "128" in writer.description

    def test_write_correct_size(self, tmp_path, writer):
        path = str(tmp_path / "f")
        _, size = writer.write_file(path, 4096)
        assert size == 4096
        assert os.path.getsize(path) == 4096

    def test_checksum_in_range(self, tmp_path, writer):
        path = str(tmp_path / "f")
        checksum, _ = writer.write_file(path, 512)
        assert 0 <= checksum <= 0xFFFFFFFF

    def test_size_mismatch_raises(self, tmp_path, writer):
        path = str(tmp_path / "f")
        real_stat = os.stat
        def fake_stat(p, **kw):
            r = real_stat(p, **kw)
            return os.stat_result((r.st_mode, r.st_ino, r.st_dev, r.st_nlink,
                                   r.st_uid, r.st_gid, 0,
                                   r.st_atime, r.st_mtime, r.st_ctime))
        with patch("os.stat", side_effect=fake_stat):
            with pytest.raises(RuntimeError, match="File size mismatch"):
                writer.write_file(path, 256)

    def test_partial_write_loop_completes_full_chunk(self, tmp_path, writer):
        """
        If os.write returns fewer bytes than requested, the inner loop must
        keep writing until the full chunk is flushed.
        """
        real_write = os.write
        call_sizes = []

        def capped_write(fd, data):
            chunk = bytes(data[:100])
            call_sizes.append(len(chunk))
            return real_write(fd, chunk)

        size = 512
        with patch("os.write", side_effect=capped_write):
            checksum, written = writer.write_file(str(tmp_path / "f"), size)

        assert written == size
        assert os.path.getsize(str(tmp_path / "f")) == size
        assert len(call_sizes) >= 6
        assert 0 <= checksum <= 0xFFFFFFFF

    def test_from_config_reads_ring_size(self):
        class FakeConfig:
            buffer_reuse_ring_size = _MIN_RING_SIZE

        w = BufferReuseFileWriter.from_config(FakeConfig())
        assert w._ring_size == _MIN_RING_SIZE

    def test_from_config_none_uses_default(self):
        w = BufferReuseFileWriter.from_config(None)
        assert w._ring_size == _DEFAULT_RING_SIZE

    def test_extended_buffer_length(self, writer):
        # Extended buffer = ring_size + CHUNK_SIZE bytes (wrap-around copy).
        assert len(writer._extended) == self._RING + CHUNK_SIZE

    def test_thread_safe_read_only_after_construction(self, writer):
        # _extended must be a memoryview (read-only after construction).
        assert isinstance(writer._extended, memoryview)


class TestConcurrentWrites:
    """
    Verify that a single FileWriter instance shared across threads produces
    valid, independently checksummed files with no data corruption.
    """

    _N_THREADS = 4
    _FILE_SIZE = 4096

    def _run_concurrent(self, tmp_path, writer):
        import threading
        import zlib as _zlib

        errors = []
        results = {}
        barrier = threading.Barrier(self._N_THREADS)

        def worker(idx):
            path = str(tmp_path / "f{}.dat".format(idx))
            try:
                barrier.wait()   # all threads start simultaneously
                checksum, size = writer.write_file(path, self._FILE_SIZE)
                with open(path, "rb") as fh:
                    data = fh.read()
                expected = _zlib.adler32(data, 1) & 0xFFFFFFFF
                results[idx] = (checksum, size, expected)
            except Exception as exc:
                errors.append((idx, exc))

        threads = [threading.Thread(target=worker, args=(i,))
                   for i in range(self._N_THREADS)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        return errors, results

    def test_csprng_concurrent_no_errors(self, tmp_path):
        writer = CsprngFileWriter()
        errors, results = self._run_concurrent(tmp_path, writer)
        assert errors == [], "Thread errors: {}".format(errors)

    def test_csprng_concurrent_correct_checksums(self, tmp_path):
        writer = CsprngFileWriter()
        _, results = self._run_concurrent(tmp_path, writer)
        assert len(results) == self._N_THREADS
        for idx, (checksum, size, expected) in results.items():
            assert size == self._FILE_SIZE, "thread {}: wrong size".format(idx)
            assert checksum == expected, "thread {}: checksum mismatch".format(idx)

    def test_csprng_concurrent_independent_files(self, tmp_path):
        """Each thread must write to its own file — no cross-thread contamination."""
        writer = CsprngFileWriter()
        _, results = self._run_concurrent(tmp_path, writer)
        sizes = [r[1] for r in results.values()]
        assert all(s == self._FILE_SIZE for s in sizes)

    def test_buffer_reuse_concurrent_no_errors(self, tmp_path):
        writer = BufferReuseFileWriter(ring_size=_MIN_RING_SIZE)
        errors, results = self._run_concurrent(tmp_path, writer)
        assert errors == [], "Thread errors: {}".format(errors)

    def test_buffer_reuse_concurrent_correct_checksums(self, tmp_path):
        writer = BufferReuseFileWriter(ring_size=_MIN_RING_SIZE)
        _, results = self._run_concurrent(tmp_path, writer)
        assert len(results) == self._N_THREADS
        for idx, (checksum, size, expected) in results.items():
            assert size == self._FILE_SIZE, "thread {}: wrong size".format(idx)
            assert checksum == expected, "thread {}: checksum mismatch".format(idx)


class TestGetFileWriter:
    def test_csprng_returns_csprng_writer(self):
        w = get_file_writer("csprng")
        assert isinstance(w, CsprngFileWriter)

    def test_unknown_mode_raises_value_error(self):
        with pytest.raises(ValueError, match="Unknown generation_mode"):
            get_file_writer("nonexistent_mode")

    def test_error_lists_valid_modes(self):
        try:
            get_file_writer("bad")
        except ValueError as exc:
            assert "csprng" in str(exc)

    def test_registry_contains_csprng(self):
        assert "csprng" in _WRITER_REGISTRY

    def test_registry_contains_buffer_reuse(self):
        assert "buffer-reuse" in _WRITER_REGISTRY

    def test_buffer_reuse_returns_buffer_reuse_writer(self):
        w = get_file_writer("buffer-reuse")
        assert isinstance(w, BufferReuseFileWriter)

    def test_each_call_returns_new_instance(self):
        w1 = get_file_writer("csprng")
        w2 = get_file_writer("csprng")
        assert w1 is not w2


# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------

class TestModuleConstants:
    def test_chunk_size_is_128_mib(self):
        assert CHUNK_SIZE == 128 * 1024 * 1024

    def test_rand_method_is_string(self):
        assert isinstance(_RAND_METHOD, str)
        assert len(_RAND_METHOD) > 0
