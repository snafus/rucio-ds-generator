"""
Tests for writers.py — FileWriter interface, CsprngFileWriter, and factory.
"""

import os
import zlib
from unittest.mock import patch

import pytest

from dataset_generator.writers import (
    CHUNK_SIZE,
    CsprngFileWriter,
    FileWriter,
    _HAS_FALLOCATE,
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

    def test_description_contains_fallocate_status(self, writer):
        expected = "yes" if _HAS_FALLOCATE else "no"
        assert "fallocate: {}".format(expected) in writer.description

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

    def test_thread_safe_no_shared_mutable_state(self, writer):
        """CsprngFileWriter has no instance attributes — safe to share across threads."""
        assert writer.__dict__ == {}


# ---------------------------------------------------------------------------
# Registry and factory
# ---------------------------------------------------------------------------

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

    def test_has_fallocate_is_bool(self):
        assert isinstance(_HAS_FALLOCATE, bool)
