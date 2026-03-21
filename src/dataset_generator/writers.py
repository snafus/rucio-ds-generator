"""
writers.py — File-generation back-ends for rucio-ds-generator.

Each back-end implements the ``FileWriter`` interface (ABC).  The factory
function ``get_file_writer(mode)`` maps a ``generation_mode`` string (from
config or CLI) to a concrete instance ready for use by Pool A threads.

Adding a new back-end
---------------------
1. Subclass ``FileWriter`` and implement ``description`` and ``write_file``.
2. Register it in ``_WRITER_REGISTRY`` with a unique string key.
3. Document the key in ``config.example.yaml`` and ``CLAUDE.md``.

Thread safety
-------------
A single ``FileWriter`` instance is shared across all Pool A worker threads.
Implementations must be thread-safe.  Stateless implementations (no mutable
instance attributes) are inherently safe.

Available modes
---------------
csprng (default)
    Streaming pseudo-random data via the fastest available stdlib generator
    (``random.randbytes`` on Python 3.9+, ``os.urandom`` on 3.6–3.8).
    Each chunk is independently generated and written through a raw OS file
    descriptor (bypasses ``BufferedWriter`` memcpy).  ``posix_fallocate``
    pre-allocates the file extent where supported.
"""

import os
import sys
import zlib
from abc import ABC, abstractmethod
from typing import Optional

# ---------------------------------------------------------------------------
# Runtime capability detection — resolved once at import, logged at run time.
# ---------------------------------------------------------------------------

# Chunk size used by CsprngFileWriter.  128 MiB halves loop/allocation
# overhead vs 64 MiB while keeping peak RSS reasonable across threads
# (4 threads × 128 MiB = 512 MiB, 8 threads × 128 MiB = 1 GiB).
CHUNK_SIZE = 128 * 1024 * 1024  # 128 MiB

# Fastest available bulk-random-bytes generator.
#
# Python 3.9+  random.randbytes — MT19937 PRNG running entirely in userspace;
#              ~2–4× faster than os.urandom for large sequential generation.
#              Not cryptographically secure, which is fine for test data.
#
# Python 3.6–3.8  os.urandom — calls getrandom() on Linux 3.17+, /dev/urandom
#              elsewhere.  No faster stdlib PRNG alternative exists for bulk
#              sizes: random.getrandbits + bigint .to_bytes is slower due to
#              Python bigint overhead.
if sys.version_info >= (3, 9):
    from random import randbytes as _randbytes
    _RAND_METHOD = "random.randbytes (MT19937, Python {}.{})".format(*sys.version_info[:2])
else:
    _randbytes = os.urandom
    _RAND_METHOD = "os.urandom (CSPRNG, Python {}.{})".format(*sys.version_info[:2])

# posix_fallocate pre-allocates the full file extent on disk before any data
# is written, eliminating extent-allocation overhead during sequential writes.
# Available on Linux/macOS; absent on Windows and some exotic filesystems.
_HAS_FALLOCATE = hasattr(os, "posix_fallocate")


# ---------------------------------------------------------------------------
# Abstract base class
# ---------------------------------------------------------------------------

class FileWriter(ABC):
    """
    Interface for file-generation strategies.

    A single instance is shared across all Pool A worker threads —
    implementations must be thread-safe.  Stateless implementations
    (no mutable instance state) satisfy this requirement automatically.
    """

    @property
    @abstractmethod
    def description(self):
        # type: () -> str
        """One-line human-readable description, logged at run time."""

    @abstractmethod
    def write_file(self, path, size_bytes):
        # type: (str, int) -> tuple
        """
        Write *size_bytes* of data to *path*.

        Parameters
        ----------
        path:
            Absolute path of the file to create (must not already exist, or
            will be truncated).
        size_bytes:
            Exact number of bytes to write.

        Returns
        -------
        tuple
            ``(checksum_val, bytes_written)`` where *checksum_val* is the
            adler32 checksum of the written data with ``& 0xFFFFFFFF`` applied
            and *bytes_written* == *size_bytes*.

        Raises
        ------
        RuntimeError
            If the on-disk file size does not equal *size_bytes* after writing.
        OSError
            On any filesystem error.
        """


# ---------------------------------------------------------------------------
# Default implementation: streaming CSPRNG / PRNG
# ---------------------------------------------------------------------------

class CsprngFileWriter(FileWriter):
    """
    Default file writer — streaming pseudo-random data.

    Uses the fastest available stdlib PRNG (``random.randbytes`` on Python
    3.9+, ``os.urandom`` on 3.6–3.8).  Writes via a raw OS file descriptor
    to avoid the ``BufferedWriter`` memcpy overhead.  ``os.posix_fallocate``
    pre-allocates the file extent where supported.

    Thread-safe: no mutable instance state.
    """

    @property
    def description(self):
        # type: () -> str
        return "csprng | {} | chunk: {} MiB | fallocate: {}".format(
            _RAND_METHOD,
            CHUNK_SIZE // (1024 * 1024),
            "yes" if _HAS_FALLOCATE else "no",
        )

    def write_file(self, path, size_bytes):
        # type: (str, int) -> tuple
        checksum = 1  # adler32 initial value per RFC 1950
        bytes_written = 0
        remaining = size_bytes

        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o666)
        try:
            # Pre-allocate full extent; silently ignored if unsupported.
            if _HAS_FALLOCATE:
                try:
                    os.posix_fallocate(fd, 0, size_bytes)
                except OSError:
                    pass

            while remaining > 0:
                chunk = min(CHUNK_SIZE, remaining)
                data = _randbytes(chunk)
                # memoryview avoids a copy in the partial-write loop.
                view = memoryview(data)
                written = 0
                while written < chunk:
                    written += os.write(fd, view[written:])
                # Apply mask on every iteration to keep value in 32-bit range.
                checksum = zlib.adler32(data, checksum) & 0xFFFFFFFF
                bytes_written += chunk
                remaining -= chunk
        finally:
            os.close(fd)

        actual = os.stat(path).st_size
        if actual != size_bytes:
            raise RuntimeError(
                "File size mismatch after write: expected {} bytes, "
                "got {} bytes ({})".format(size_bytes, actual, path)
            )

        return checksum, bytes_written


# ---------------------------------------------------------------------------
# Registry and factory
# ---------------------------------------------------------------------------

#: Maps mode strings (as used in config / CLI) to ``FileWriter`` subclasses.
#: Add new back-ends here to make them selectable via ``generation_mode``.
_WRITER_REGISTRY = {
    "csprng": CsprngFileWriter,
}  # type: dict


def get_file_writer(mode):
    # type: (str) -> FileWriter
    """
    Return a ``FileWriter`` instance for *mode*.

    Parameters
    ----------
    mode:
        One of the registered mode strings (e.g. ``"csprng"``).

    Raises
    ------
    ValueError
        If *mode* is not a registered back-end.
    """
    cls = _WRITER_REGISTRY.get(mode)
    if cls is None:
        raise ValueError(
            "Unknown generation_mode {!r}. Valid modes: {}".format(
                mode, ", ".join(sorted(_WRITER_REGISTRY))
            )
        )
    return cls()
