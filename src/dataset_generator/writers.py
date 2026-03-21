"""
writers.py — File-generation back-ends for rucio-ds-generator.

Each back-end implements the ``FileWriter`` interface (ABC).  The factory
function ``get_file_writer(mode, config)`` maps a ``generation_mode`` string
(from config or CLI) to a concrete instance ready for use by Pool A threads.

Adding a new back-end
---------------------
1. Subclass ``FileWriter`` and implement ``description`` and ``write_file``.
2. Override ``from_config(cls, config)`` if the back-end needs config params.
3. Register it in ``_WRITER_REGISTRY`` with a unique string key.
4. Document the key in ``config.example.yaml`` and ``CLAUDE.md``.

Thread safety
-------------
A single ``FileWriter`` instance is shared across all Pool A worker threads.
Implementations must be thread-safe.  Stateless implementations (no mutable
instance state after construction) satisfy this automatically.

Available modes
---------------
csprng (default)
    Streaming pseudo-random data via the fastest available stdlib generator
    (``random.randbytes`` on Python 3.9+, ``os.urandom`` on 3.6–3.8).
    Each chunk is independently generated and written through a raw OS file
    descriptor.  ``posix_fallocate`` pre-allocates the file extent where
    supported.

buffer-reuse
    Pre-fills a fixed-size ring buffer with random data once at construction.
    For each chunk write a random start offset into the ring is chosen,
    providing data variety without calling the PRNG again.  Write throughput
    becomes disk/memory-bandwidth-limited rather than PRNG-limited.
    Configured by ``buffer_reuse_ring_size`` (default 512 MiB).
"""

import os
import random
import sys
import zlib
from abc import ABC, abstractmethod
from typing import Any, Optional

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
    (no mutable instance state after construction) satisfy this automatically.
    """

    @classmethod
    def from_config(cls, config):
        # type: (Any) -> FileWriter
        """
        Construct a ``FileWriter`` from a ``Config`` instance.

        The default implementation calls the no-argument constructor, which is
        sufficient for back-ends that require no configuration parameters.
        Parameterised back-ends (e.g. ``BufferReuseFileWriter``) override this
        method to read the relevant attributes from *config*.

        Parameters
        ----------
        config:
            ``Config`` instance, or ``None`` when called from tests that do
            not need a full config.
        """
        return cls()

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
# Buffer-reuse implementation
# ---------------------------------------------------------------------------

#: Minimum ring size: must be >= CHUNK_SIZE so that every chunk access
#: is a contiguous slice of the extended ring.
_MIN_RING_SIZE = CHUNK_SIZE

#: Default ring size when no config is available (512 MiB).
_DEFAULT_RING_SIZE = 512 * 1024 * 1024


class BufferReuseFileWriter(FileWriter):
    """
    File writer using a pre-allocated random ring buffer.

    A fixed-size ring is filled with random data **once** at construction.
    For each chunk written, a uniformly random start offset within the ring
    is chosen and the chunk is read as a contiguous slice — no PRNG call is
    made per chunk.  Write throughput becomes disk/memory-bandwidth-limited
    rather than PRNG-limited.

    Ring layout
    -----------
    To guarantee every chunk access is contiguous (no wraparound copy), the
    ring is extended by ``CHUNK_SIZE`` bytes at construction by duplicating
    its first ``CHUNK_SIZE`` bytes::

        [ ring_size bytes of random data | CHUNK_SIZE bytes (copy of start) ]

    Any start offset ``s`` in ``[0, ring_size)`` yields a valid contiguous
    slice ``extended[s : s + chunk_size]``.

    Memory
    ------
    Peak RSS during construction ≈ 2 × ring_size (original random bytes +
    extended buffer).  After construction only ``ring_size + CHUNK_SIZE``
    bytes are retained.

    Thread safety
    -------------
    The extended buffer is read-only after construction.  ``random.randrange``
    uses Python's module-level ``Random`` instance which is protected by the
    GIL — safe for concurrent Pool A threads without additional locking.

    Parameters
    ----------
    ring_size:
        Size of the random ring in bytes.  Must be >= ``CHUNK_SIZE``
        (128 MiB).  Larger values provide greater data variety across chunks.
    """

    def __init__(self, ring_size=_DEFAULT_RING_SIZE):
        # type: (int) -> None
        if ring_size < _MIN_RING_SIZE:
            raise ValueError(
                "buffer_reuse_ring_size ({} bytes) must be >= CHUNK_SIZE "
                "({} bytes / {} MiB)".format(
                    ring_size, _MIN_RING_SIZE, _MIN_RING_SIZE // (1024 * 1024)
                )
            )
        self._ring_size = ring_size

        # Fill ring with random data, then extend by CHUNK_SIZE bytes
        # (duplicate the ring's start) so every chunk access is contiguous.
        extended = bytearray(_randbytes(ring_size))
        extended += extended[:CHUNK_SIZE]
        self._extended = memoryview(extended)

    @classmethod
    def from_config(cls, config):
        # type: (Any) -> BufferReuseFileWriter
        ring_size = (
            config.buffer_reuse_ring_size
            if config is not None
            else _DEFAULT_RING_SIZE
        )
        return cls(ring_size=ring_size)

    @property
    def description(self):
        # type: () -> str
        return (
            "buffer-reuse | ring: {} MiB | chunk: {} MiB | fallocate: {}".format(
                self._ring_size // (1024 * 1024),
                CHUNK_SIZE // (1024 * 1024),
                "yes" if _HAS_FALLOCATE else "no",
            )
        )

    def write_file(self, path, size_bytes):
        # type: (str, int) -> tuple
        checksum = 1  # adler32 initial value per RFC 1950
        bytes_written = 0
        remaining = size_bytes
        extended = self._extended  # local ref avoids repeated attr lookup

        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o666)
        try:
            if _HAS_FALLOCATE:
                try:
                    os.posix_fallocate(fd, 0, size_bytes)
                except OSError:
                    pass

            while remaining > 0:
                chunk = min(CHUNK_SIZE, remaining)
                # Random start in [0, ring_size); extension guarantees the
                # slice [start, start+chunk) is always contiguous.
                start = random.randrange(self._ring_size)
                data = extended[start:start + chunk]

                written = 0
                while written < chunk:
                    written += os.write(fd, data[written:])

                # zlib.adler32 accepts buffer-protocol objects (memoryview).
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
    "buffer-reuse": BufferReuseFileWriter,
}  # type: dict


def get_file_writer(mode, config=None):
    # type: (str, Any) -> FileWriter
    """
    Return a ``FileWriter`` instance for *mode*.

    Calls ``cls.from_config(config)`` on the registered class, allowing
    parameterised back-ends to read their settings from the ``Config``
    object.  ``CsprngFileWriter.from_config`` ignores *config*; passing
    ``None`` is safe for all built-in back-ends.

    Parameters
    ----------
    mode:
        One of the registered mode strings (e.g. ``"csprng"``,
        ``"buffer-reuse"``).
    config:
        ``Config`` instance, or ``None`` (e.g. in tests).

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
    return cls.from_config(config)
