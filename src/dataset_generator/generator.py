"""
generator.py — File generation for rucio-ds-generator.

This module handles Phase 1 of the pipeline: creating random binary files
on the POSIX-mounted RSE storage and recording their state.

File generation pipeline (per file)
-------------------------------------
1. A worker process writes random data to a temp path in ``staging_dir``
   using 128 MiB chunks.  The adler32 checksum is accumulated
   incrementally — the full file is never held in memory.
2. The worker returns ``(idx, staging_path, checksum_hex, bytes_written, error_str)``
   to the main process via ``multiprocessing.Pool.imap_unordered``.
3. The main process derives the LFN: ``{file_prefix}_{adler32_hex}``.
4. The main process calls ``RucioManager.lfns2pfn`` to resolve the
   canonical PFN and translates it to a local filesystem path via
   ``_pfn_to_local``.
5. The main process creates parent (hash) directories and calls
   ``_place_file`` to move the staged file atomically to its final RSE
   location.
6. The main process updates the state entry to ``FileStatus.CREATED``.

Process model (Pool A)
-----------------------
``multiprocessing.Pool`` (``--threads`` processes, default 4) is used for
file generation.  Each worker process has its own Python interpreter and
its own GIL, so ``random.randbytes`` / ``os.urandom`` run in true parallel.
The pool is initialised via ``_worker_init`` which:

* Re-seeds the PRNG after ``fork`` so worker processes diverge from the
  parent's MT19937 state (without this all workers would generate
  identical bytes and thus identical checksums / LFN collisions).
* Constructs a ``FileWriter`` once per worker, avoiding re-pickling a
  large ring buffer on every task submission.
* Routes log records through a ``multiprocessing.Queue`` to a
  ``QueueListener`` in the main process (safe cross-process logging).

State and Rucio access stay in the main process — worker tasks return
plain data; the main process performs all state mutations and Rucio calls.

Dry-run behaviour
-----------------
When ``config.dry_run`` is ``True``:
* No files are written to disk.
* No Rucio API calls are made.
* State is updated with placeholder values so the rest of the pipeline
  can proceed in dry-run mode too.

Adler32 correctness
-------------------
``zlib.adler32(data, value)`` returns a signed 32-bit integer on Python 2
but an unsigned value on Python 3.  Applying ``& 0xFFFFFFFF`` makes the
result portable and matches what Rucio expects.
"""

import errno
import logging
import logging.handlers
import multiprocessing
import os
import random
import struct
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List, Optional

from tqdm import tqdm

from .state import FileStatus, StateFile
from .writers import FileWriter, get_file_writer

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# XrdCks extended-attribute support
# ---------------------------------------------------------------------------

#: Extended-attribute key used by XRootD for adler32 checksums.
#: XrdCksXAttr::Name() returns "XrdCks.<algname>"; the Linux setxattr layer
#: prepends "user." (XrdSysFAttrLnx.icc, AttrName macro).
_XRDCKS_XATTR_NAME = "user.XrdCks.adler32"

#: True when the OS supports os.setxattr (Linux kernel 2.6+, Python 3.3+).
#: On macOS, Windows, or older kernels this is False and xattr writes are
#: silently skipped regardless of the ``xattr`` config flag.
_HAS_SETXATTR = hasattr(os, "setxattr")


def _set_xrdcks_xattr(path, adler32_int):
    # type: (str, int) -> None
    """
    Write the XRootD ``user.XrdCks.adler32`` extended attribute to *path*.

    XRootD reads this attribute via ``XrdCksXAttr`` and uses it for
    checksum validation without re-reading the file.  When the stored
    ``fmTime`` (file-modification time) matches the file's current mtime
    on disk XRootD trusts the stored checksum; a mismatch causes it to
    re-checksum the file automatically.

    Blob layout (96 bytes total — ``XrdCksData`` struct)
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    ============  =====  =========  ==============  =====================
    Field         Bytes  C type     Endian          Content
    ============  =====  =========  ==============  =====================
    Name          16     char[16]   n/a             "adler32\\0" + padding
    fmTime        8      int64      big-endian      file mtime (seconds)
    csTime        4      int32      big-endian      checksum time (secs)
    Rsvd1         2      int16      host            reserved (0)
    Rsvd2         1      uint8      host            reserved (0)
    Length        1      uint8      host            value length in bytes
    Value         64     uint8[64]  host (first 4)  adler32 in first 4 B
    ============  =====  =========  ==============  =====================

    Parameters
    ----------
    path:
        Absolute path to the file on the RSE (after placement/rename).
    adler32_int:
        Adler32 checksum of the file as an unsigned 32-bit integer
        (i.e. ``zlib.adler32(data, 1) & 0xFFFFFFFF``).
    """
    now = int(time.time())
    fm_time = int(os.stat(path).st_mtime)
    # Name field: "adler32\0" padded to 16 bytes.
    name_bytes = b"adler32\x00" + b"\x00" * 8  # 16 bytes
    # fmTime and csTime: big-endian (network byte order) signed integers.
    times = struct.pack(">qi", fm_time, now)    # 8 + 4 = 12 bytes
    # Rsvd1 (int16), Rsvd2 (uint8), Length (uint8 = 4): host byte order.
    meta = struct.pack("=hBB", 0, 0, 4)         # 4 bytes
    # Value: adler32 in first 4 bytes (host byte order), rest zero-padded.
    value = struct.pack("=I", adler32_int) + b"\x00" * 60  # 64 bytes
    blob = name_bytes + times + meta + value    # 16+12+4+64 = 96 bytes
    os.setxattr(path, _XRDCKS_XATTR_NAME, blob)


# ---------------------------------------------------------------------------
# Per-process worker state — set by _worker_init, reused across all tasks
# ---------------------------------------------------------------------------

#: Config instance for this worker process.  Set by ``_worker_init``.
_proc_config = None   # type: Optional[object]

#: FileWriter instance for this worker process.  Set by ``_worker_init``.
_proc_writer = None   # type: Optional[FileWriter]


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _state_key(idx):
    # type: (int) -> str
    """Canonical state key for the file at zero-based *idx*."""
    return "file_{:06d}".format(idx)


def _worker_init(config, log_queue):
    # type: (object, multiprocessing.Queue) -> None
    """
    Pool initialiser — runs once in each worker process before any tasks.

    Sets up per-process logging (routes all records through *log_queue* to
    the main process's ``QueueListener``), re-seeds the PRNG so forked
    workers diverge from the parent's MT19937 state, and constructs a
    ``FileWriter`` that is reused across all tasks in this process.

    Parameters
    ----------
    config:
        ``Config`` instance (pickled from the parent process).
    log_queue:
        ``multiprocessing.Queue`` that the main process's ``QueueListener``
        is draining.
    """
    global _proc_config, _proc_writer

    # Re-seed AFTER fork so each worker produces a distinct byte sequence.
    # os.fork() copies the parent's MT19937 state verbatim; without this,
    # every worker would generate identical bytes → identical checksums →
    # LFN collisions.  random.seed() with no argument sources entropy from
    # the OS, giving each process a unique seed.
    random.seed()

    # Route all log records from this worker to the main-process listener.
    # Clears any inherited handlers to avoid double-logging or dead-locking
    # on a file handler whose lock was held at fork time.
    root = logging.getLogger()
    root.handlers = []
    root.addHandler(logging.handlers.QueueHandler(log_queue))
    # Use the configured log level so DEBUG records are not created (and
    # therefore not pickled, queued, and unpickled) when the operator has
    # selected INFO or higher.  Previously hardcoded to DEBUG, which caused
    # ~4 × N unnecessary LogRecord objects crossing the IPC pipe.
    root.setLevel(getattr(logging, config.log_level, logging.INFO))

    _proc_config = config
    # IMPORTANT: random.seed() above MUST be called before get_file_writer()
    # below.  BufferReuseFileWriter fills its ring buffer with _randbytes()
    # during __init__, so the seed must be in place before construction to
    # guarantee each worker fills its ring with a distinct byte sequence.
    _proc_writer = get_file_writer(config.generation_mode, config)

    pname = multiprocessing.current_process().name
    log.info("[%s] Worker ready (pid=%d): %s",
             pname, os.getpid(), _proc_writer.description)


def _write_one_worker(idx):
    # type: (int) -> tuple
    """
    Worker task: write one file to the staging directory.

    Called by ``multiprocessing.Pool.imap_unordered`` in a worker process.
    Uses the module-level ``_proc_config`` and ``_proc_writer`` set by
    ``_worker_init``.

    Never raises — failures are returned as the fifth tuple element so
    the main-process result loop handles them uniformly.

    Returns
    -------
    tuple
        ``(idx, staging_path, checksum_hex, bytes_written, None)`` on
        success.  ``(idx, None, None, None, error_message)`` on failure
        (any partial staging file has already been cleaned up).
    """
    global _proc_config, _proc_writer
    config = _proc_config
    writer = _proc_writer
    pname = multiprocessing.current_process().name

    staging_dir = config.staging_dir
    tmp_name = "{}.tmp.{:06d}".format(config.file_prefix, idx)
    tmp_path = os.path.join(staging_dir, tmp_name)

    log.debug("[%s|file-%06d] Starting write: staging=%s size=%d",
              pname, idx, staging_dir, config.file_size_bytes)

    try:
        if config.dry_run:
            checksum_hex = format(0xDEADBEEF & 0xFFFFFFFF, "08x")
            bytes_written = config.file_size_bytes
            log.debug("[%s|file-%06d] dry-run: skipping write", pname, idx)
        else:
            log.debug("[%s|file-%06d] Writing to staging: %s", pname, idx, tmp_path)
            checksum_val, bytes_written = writer.write_file(tmp_path, config.file_size_bytes)
            checksum_hex = format(checksum_val, "08x")
            log.debug("[%s|file-%06d] Write complete: %d bytes, adler32=%s",
                      pname, idx, bytes_written, checksum_hex)

        return (idx, tmp_path, checksum_hex, bytes_written, None)

    except Exception as exc:
        log.error("[%s|file-%06d] Write failed: %s", pname, idx, exc)
        # Best-effort cleanup of any partial staging file.
        try:
            if not config.dry_run and os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except OSError:
            pass
        return (idx, None, None, None, str(exc))


# ---------------------------------------------------------------------------
# Directory creation cache — Phase 5 optimisation
# ---------------------------------------------------------------------------

#: Set of directory paths that have already been created in this process.
#: ``_place_file`` checks this before calling ``_makedirs_chown`` so that
#: the syscall sequence (stat + mkdir + chown) is skipped for repeated
#: paths — common when all files land under the same hash directory tree.
#: Cleared at the start of ``run_generation`` to avoid stale entries from
#: a previous call.
_known_dirs = set()   # type: set
_known_dirs_lock = threading.Lock()


def _ensure_dir(path, uid, gid):
    # type: (str, Optional[int], Optional[int]) -> None
    """
    Create *path* (and parents) unless it is already in the cache.

    Thread-safe: the lock is held for the full check-then-create sequence so
    two threads arriving simultaneously for the same path serialise — the
    second thread finds the path in the cache and returns without a syscall.
    ``_makedirs_chown`` is a handful of stat/mkdir/chown calls (fast, no
    network I/O), so holding the lock across it does not cause contention.
    """
    with _known_dirs_lock:
        if path in _known_dirs:
            return
        _makedirs_chown(path, uid, gid)
        _known_dirs.add(path)


def _makedirs_chown(path, uid, gid):
    # type: (str, Optional[int], Optional[int]) -> None
    """
    Create *path* and all intermediate directories, then ``chown`` each
    newly created directory to (*uid*, *gid*).

    Only directories that did not exist before this call are chowned —
    pre-existing directories are left untouched.  If both *uid* and *gid*
    are ``None`` the function behaves identically to
    ``os.makedirs(path, exist_ok=True)``.

    Parameters
    ----------
    path:
        Directory path to create (absolute).
    uid:
        User ID to assign, or ``None`` to leave unchanged (POSIX ``-1``).
    gid:
        Group ID to assign, or ``None`` to leave unchanged (POSIX ``-1``).
    """
    # Walk upward from path collecting dirs that do not yet exist.
    to_create = []
    current = os.path.normpath(path)
    while not os.path.exists(current):
        to_create.append(current)
        parent = os.path.dirname(current)
        if parent == current:   # reached filesystem root
            break
        current = parent

    # exist_ok=True handles EEXIST races when multiple placement threads
    # create the same hash directory concurrently (common on CephFS where
    # metadata ops have higher latency than on local filesystems).
    os.makedirs(path, exist_ok=True)

    if uid is not None or gid is not None:
        effective_uid = uid if uid is not None else -1
        effective_gid = gid if gid is not None else -1
        # to_create is deepest-first; chown top-down (reversed) for clarity.
        for d in reversed(to_create):
            os.chown(d, effective_uid, effective_gid)


def _pfn_to_local(pfn, rse_pfn_prefix, rse_mount):
    # type: (str, Optional[str], str) -> str
    """
    Translate a Rucio-returned PFN to an absolute local filesystem path.

    Rucio may return a path whose prefix differs from the local POSIX mount
    point.  For example, Rucio knows the RSE root as ``/data/rucio`` but on
    this host it is mounted at ``/mnt/rse``.

    Steps:
    1. Strip a ``file://`` protocol prefix if present (other protocols are
       not supported by this tool and will raise).
    2. If *rse_pfn_prefix* is set, assert the path starts with it, then
       replace it with *rse_mount*.  If not set, return the path unchanged.

    Parameters
    ----------
    pfn:
        PFN string as returned by ``RucioManager.lfns2pfn``.
    rse_pfn_prefix:
        The path component that Rucio uses as the RSE root, e.g.
        ``/data/rucio``.  ``None`` means no translation is needed.
    rse_mount:
        Local POSIX mount point for the RSE, e.g. ``/mnt/rse``.

    Returns
    -------
    str
        Absolute local filesystem path.
    """
    # rse_pfn_prefix is matched against the raw PFN (including any protocol),
    # so it can be a full URL prefix such as "davs://host:port/rse/root".
    if rse_pfn_prefix:
        if not pfn.startswith(rse_pfn_prefix):
            raise RuntimeError(
                "PFN {!r} does not start with rse_pfn_prefix {!r}".format(pfn, rse_pfn_prefix)
            )
        # Strip the prefix and ensure exactly one '/' between mount and suffix,
        # regardless of whether rse_pfn_prefix or rse_mount has a trailing slash.
        suffix = pfn[len(rse_pfn_prefix):]
        if not suffix.startswith("/"):
            suffix = "/" + suffix
        return rse_mount.rstrip("/") + suffix

    # No prefix mapping — strip file:// if present; reject other protocols.
    if pfn.startswith("file://"):
        return pfn[7:]

    if "://" in pfn:
        raise RuntimeError(
            "Unsupported PFN protocol in {!r}. Set rse_pfn_prefix to the full "
            "URL prefix returned by Rucio (e.g. 'davs://host:port/rse/root') "
            "and rse_mount to the corresponding local POSIX path.".format(pfn)
        )

    return pfn


#: Buffer size for the fallback cross-filesystem copy loop in _copy_file_fast.
#: 8 MiB gives 512× fewer syscalls than shutil.copy2's 16 KiB default.
_COPY_BUFSIZE = 8 * 1024 * 1024   # 8 MiB


def _copy_file_fast(src, dst):
    # type: (str, str) -> None
    """
    Copy *src* to *dst* using the fastest available method.

    Prefers ``os.sendfile`` (Linux/macOS, Python 3.3+) which performs a
    kernel-level copy that releases the GIL and avoids user-space buffering.
    Falls back to an 8 MiB read/write loop on platforms without sendfile —
    ``shutil.copy2`` uses only 16 KiB on Python 3.6, making it 512× slower
    per syscall pair for large files.

    Only file data is copied; metadata (timestamps, xattrs) is not transferred.
    The caller (``_place_file``) handles ownership separately via ``os.chown``.
    """
    with open(src, "rb") as fsrc:
        with open(dst, "wb") as fdst:
            src_fd = fsrc.fileno()
            dst_fd = fdst.fileno()
            if hasattr(os, "sendfile"):
                # sendfile(out_fd, in_fd, offset, count) — releases the GIL;
                # performs a kernel-level copy on Linux with no user-space copy.
                file_size = os.fstat(src_fd).st_size
                offset = 0
                while offset < file_size:
                    sent = os.sendfile(dst_fd, src_fd, offset, file_size - offset)
                    if sent == 0:
                        break
                    offset += sent
            else:
                while True:
                    buf = fsrc.read(_COPY_BUFSIZE)
                    if not buf:
                        break
                    fdst.write(buf)


def _place_file(staging_path, local_pfn, uid=None, gid=None):
    # type: (str, str, Optional[int], Optional[int]) -> None
    """
    Move a fully-written staged file to its final location on the RSE,
    creating hash directories as needed, with an atomic final rename.

    Two-step placement guarantees the final PFN is never partially present:

    1. Move *staging_path* to ``{local_pfn}.part.{pid}.{tid}`` — a temporary
       name *on the RSE filesystem*.  If staging and RSE are on the same
       filesystem ``os.rename`` is used (instant, no data copy).  If they are
       on different filesystems (``EXDEV``), ``_copy_file_fast`` is used followed
       by removal of the staging file.
    2. Set ownership (uid/gid) on the ``.part`` file while it still has a
       temporary name (ownership is preserved across ``os.rename``).
    3. ``os.rename(.part → local_pfn)`` — both names are on the RSE filesystem
       so this rename is always atomic.

    Parameters
    ----------
    staging_path:
        Fully-written temp file in the staging directory.
    local_pfn:
        Absolute local path of the final RSE file location.
    uid:
        User ID to assign to placed file and new directories, or ``None``.
    gid:
        Group ID to assign, or ``None``.
    """
    parent = os.path.dirname(local_pfn)
    if parent:
        _ensure_dir(parent, uid, gid)

    tname = threading.current_thread().name
    part_path = "{}.part.{}.{}".format(local_pfn, os.getpid(), threading.get_ident())

    log.debug("[%s] _place_file: staging=%s part=%s", tname, staging_path, part_path)

    # Step 1: move from staging to RSE filesystem (handles cross-filesystem).
    try:
        os.rename(staging_path, part_path)
        log.debug("[%s] _place_file: same-fs rename ok: %s → %s", tname, staging_path, part_path)
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            raise
        # Cross-filesystem: copy bytes then remove the staging copy.
        # Clean up any partial .part file if the copy fails (e.g. disk full).
        log.debug("[%s] _place_file: EXDEV — falling back to fast copy: %s → %s",
                  tname, staging_path, part_path)
        try:
            _copy_file_fast(staging_path, part_path)
            log.debug("[%s] _place_file: fast copy ok", tname)
        except Exception:
            try:
                os.unlink(part_path)
                log.debug("[%s] _place_file: cleaned up orphaned part after copy failure", tname)
            except OSError:
                pass
            raise
        os.unlink(staging_path)
        log.debug("[%s] _place_file: staging file removed after fast copy", tname)

    # Steps 2 & 3: chown then atomic rename on the RSE filesystem.
    # If either fails the .part file is removed so it does not linger on the RSE.
    try:
        if uid is not None or gid is not None:
            log.debug("[%s] _place_file: chown(%s, uid=%s, gid=%s)", tname, part_path, uid, gid)
            os.chown(part_path,
                     uid if uid is not None else -1,
                     gid if gid is not None else -1)

        log.debug("[%s] _place_file: final rename: %s → %s", tname, part_path, local_pfn)
        os.rename(part_path, local_pfn)
        log.debug("[%s] _place_file: done: %s", tname, local_pfn)
    except Exception:
        try:
            os.unlink(part_path)
            log.debug("[%s] _place_file: cleaned up part after rename/chown failure", tname)
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_generation(config, state, rucio_manager, new_count=None):
    # type: (object, StateFile, object, Optional[int]) -> List[dict]
    """
    Generate files for this run using Pool A (``config.threads`` processes).

    Files whose state entry is already ``CREATED``, ``REGISTERED``, or
    ``RULED`` are skipped.  Files in ``FAILED_CREATION`` (or ``PENDING``) are
    (re-)generated.

    Worker processes handle file writing only.  PFN resolution, file
    placement, and state updates are performed in the main process as
    results arrive via ``imap_unordered``.

    Parameters
    ----------
    config:
        ``Config`` instance.
    state:
        ``StateFile`` instance (already loaded or created by ``__main__``).
    rucio_manager:
        ``RucioManager`` instance for ``lfns2pfn`` calls (main process only).
    new_count:
        Number of *additional* files to generate beyond those already tracked
        in the state file.  The total managed by this call is
        ``state.count() + new_count``.  Defaults to ``config.num_files``
        (the original behaviour when no existing state entries are present).

    Returns
    -------
    list of dict
        Metadata dicts for every successfully generated file (including
        those that were already in ``CREATED`` state from a prior run).
    """
    # Clear the directory-creation cache at the start of each run so that
    # tests and sequential calls don't reuse stale entries from a previous run.
    global _known_dirs
    with _known_dirs_lock:
        _known_dirs = set()

    if new_count is None:
        total = config.num_files
    else:
        total = state.count() + new_count

    # Pre-allocate state entries for all expected files so resume logic works.
    # allocate_batch performs a single JSON write regardless of N, avoiding
    # the O(N²) total-bytes-written cost of N individual allocate() calls.
    state.allocate_batch([_state_key(idx) for idx in range(total)])

    # Determine which files still need generation.
    done_statuses = {FileStatus.CREATED, FileStatus.REGISTERED, FileStatus.RULED}
    to_generate = []   # type: List[int]
    already_done = []  # type: List[dict]

    for idx in range(total):
        key = _state_key(idx)
        entry = state.get_file(key)
        if entry and entry.get("status") in done_statuses:
            already_done.append(entry)
            already_done[-1]["key"] = key
        else:
            to_generate.append(idx)

    if not to_generate:
        log.info("All %d files already generated — skipping Pool A", total)
        return already_done

    log.info(
        "Generating %d file(s) using %d process(es) [%d already done]",
        len(to_generate), config.threads, len(already_done),
    )

    # Pre-render the file_prefix Jinja2 template in the main process so all
    # workers receive a frozen, consistent value in the pickled Config.
    _ = config.file_prefix

    results = list(already_done)
    failures = []

    # Set up cross-process logging: worker records are queued here and
    # emitted by the listener in the main process.
    log_queue = multiprocessing.Queue()  # type: ignore
    root_handlers = logging.getLogger().handlers[:]
    if not root_handlers:
        root_handlers = [logging.StreamHandler()]
    listener = logging.handlers.QueueListener(
        log_queue, *root_handlers, respect_handler_level=True
    )

    # Fix 2: Create the worker pool BEFORE starting the listener thread.
    # os.fork() copies the parent's thread state verbatim; if the listener
    # thread is alive at fork time its log-queue lock may be inherited in a
    # locked state by the child, causing it to deadlock on its first log write.
    pool = multiprocessing.Pool(
        processes=config.threads,
        initializer=_worker_init,
        initargs=(config, log_queue),
    )
    listener.start()   # All forks are done — safe to start the listener thread.

    # Fix 5: Placement thread pool — one slot per generation worker so that
    # file placements (I/O-bound: same-fs rename or cross-fs copy) overlap
    # with the next write rather than serialising after each write result.
    placement_executor = ThreadPoolExecutor(max_workers=config.threads)
    # Maps future → (idx, staging_path, checksum_hex, bytes_written, lfn, lfn_name, local_pfn)
    pending_placements = {}  # type: dict

    try:
        # Read terminal width once; avoid per-update ioctl(TIOCGWINSZ) calls
        # from dynamic_ncols=True which issues a syscall on every pbar.update().
        # mininterval=0.5 limits redraws to twice per second regardless of
        # how many updates arrive — important at 100k+ files/s.
        try:
            _ncols = os.get_terminal_size().columns
        except OSError:
            _ncols = 80
        with tqdm(
            total=total,
            initial=len(already_done),
            unit="file",
            desc="Generating",
            ncols=_ncols,
            mininterval=0.5,
        ) as pbar:
            # -- Placement helpers (closures over pbar and shared state) ------

            def _handle_placement_future(future):
                # type: (object) -> None
                """Collect one completed placement future; update state and pbar."""
                (idx2, staging_path2, checksum_hex2,
                 bytes_written2, lfn2, lfn_name2, local_pfn2) = pending_placements.pop(future)
                key2 = _state_key(idx2)
                try:
                    future.result()   # re-raises any exception from _place_file
                except Exception as exc:
                    log.error("[file-%06d] Placement failed: %s", idx2, exc)
                    # staging_path2 may still exist if step 1 of _place_file failed.
                    try:
                        if staging_path2 and os.path.exists(staging_path2):
                            os.unlink(staging_path2)
                    except OSError:
                        pass
                    state.update(key2, status=FileStatus.FAILED_CREATION)
                    failures.append(idx2)
                    pbar.update(1)
                    return
                state.update(
                    key2,
                    status=FileStatus.CREATED,
                    lfn=lfn2,
                    lfn_name=lfn_name2,
                    pfn=local_pfn2,
                    bytes=bytes_written2,
                    adler32=checksum_hex2,
                )
                if config.xattr and _HAS_SETXATTR:
                    try:
                        _set_xrdcks_xattr(local_pfn2, int(checksum_hex2, 16))
                        log.debug("[file-%06d] XrdCks xattr set: %s",
                                  idx2, local_pfn2)
                    except OSError as exc:
                        log.warning("[file-%06d] XrdCks xattr failed: %s",
                                    idx2, exc)
                results.append({
                    "key": key2,
                    "lfn": lfn2,
                    "lfn_name": lfn_name2,
                    "pfn": local_pfn2,
                    "bytes": bytes_written2,
                    "adler32": checksum_hex2,
                })
                log.info("[file-%06d] Created: %s (%d bytes, adler32=%s)",
                         idx2, lfn2, bytes_written2, checksum_hex2)
                pbar.update(1)

            def _collect_done_placements():
                # type: () -> None
                """Drain any already-completed placement futures without blocking."""
                for future in [f for f in list(pending_placements) if f.done()]:
                    _handle_placement_future(future)

            # -- Batch PFN resolution -----------------------------------------

            # pfn_pending accumulates write results until the batch is full or
            # all writes finish.  One lfns2pfns_batch call replaces N serial
            # lfns2pfn calls, reducing HTTP round-trips from O(N) to
            # O(N/pfn_batch_size).  pfn_batch_size=0 means "all at once".
            pfn_pending = []  # type: List[tuple]  # (idx, staging, chk, bytes, lfn, lfn_name)
            _pfn_batch_size = (
                config.pfn_batch_size if config.pfn_batch_size > 0
                else len(to_generate) + 1   # larger than total → flush only at end
            )

            def _flush_pfn_pending():
                # type: () -> None
                """Resolve PFNs for all buffered write results and submit placements."""
                if not pfn_pending:
                    return
                lfns = [item[4] for item in pfn_pending]
                log.debug("Resolving %d PFN(s) in one batch call (rse=%s)",
                          len(lfns), config.rse)
                try:
                    pfn_map = rucio_manager.lfns2pfns_batch(config.rse, lfns)
                except Exception as exc:
                    # Entire batch failed — mark all as FAILED_CREATION.
                    for (bi, bstage, bchk, bbytes, blfn, blfn_name) in pfn_pending:
                        log.error("[file-%06d] Batch PFN resolution failed: %s", bi, exc)
                        try:
                            if bstage and os.path.exists(bstage):
                                os.unlink(bstage)
                        except OSError:
                            pass
                        state.update(_state_key(bi), status=FileStatus.FAILED_CREATION)
                        failures.append(bi)
                        pbar.update(1)
                    pfn_pending[:] = []
                    return

                for (bi, bstage, bchk, bbytes, blfn, blfn_name) in pfn_pending:
                    try:
                        rucio_pfn = pfn_map[blfn]
                        local_pfn = _pfn_to_local(
                            rucio_pfn, config.rse_pfn_prefix, config.rse_mount
                        )
                        log.debug("[file-%06d] PFN resolved: rucio=%s local=%s",
                                  bi, rucio_pfn, local_pfn)
                        future = placement_executor.submit(
                            _place_file, bstage, local_pfn,
                            config.rse_uid, config.rse_gid,
                        )
                        pending_placements[future] = (
                            bi, bstage, bchk, bbytes, blfn, blfn_name, local_pfn,
                        )
                    except Exception as exc:
                        log.error("[file-%06d] Post-write processing failed: %s", bi, exc)
                        try:
                            if bstage and os.path.exists(bstage):
                                os.unlink(bstage)
                        except OSError:
                            pass
                        state.update(_state_key(bi), status=FileStatus.FAILED_CREATION)
                        failures.append(bi)
                        pbar.update(1)
                pfn_pending[:] = []

            # -- Main write/placement loop ------------------------------------

            # chunksize > 1 batches multiple tasks per pipe transaction,
            # reducing IPC overhead from O(N) round-trips to O(N/chunksize).
            # Formula: enough chunks that each worker always has work queued
            # (4 chunks per worker) without pre-loading so many tasks that
            # progress reporting becomes coarse.  Minimum 1 (single-file runs).
            _chunksize = config.pool_chunksize
            if _chunksize == 0:
                _chunksize = max(1, len(to_generate) // (config.threads * 4))
            log.debug("imap_unordered chunksize=%d (%d tasks, %d workers)",
                      _chunksize, len(to_generate), config.threads)

            for result_tuple in pool.imap_unordered(
                _write_one_worker, to_generate, chunksize=_chunksize
            ):
                idx, staging_path, checksum_hex, bytes_written, error_str = result_tuple
                key = _state_key(idx)

                if error_str is not None:
                    log.error("[file-%06d] Write failed: %s", idx, error_str)
                    state.update(key, status=FileStatus.FAILED_CREATION)
                    failures.append(idx)
                    pbar.update(1)
                    _collect_done_placements()
                    continue

                # Post-write: compute LFN then either process immediately
                # (dry-run) or buffer for batch PFN resolution.
                try:
                    lfn_name = "{}_{}".format(config.file_prefix, checksum_hex)
                    lfn = "{}:{}".format(config.scope, lfn_name)

                    if config.dry_run:
                        local_pfn = os.path.join(
                            config.rse_mount, config.scope, lfn_name
                        )
                        log.debug("[file-%06d] dry-run: synthetic pfn=%s", idx, local_pfn)
                        state.update(
                            key,
                            status=FileStatus.CREATED,
                            lfn=lfn,
                            lfn_name=lfn_name,
                            pfn=local_pfn,
                            bytes=bytes_written,
                            adler32=checksum_hex,
                        )
                        if config.xattr and _HAS_SETXATTR:
                            log.info("[DRY-RUN] Would set XrdCks xattr: %s",
                                     local_pfn)
                        results.append({
                            "key": key,
                            "lfn": lfn,
                            "lfn_name": lfn_name,
                            "pfn": local_pfn,
                            "bytes": bytes_written,
                            "adler32": checksum_hex,
                        })
                        log.info("[file-%06d] Created: %s (%d bytes, adler32=%s)",
                                 idx, lfn, bytes_written, checksum_hex)
                        pbar.update(1)
                    else:
                        # Buffer this result for batch PFN resolution.
                        pfn_pending.append(
                            (idx, staging_path, checksum_hex, bytes_written, lfn, lfn_name)
                        )
                        if len(pfn_pending) >= _pfn_batch_size:
                            _flush_pfn_pending()

                except Exception as exc:
                    log.error("[file-%06d] Post-write processing failed: %s", idx, exc)
                    try:
                        if staging_path and not config.dry_run \
                                and os.path.exists(staging_path):
                            os.unlink(staging_path)
                    except OSError:
                        pass
                    state.update(key, status=FileStatus.FAILED_CREATION)
                    failures.append(idx)
                    pbar.update(1)

                # Collect any placements that finished while we processed
                # this write result so the dict stays bounded.
                _collect_done_placements()

            # All writes finished; flush any remaining buffered PFN resolutions,
            # then wait for outstanding placements.
            _flush_pfn_pending()
            for future in list(pending_placements):
                _handle_placement_future(future)

        pool.close()

    except BaseException:   # Fix 1: catches KeyboardInterrupt, SystemExit, etc.
        log.warning("Interrupted — terminating worker pool")
        pool.terminate()
        # Flush any buffered state updates so resume does not re-process
        # files that were already created before the interrupt.
        state.flush()
        raise

    finally:
        pool.join()
        # wait=True: in the happy path all futures are already done (zero cost).
        # In the error path this allows in-flight placement threads to complete
        # or fail cleanly, preventing orphaned .part files on the RSE.
        placement_executor.shutdown(wait=True)
        listener.stop()

    # Flush any buffered state updates that were held back by flush_interval.
    # In the flush_interval=1 case this is a no-op (dirty==0).
    state.flush()

    if failures:
        log.warning(
            "%d file(s) failed generation: indices %s",
            len(failures), failures,
        )
    else:
        log.info("Generation complete: %d file(s) created successfully", len(to_generate))

    return results
