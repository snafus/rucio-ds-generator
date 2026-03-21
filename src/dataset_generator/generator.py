"""
generator.py — File generation for rucio-ds-generator.

This module handles Phase 1 of the pipeline: creating random binary files
on the POSIX-mounted RSE storage and recording their state.

File generation pipeline (per file)
-------------------------------------
1. Write random data to a temp path in ``staging_dir`` using 128 MiB chunks.
   The adler32 checksum is accumulated incrementally — the full file is never
   held in memory.  ``staging_dir`` may be on a different filesystem from the
   RSE mount.
2. Compute the final LFN name: ``{file_prefix}_{adler32_hex}`` where
   ``adler32_hex`` is the checksum formatted as 8-char lowercase hex.
3. Call ``RucioManager.lfns2pfn`` to resolve the canonical PFN for the LFN.
   Translate it to the local filesystem path via ``_pfn_to_local`` (strips
   any ``rse_pfn_prefix`` and prepends ``rse_mount``).
4. Create parent (hash) directories of the local PFN (``_makedirs_chown``).
5. Move the staged file to a ``.part`` temp name *on the RSE filesystem*:
   - If staging and RSE are on the same filesystem: ``os.rename`` (instant).
   - If cross-filesystem: ``shutil.copy2`` then ``os.unlink`` the staged copy.
6. Set ownership (uid/gid) on the ``.part`` file while it still has a
   temporary name — ownership is preserved across the final rename.
7. Atomically rename ``.part`` → final PFN path (``os.rename``).  Both names
   are on the RSE filesystem so this rename is always atomic.
8. Update the state entry to ``FileStatus.CREATED``.

Threading model
---------------
Pool A (``--threads``, default 4) is used for file generation.  This pool
is CPU/IO bound (``FileWriter.write_file`` + staging I/O).  A separate Pool B (in
``rucio_client`` / ``__main__``) handles network-bound Rucio API calls.

Each thread pre-allocates its state entry at submission time, before any
I/O begins.  On failure the entry is updated to ``FileStatus.FAILED_CREATION``
so that a subsequent resume run can retry it.

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
import os
import shutil
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional

from tqdm import tqdm

from .state import FileStatus, StateFile
from .writers import FileWriter, get_file_writer

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _state_key(idx):
    # type: (int) -> str
    """Canonical state key for the file at zero-based *idx*."""
    return "file_{:06d}".format(idx)


def _generate_one(idx, config, state, rucio_manager, writer, progress_lock, progress_bar):
    # type: (int, object, StateFile, object, FileWriter, threading.Lock, tqdm) -> dict
    """
    Generate a single file and update state.

    Called from Pool A worker threads.  Returns a dict with file metadata on
    success; raises on unrecoverable error (state is set to FAILED_CREATION
    before raising so the caller does not need to do so).

    Parameters
    ----------
    idx:
        Zero-based file index within this run.
    config:
        ``Config`` instance.
    state:
        ``StateFile`` instance shared across all threads.
    rucio_manager:
        ``RucioManager`` instance — used for ``lfns2pfn`` only.
    writer:
        ``FileWriter`` instance used to write the file contents.  Shared
        across all Pool A threads; must be thread-safe.
    progress_lock:
        Lock protecting writes to the ``tqdm`` progress bar from multiple
        threads.
    progress_bar:
        ``tqdm`` instance for the overall generation progress.
    """
    key = _state_key(idx)
    thread_name = threading.current_thread().name

    # config.staging_dir is a unique per-run temp dir created by main() before
    # any generation starts.  It is guaranteed to exist and is exclusive to
    # this invocation, so a simple index-based name is sufficient.
    staging_dir = config.staging_dir
    tmp_name = "{}.tmp.{:06d}".format(config.file_prefix, idx)
    tmp_path = os.path.join(staging_dir, tmp_name)

    log.debug("[%s|file-%06d] Starting generation (staging=%s)", thread_name, idx, staging_dir)

    try:
        if config.dry_run:
            checksum_val = 0xDEADBEEF & 0xFFFFFFFF
            bytes_written = config.file_size_bytes
        else:
            checksum_val, bytes_written = writer.write_file(tmp_path, config.file_size_bytes)

        checksum_hex = format(checksum_val, "08x")
        lfn_name = "{}_{}".format(config.file_prefix, checksum_hex)
        lfn = "{}:{}".format(config.scope, lfn_name)

        log.debug("[%s|file-%06d] checksum=%s lfn=%s", thread_name, idx, checksum_hex, lfn)

        # Resolve Rucio PFN, then translate to local filesystem path.
        if config.dry_run:
            local_pfn = os.path.join(config.rse_mount, config.scope, lfn_name)
        else:
            rucio_pfn = rucio_manager.lfns2pfn(config.rse, lfn)
            local_pfn = _pfn_to_local(rucio_pfn, config.rse_pfn_prefix, config.rse_mount)

        if not config.dry_run:
            _place_file(tmp_path, local_pfn, uid=config.rse_uid, gid=config.rse_gid)
            log.debug("[%s|file-%06d] Placed at: %s", thread_name, idx, local_pfn)

        state.update(
            key,
            status=FileStatus.CREATED,
            lfn=lfn,
            lfn_name=lfn_name,
            pfn=local_pfn,
            bytes=bytes_written,
            adler32=checksum_hex,
        )

        log.info("[file-%06d] Created: %s (%d bytes, adler32=%s)",
                 idx, lfn, bytes_written, checksum_hex)

        with progress_lock:
            progress_bar.update(1)

        return {
            "key": key,
            "lfn": lfn,
            "lfn_name": lfn_name,
            "pfn": local_pfn,
            "bytes": bytes_written,
            "adler32": checksum_hex,
        }

    except Exception as exc:
        log.error("[%s|file-%06d] Generation failed: %s", thread_name, idx, exc)
        # Best-effort cleanup of any partial staging file
        try:
            if not config.dry_run and os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except OSError:
            pass
        state.update(key, status=FileStatus.FAILED_CREATION)
        raise


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


def _place_file(staging_path, local_pfn, uid=None, gid=None):
    # type: (str, str, Optional[int], Optional[int]) -> None
    """
    Move a fully-written staged file to its final location on the RSE,
    creating hash directories as needed, with an atomic final rename.

    Two-step placement guarantees the final PFN is never partially present:

    1. Move *staging_path* to ``{local_pfn}.part.{pid}.{tid}`` — a temporary
       name *on the RSE filesystem*.  If staging and RSE are on the same
       filesystem ``os.rename`` is used (instant, no data copy).  If they are
       on different filesystems (``EXDEV``), ``shutil.copy2`` is used followed
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
        _makedirs_chown(parent, uid, gid)

    part_path = "{}.part.{}.{}".format(local_pfn, os.getpid(), threading.get_ident())

    # Step 1: move from staging to RSE filesystem (handles cross-filesystem).
    try:
        os.rename(staging_path, part_path)
    except OSError as exc:
        if exc.errno != errno.EXDEV:
            raise
        # Cross-filesystem: copy bytes then remove the staging copy.
        shutil.copy2(staging_path, part_path)
        os.unlink(staging_path)

    # Steps 2 & 3: chown then atomic rename on the RSE filesystem.
    # If either fails the .part file is removed so it does not linger on the RSE.
    try:
        if uid is not None or gid is not None:
            os.chown(part_path,
                     uid if uid is not None else -1,
                     gid if gid is not None else -1)

        os.rename(part_path, local_pfn)
    except Exception:
        try:
            os.unlink(part_path)
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_generation(config, state, rucio_manager, new_count=None):
    # type: (object, StateFile, object, Optional[int]) -> List[dict]
    """
    Generate files for this run using Pool A (``config.threads`` threads).

    Files whose state entry is already ``CREATED``, ``REGISTERED``, or
    ``RULED`` are skipped.  Files in ``FAILED_CREATION`` (or ``PENDING``) are
    (re-)generated.

    Parameters
    ----------
    config:
        ``Config`` instance.
    state:
        ``StateFile`` instance (already loaded or created by ``__main__``).
    rucio_manager:
        ``RucioManager`` instance for ``lfns2pfn`` calls.
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
    if new_count is None:
        # Default: manage exactly config.num_files entries total (original
        # behaviour — the caller has not told us about pre-existing Rucio files).
        total = config.num_files
    else:
        # Additive: caller has computed how many *additional* files are needed
        # on top of what the state file already tracks.
        total = state.count() + new_count

    # Pre-allocate state entries for all expected files so resume logic works.
    for idx in range(total):
        state.allocate(_state_key(idx))

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
        "Generating %d file(s) using %d thread(s) [%d already done]",
        len(to_generate), config.threads, len(already_done),
    )

    writer = get_file_writer(config.generation_mode, config)
    log.info("File writer: %s", writer.description)

    results = list(already_done)
    failures = []

    progress_lock = threading.Lock()
    with tqdm(
        total=total,
        initial=len(already_done),
        unit="file",
        desc="Generating",
        dynamic_ncols=True,
    ) as pbar:
        with ThreadPoolExecutor(
            max_workers=config.threads,
            thread_name_prefix="gen",
        ) as pool:
            futures = {
                pool.submit(
                    _generate_one,
                    idx, config, state, rucio_manager, writer, progress_lock, pbar,
                ): idx
                for idx in to_generate
            }

            for future in as_completed(futures):
                idx = futures[future]
                try:
                    result = future.result()
                    results.append(result)
                except Exception as exc:
                    log.error("File index %d failed permanently: %s", idx, exc)
                    failures.append(idx)

    if failures:
        log.warning(
            "%d file(s) failed generation: indices %s",
            len(failures), failures,
        )
    else:
        log.info("Generation complete: %d file(s) created successfully", len(to_generate))

    return results
