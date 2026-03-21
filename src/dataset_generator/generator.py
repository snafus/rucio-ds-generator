"""
generator.py — File generation for claude-dataset-generator.

This module handles Phase 1 of the pipeline: creating random binary files
on the POSIX-mounted RSE storage and recording their state.

File generation pipeline (per file)
-------------------------------------
1. Write random data to a temp path in ``{rse_mount}/.gen_tmp/`` using
   64 MiB chunks.  The adler32 checksum is accumulated incrementally over
   each chunk — the full file is never held in memory.
2. Compute the final LFN name: ``{file_prefix}_{adler32_hex}`` where
   ``adler32_hex`` is the checksum formatted as 8-char lowercase hex.
3. Call ``RucioManager.lfns2pfn`` to resolve the canonical physical path
   for the final LFN on the target RSE.
4. Create parent directories of the final PFN (``os.makedirs`` with
   ``exist_ok=True``).
5. Atomically rename the temp file to the final PFN path (``os.rename``).
   This is POSIX-atomic on the same filesystem; both the temp dir and the
   final path are under ``rse_mount``, guaranteeing the same filesystem.
6. Update the state entry to ``FileStatus.CREATED`` with lfn, bytes,
   adler32, and pfn fields.

Threading model
---------------
Pool A (``--threads``, default 4) is used for file generation.  This pool
is CPU/IO bound (``os.urandom`` + write).  A separate Pool B (in
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

import logging
import os
import threading
import zlib
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List

from tqdm import tqdm

from .state import FileStatus, StateFile

log = logging.getLogger(__name__)

CHUNK_SIZE = 64 * 1024 * 1024  # 64 MiB per write chunk


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _state_key(idx):
    # type: (int) -> str
    """Canonical state key for the file at zero-based *idx*."""
    return "file_{:06d}".format(idx)


def _generate_one(idx, config, state, rucio_manager, progress_lock, progress_bar):
    # type: (int, object, StateFile, object, threading.Lock, tqdm) -> dict
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
    progress_lock:
        Lock protecting writes to the ``tqdm`` progress bar from multiple
        threads.
    progress_bar:
        ``tqdm`` instance for the overall generation progress.
    """
    key = _state_key(idx)
    pid = os.getpid()
    thread_name = threading.current_thread().name

    tmp_dir = os.path.join(config.rse_mount, ".gen_tmp")
    tmp_name = "{}.tmp.{}.{}".format(config.file_prefix, pid, idx)
    tmp_path = os.path.join(tmp_dir, tmp_name)

    log.debug("[%s|file-%06d] Starting generation", thread_name, idx)

    try:
        if config.dry_run:
            checksum_val = 0xDEADBEEF & 0xFFFFFFFF
            bytes_written = config.file_size_bytes
        else:
            os.makedirs(tmp_dir, exist_ok=True)
            checksum_val, bytes_written = _write_file(tmp_path, config.file_size_bytes)

        checksum_hex = format(checksum_val, "08x")
        lfn_name = "{}_{}".format(config.file_prefix, checksum_hex)
        lfn = "{}:{}".format(config.scope, lfn_name)

        log.debug("[%s|file-%06d] checksum=%s lfn=%s", thread_name, idx, checksum_hex, lfn)

        # Resolve physical path via Rucio lfns2pfns
        if config.dry_run:
            pfn = os.path.join(config.rse_mount, config.scope, lfn_name)
        else:
            pfn = rucio_manager.lfns2pfn(config.rse, lfn)

        if not config.dry_run:
            _place_file(tmp_path, pfn)
            log.debug("[%s|file-%06d] Placed at: %s", thread_name, idx, pfn)

        state.update(
            key,
            status=FileStatus.CREATED,
            lfn=lfn,
            lfn_name=lfn_name,
            pfn=pfn,
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
            "pfn": pfn,
            "bytes": bytes_written,
            "adler32": checksum_hex,
        }

    except Exception as exc:
        log.error("[%s|file-%06d] Generation failed: %s", thread_name, idx, exc)
        # Best-effort cleanup of any partial temp file
        try:
            if not config.dry_run and os.path.exists(tmp_path):
                os.unlink(tmp_path)
        except OSError:
            pass
        state.update(key, status=FileStatus.FAILED_CREATION)
        raise


def _write_file(path, size_bytes):
    # type: (str, int) -> tuple
    """
    Write *size_bytes* of random data to *path* in ``CHUNK_SIZE`` chunks.

    Returns ``(checksum_val, bytes_written)`` where ``checksum_val`` is the
    accumulated adler32 with the ``& 0xFFFFFFFF`` mask applied.

    The checksum initial value of ``1`` matches the adler32 standard
    (``zlib.adler32`` uses 1 as its default seed).
    """
    checksum = 1  # adler32 initial value per RFC 1950
    bytes_written = 0
    remaining = size_bytes

    with open(path, "wb") as fh:
        while remaining > 0:
            chunk = min(CHUNK_SIZE, remaining)
            data = os.urandom(chunk)
            fh.write(data)
            # Apply mask on every iteration to keep value in 32-bit range.
            checksum = zlib.adler32(data, checksum) & 0xFFFFFFFF
            bytes_written += chunk
            remaining -= chunk

    return checksum, bytes_written


def _place_file(tmp_path, final_pfn):
    # type: (str, str) -> None
    """
    Atomically rename *tmp_path* to *final_pfn*.

    Creates parent directories of *final_pfn* if they do not exist.
    Both paths must be on the same POSIX filesystem (guaranteed because both
    live under ``config.rse_mount``).
    """
    parent = os.path.dirname(final_pfn)
    if parent:
        os.makedirs(parent, exist_ok=True)
    os.rename(tmp_path, final_pfn)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def run_generation(config, state, rucio_manager):
    # type: (object, StateFile, object) -> List[dict]
    """
    Generate all files for this run using Pool A (``config.threads`` threads).

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

    Returns
    -------
    list of dict
        Metadata dicts for every successfully generated file (including
        those that were already in ``CREATED`` state from a prior run).
    """
    # Pre-allocate state entries for all expected files so resume logic works.
    for idx in range(config.num_files):
        state.allocate(_state_key(idx))

    # Determine which files still need generation.
    done_statuses = {FileStatus.CREATED, FileStatus.REGISTERED, FileStatus.RULED}
    to_generate = []   # type: List[int]
    already_done = []  # type: List[dict]

    for idx in range(config.num_files):
        key = _state_key(idx)
        entry = state.get_file(key)
        if entry and entry.get("status") in done_statuses:
            already_done.append(entry)
            already_done[-1]["key"] = key
        else:
            to_generate.append(idx)

    if not to_generate:
        log.info("All %d files already generated — skipping Pool A", config.num_files)
        return already_done

    log.info(
        "Generating %d file(s) using %d thread(s) [%d already done]",
        len(to_generate), config.threads, len(already_done),
    )

    results = list(already_done)
    failures = []

    progress_lock = threading.Lock()
    with tqdm(
        total=config.num_files,
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
                    idx, config, state, rucio_manager, progress_lock, pbar,
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
