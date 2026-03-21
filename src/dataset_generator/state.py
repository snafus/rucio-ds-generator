"""
state.py — Persistent JSON state file for rucio-ds-generator.

Responsibilities
----------------
* Track the lifecycle of every generated file across the pipeline stages:
  ``created`` → ``registered`` → ``ruled`` (or one of the ``failed_*`` states).
* Allow the run to be resumed after interruption without re-doing completed
  steps and without losing work.
* Guarantee atomic, thread-safe writes so no partial state is ever persisted.

Thread-safety model
-------------------
All mutations go through ``update()``, which holds ``_lock`` for the duration
of both the in-memory update and the atomic disk write.  Reads via
``get_file`` and ``get_files_by_status`` do NOT hold the lock (callers that
need a consistent snapshot should use ``snapshot()`` instead).

Atomic write strategy
---------------------
1. Serialize state to JSON.
2. Write to ``{state_file}.{pid}.tmp``.
3. ``os.rename`` the temp file onto the canonical path.

``os.rename`` is atomic on POSIX filesystems, so readers always see either
the old state or the new state — never a partially written file.  Using the
PID in the temp name prevents collisions if multiple processes write to the
same state file (only the last writer wins, but no corruption occurs).

State schema (version 1)
------------------------
.. code-block:: json

    {
        "version": 1,
        "run_id": "abc123def456",
        "files": {
            "file_000000": {
                "status": "created",
                "lfn": "cms:prefix_1a2b3c4d",
                "lfn_name": "prefix_1a2b3c4d",
                "pfn": "/mnt/rse/cms/ab/cd/prefix_1a2b3c4d",
                "bytes": 1073741824,
                "adler32": "1a2b3c4d"
            }
        }
    }

All fields except ``status`` may be absent for entries that have not yet
completed the ``created`` transition (i.e. entries pre-allocated with
``allocate()`` before generation begins).
"""

import json
import logging
import os
import threading
from typing import Dict, List, Optional

log = logging.getLogger(__name__)

STATE_VERSION = 1


class FileStatus(object):
    """String constants for the ``status`` field of a state file entry."""
    PENDING = "pending"                          # Pre-allocated; generation not started
    CREATED = "created"                          # File written to RSE; not yet registered
    REGISTERED = "registered"                    # ``add_replicas`` succeeded
    RULED = "ruled"                              # Replication rule created
    FAILED_CREATION = "failed_creation"          # File generation or rename failed
    FAILED_REGISTRATION = "failed_registration"  # ``add_replicas`` failed
    FAILED_RULE = "failed_rule"                  # ``add_replication_rule`` failed

    # Statuses that count as "done" — never re-processed
    TERMINAL = frozenset([RULED])

    # Statuses that should be retried on resume
    RETRIABLE = frozenset([
        PENDING,
        FAILED_CREATION,
        FAILED_REGISTRATION,
        FAILED_RULE,
    ])


class StateError(RuntimeError):
    """Raised when state file I/O or format validation fails."""


class StateFile(object):
    """
    Thread-safe, atomically-written JSON state file.

    Parameters
    ----------
    path:
        Absolute or relative path to the ``state_{run_id}.json`` file.
    run_id:
        Run identifier string.  Used to cross-check the state file on load.
    """

    def __init__(self, path, run_id):
        # type: (str, str) -> None
        self._path = path
        self._run_id = run_id
        self._lock = threading.Lock()
        self._state = {}  # type: dict  # in-memory representation
        self._load_or_create()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def allocate(self, key):
        # type: (str) -> None
        """
        Pre-allocate a state entry for *key* with status ``pending``.

        Idempotent: if the key already exists, this is a no-op.  This allows
        the caller to call ``allocate`` for every expected file at startup
        without worrying about overwriting in-progress entries on resume.
        """
        with self._lock:
            if key not in self._state["files"]:
                self._state["files"][key] = {"status": FileStatus.PENDING}
                self._save_locked()

    def update(self, key, **fields):
        # type: (str, **object) -> None
        """
        Update fields of an existing state entry and persist immediately.

        The ``key`` must already exist (created by ``allocate``).  Passing
        ``status=FileStatus.CREATED`` alongside ``lfn``, ``bytes``, etc. is
        the canonical pattern after a file is successfully generated.

        Parameters
        ----------
        key:
            State entry identifier (e.g. ``"file_000000"``).
        **fields:
            Arbitrary fields to merge into the entry dict.  A ``status``
            field should always be included.
        """
        with self._lock:
            if key not in self._state["files"]:
                raise StateError("State key {!r} has not been allocated".format(key))
            self._state["files"][key].update(fields)
            self._save_locked()

    def count(self):
        # type: () -> int
        """Return the total number of state entries regardless of status."""
        return len(self._state["files"])

    def get_file(self, key):
        # type: (str) -> Optional[dict]
        """Return a *copy* of the state entry for *key*, or ``None``."""
        entry = self._state["files"].get(key)
        return dict(entry) if entry is not None else None

    def get_files_by_status(self, *statuses):
        # type: (*str) -> List[dict]
        """
        Return a list of entry dicts whose ``status`` is in *statuses*.

        Each returned dict includes the ``key`` field so the caller can
        reference the entry later.
        """
        status_set = set(statuses)
        result = []
        for key, entry in self._state["files"].items():
            if entry.get("status") in status_set:
                row = dict(entry)
                row["key"] = key
                result.append(row)
        return result

    def snapshot(self):
        # type: () -> dict
        """
        Return a deep copy of the entire in-memory state under the lock.

        Use this when you need a consistent multi-key view (e.g. to build
        a registration batch).
        """
        with self._lock:
            import copy
            return copy.deepcopy(self._state)

    @property
    def run_id(self):
        # type: () -> str
        """The run_id stored in the state file."""
        return self._state.get("run_id", self._run_id)

    @property
    def path(self):
        # type: () -> str
        """Canonical path to the state file on disk."""
        return self._path

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load_or_create(self):
        # type: () -> None
        """
        Load an existing state file or create a fresh one.

        On load, validates that the ``version`` and ``run_id`` fields match
        expectations.  Raises ``StateError`` on version mismatch.  A
        ``run_id`` mismatch raises ``StateError`` unless the caller
        constructed us with the run_id read from the file (handled externally
        by ``__main__``).
        """
        if os.path.exists(self._path):
            try:
                with open(self._path, "r") as fh:
                    self._state = json.load(fh)
            except (IOError, ValueError) as exc:
                raise StateError(
                    "Failed to load state file {!r}: {}".format(self._path, exc)
                )
            self._validate_loaded()
            log.info("Loaded existing state file: %s (%d file entries)",
                     self._path, len(self._state.get("files", {})))
        else:
            self._state = {
                "version": STATE_VERSION,
                "run_id": self._run_id,
                "files": {},
            }
            self._save_locked()
            log.info("Created new state file: %s", self._path)

    def _validate_loaded(self):
        # type: () -> None
        """Raise ``StateError`` if the loaded state is structurally invalid."""
        version = self._state.get("version")
        if version != STATE_VERSION:
            raise StateError(
                "State file {!r} has unsupported version {} (expected {})".format(
                    self._path, version, STATE_VERSION
                )
            )
        if "files" not in self._state or not isinstance(self._state["files"], dict):
            raise StateError(
                "State file {!r} is missing the 'files' dict".format(self._path)
            )
        stored_run_id = self._state.get("run_id")
        if stored_run_id and stored_run_id != self._run_id:
            raise StateError(
                "State file run_id {!r} does not match expected {!r}. "
                "Use --state-file to point to the correct state file, or "
                "--run-id to override the run identifier.".format(
                    stored_run_id, self._run_id
                )
            )

    def _save_locked(self):
        # type: () -> None
        """
        Atomically write the current in-memory state to disk.

        MUST be called while ``_lock`` is held.  Writes to a PID-namespaced
        temp file then renames over the canonical path.
        """
        tmp_path = "{}.{}.tmp".format(self._path, os.getpid())
        try:
            with open(tmp_path, "w") as fh:
                json.dump(self._state, fh, indent=2, sort_keys=True)
                fh.write("\n")
            os.rename(tmp_path, self._path)
        except (IOError, OSError) as exc:
            # Best-effort cleanup of the temp file
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise StateError(
                "Failed to write state file {!r}: {}".format(self._path, exc)
            )
