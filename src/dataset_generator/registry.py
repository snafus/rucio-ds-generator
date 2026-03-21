"""
registry.py — Global dataset registry for rucio-ds-generator.

Records every dataset that this tool creates or appends to, persisted as a
JSON file in the user's home directory (default location).  The file is
updated atomically after each successful run so that a crash during the
write never corrupts the existing data.

Default path
------------
``~/.rucio-ds-generator/registry.json``

Override with the ``registry_file`` YAML key or ``--registry-file`` CLI flag.
Set to an empty string or ``null`` to disable registry recording.

Schema (version 1)
------------------
.. code-block:: json

    {
      "version": 1,
      "datasets": {
        "scope:dataset_name": {
          "dataset_did":  "scope:dataset_name",
          "rse":          "T2_US_TEST",
          "rule_id":      "abc123...",
          "num_files":    42,
          "first_seen":   "2026-03-21T09:00:00Z",
          "last_updated": "2026-03-21T10:30:00Z"
        }
      }
    }

``num_files`` is cumulative across all runs that targeted the same dataset.
``rule_id`` is updated to the value from the most recent run; if the current
run received a DuplicateRule response (``rule_id=None``), the previously
recorded rule ID is preserved.
"""

import json
import logging
import os
import threading
from datetime import datetime, timezone

log = logging.getLogger(__name__)

_REGISTRY_VERSION = 1
DEFAULT_REGISTRY_FILE = os.path.expanduser("~/.rucio-ds-generator/registry.json")


def _utcnow():
    # type: () -> str
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class DatasetRegistry(object):
    """
    Persistent registry of datasets created by this tool.

    Thread-safe: all reads and writes are protected by a ``threading.Lock``.
    Writes are atomic: the file is written to a ``{path}.tmp`` sibling and
    renamed into place so a crash mid-write never corrupts the registry.

    Parameters
    ----------
    path:
        Absolute path to the registry JSON file.  The parent directory is
        created automatically if it does not exist.
    """

    def __init__(self, path):
        # type: (str) -> None
        self._path = path
        self._lock = threading.Lock()
        self._data = self._load()

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def record(self, dataset_did, rse, rule_id, num_files):
        # type: (str, str, object, int) -> None
        """
        Upsert a dataset entry and persist the registry.

        Parameters
        ----------
        dataset_did:
            Full dataset DID (``scope:name``).
        rse:
            RSE expression used for the replication rule.
        rule_id:
            Replication rule ID string, or ``None`` if the rule already
            existed (DuplicateRule).  When ``None``, any previously recorded
            rule ID for this dataset is preserved.
        num_files:
            Number of files registered in this run (added to the running
            total for this dataset DID).
        """
        with self._lock:
            existing = self._data["datasets"].get(dataset_did, {})
            self._data["datasets"][dataset_did] = {
                "dataset_did":  dataset_did,
                "rse":          rse,
                "rule_id":      rule_id if rule_id is not None else existing.get("rule_id"),
                "num_files":    existing.get("num_files", 0) + num_files,
                "first_seen":   existing.get("first_seen", _utcnow()),
                "last_updated": _utcnow(),
            }
            self._save()
            log.debug(
                "Registry updated: %s  rule=%s  total_files=%d",
                dataset_did,
                self._data["datasets"][dataset_did]["rule_id"],
                self._data["datasets"][dataset_did]["num_files"],
            )

    def entries(self):
        # type: () -> list
        """Return a snapshot of all registry entries as a list of dicts."""
        with self._lock:
            return list(self._data["datasets"].values())

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load(self):
        # type: () -> dict
        """Load the registry file, or return a fresh empty structure."""
        if not os.path.exists(self._path):
            return {"version": _REGISTRY_VERSION, "datasets": {}}
        try:
            with open(self._path, "r") as fh:
                data = json.load(fh)
            if data.get("version") != _REGISTRY_VERSION:
                log.warning(
                    "Registry at %r has unexpected version %r — starting fresh",
                    self._path, data.get("version"),
                )
                return {"version": _REGISTRY_VERSION, "datasets": {}}
            return data
        except (ValueError, KeyError, IOError) as exc:
            log.warning("Could not read registry %r (%s) — starting fresh", self._path, exc)
            return {"version": _REGISTRY_VERSION, "datasets": {}}

    def _save(self):
        # type: () -> None
        """Atomically write the registry to disk."""
        parent = os.path.dirname(self._path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        tmp_path = "{}.tmp".format(self._path)
        try:
            with open(tmp_path, "w") as fh:
                json.dump(self._data, fh, indent=2)
                fh.write("\n")
            os.rename(tmp_path, self._path)
        except OSError as exc:
            log.error("Failed to write registry %r: %s", self._path, exc)
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
