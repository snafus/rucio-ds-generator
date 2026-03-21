"""
config.py — Configuration for claude-dataset-generator.

Merges a YAML config file with CLI arguments into a single ``Config`` object.
CLI arguments always take precedence over YAML values.

Design notes
------------
* Plain class with ``__init__`` — no ``dataclasses`` (Python 3.6 safe).
* ``from_yaml_and_args`` is the canonical factory; direct construction is for
  tests only.
* ``validate()`` must be called after construction to detect illegal
  combinations (e.g. ``--create-only`` + ``--register-only``).
* ``run_id`` is a 12-character hex string derived from a random UUID.
  On resume the caller is responsible for overriding ``run_id`` with the
  value stored in the state file.
"""

import os
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

import yaml


# ---------------------------------------------------------------------------
# Sentinel for "not provided" — distinguishes None from an explicit None
# ---------------------------------------------------------------------------
_MISSING = object()


class ConfigError(ValueError):
    """Raised when the merged configuration is invalid."""


class Config(object):
    """
    Merged configuration object.

    Attributes are documented next to their ``__init__`` parameters.
    Prefer accessing settings through the computed properties
    ``dataset_name``, ``dataset_did``, and ``state_file_path`` rather than
    composing strings elsewhere.
    """

    # Defaults applied when neither YAML nor CLI supply a value.
    _DEFAULTS = {
        "threads": 4,
        "log_level": "INFO",
        "dry_run": False,
        "create_only": False,
        "register_only": False,
        "cleanup": False,
        "state_file": None,
        "run_id": None,
        "rule_lifetime": None,   # None means no expiry (permanent rule)
    }

    def __init__(
        self,
        scope,           # type: str   Rucio scope for all DIDs
        rse,             # type: str   Target RSE name
        rse_mount,       # type: str   POSIX mount path of RSE storage on this host
        dataset_prefix,  # type: str   Dataset DID name prefix
        file_prefix,     # type: str   File LFN prefix (before checksum suffix)
        num_files,       # type: int   Number of files to generate per run
        file_size_bytes, # type: int   Size of each generated file in bytes
        token_endpoint,  # type: str   OIDC token endpoint URL
        client_id,       # type: str   OIDC client ID
        client_secret,   # type: str   OIDC client secret
        rucio_host,      # type: str   Rucio server base URL
        rucio_auth_host, # type: str   Rucio authentication server URL
        rucio_account,   # type: str   Rucio account name
        run_id=None,         # type: Optional[str]  Auto-generated if None
        threads=4,           # type: int   Pool-A (file generation) thread count
        log_level="INFO",    # type: str   Python logging level name
        dry_run=False,       # type: bool  Log actions without executing
        state_file=None,     # type: Optional[str]  Override state file path
        create_only=False,   # type: bool           Generate files; skip Rucio registration
        register_only=False, # type: bool           Register already-generated files; skip generation
        cleanup=False,       # type: bool           Remove placed files and orphaned replicas
        rule_lifetime=None,  # type: Optional[int]  Replication rule lifetime in seconds; None = permanent
    ):
        self.scope = scope
        self.rse = rse
        self.rse_mount = rse_mount
        self.dataset_prefix = dataset_prefix
        self.file_prefix = file_prefix
        self.num_files = int(num_files)
        self.file_size_bytes = int(file_size_bytes)
        self.token_endpoint = token_endpoint
        self.client_id = client_id
        self.client_secret = client_secret
        self.rucio_host = rucio_host
        self.rucio_auth_host = rucio_auth_host
        self.rucio_account = rucio_account
        self.run_id = run_id or self._new_run_id()
        self.threads = int(threads)
        self.log_level = log_level.upper()
        self.dry_run = bool(dry_run)
        self.create_only = bool(create_only)
        self.register_only = bool(register_only)
        self.cleanup = bool(cleanup)
        self.rule_lifetime = int(rule_lifetime) if rule_lifetime is not None else None
        self._state_file = state_file

    # ------------------------------------------------------------------
    # Computed properties
    # ------------------------------------------------------------------

    @property
    def dataset_name(self):
        # type: () -> str
        """
        Dataset DID *name* component (without scope prefix).

        Pattern: ``{dataset_prefix}_{YYYYMMDD}_{run_id}``
        The date is fixed at construction time (UTC) so resume runs always
        produce the same dataset name for a given run_id.
        """
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        return "{}_{}_{}".format(self.dataset_prefix, date_str, self.run_id)

    @property
    def dataset_did(self):
        # type: () -> str
        """Full dataset DID: ``{scope}:{dataset_name}``."""
        return "{}:{}".format(self.scope, self.dataset_name)

    @property
    def state_file_path(self):
        # type: () -> str
        """
        Path to the JSON state file.

        Uses ``--state-file`` if supplied; otherwise defaults to
        ``./state_{run_id}.json`` in the current working directory.
        """
        if self._state_file:
            return self._state_file
        return "state_{}.json".format(self.run_id)

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_yaml_and_args(cls, yaml_path, args):
        # type: (Optional[str], Any) -> Config
        """
        Build a ``Config`` from a YAML file and a parsed ``argparse.Namespace``.

        Precedence (highest first):
        1. CLI argument (non-None attribute on *args*)
        2. YAML key
        3. Class default (``_DEFAULTS``)

        Parameters
        ----------
        yaml_path:
            Path to the YAML config file, or ``None`` to skip YAML loading.
        args:
            ``argparse.Namespace`` from ``argparse.ArgumentParser.parse_args()``.
            Missing or ``None`` attributes are ignored.
        """
        yaml_cfg = {}  # type: dict
        if yaml_path:
            with open(yaml_path, "r") as fh:
                yaml_cfg = yaml.safe_load(fh) or {}

        def get(key, required=False):
            # type: (str, bool) -> Any
            """Resolve a single key from CLI → YAML → defaults."""
            cli_val = getattr(args, key, None)
            if cli_val is not None:
                return cli_val
            if key in yaml_cfg and yaml_cfg[key] is not None:
                return yaml_cfg[key]
            if key in cls._DEFAULTS:
                return cls._DEFAULTS[key]
            if required:
                raise ConfigError(
                    "Required setting '{}' not found in YAML config or CLI args".format(key)
                )
            return None

        return cls(
            scope=get("scope", required=True),
            rse=get("rse", required=True),
            rse_mount=get("rse_mount", required=True),
            dataset_prefix=get("dataset_prefix", required=True),
            file_prefix=get("file_prefix", required=True),
            num_files=int(get("num_files", required=True)),
            file_size_bytes=int(get("file_size_bytes", required=True)),
            token_endpoint=get("token_endpoint", required=True),
            client_id=get("client_id", required=True),
            client_secret=get("client_secret", required=True),
            rucio_host=get("rucio_host", required=True),
            rucio_auth_host=get("rucio_auth_host", required=True),
            rucio_account=get("rucio_account", required=True),
            run_id=get("run_id"),
            threads=int(get("threads")),
            log_level=get("log_level"),
            dry_run=bool(get("dry_run")),
            state_file=get("state_file"),
            create_only=bool(get("create_only")),
            register_only=bool(get("register_only")),
            cleanup=bool(get("cleanup")),
            rule_lifetime=get("rule_lifetime"),
        )

    # ------------------------------------------------------------------
    # Validation
    # ------------------------------------------------------------------

    def validate(self):
        # type: () -> None
        """
        Raise ``ConfigError`` if the configuration contains illegal values
        or combinations.

        Call this after construction and before starting any work.
        """
        if self.create_only and self.register_only:
            raise ConfigError("--create-only and --register-only are mutually exclusive")
        if self.cleanup and (self.create_only or self.register_only):
            raise ConfigError("--cleanup cannot be combined with --create-only or --register-only")
        if self.threads < 1:
            raise ConfigError("threads must be >= 1, got {}".format(self.threads))
        if self.num_files < 1:
            raise ConfigError("num_files must be >= 1, got {}".format(self.num_files))
        if self.file_size_bytes < 1:
            raise ConfigError("file_size_bytes must be >= 1, got {}".format(self.file_size_bytes))
        if self.rule_lifetime is not None and self.rule_lifetime < 1:
            raise ConfigError(
                "rule_lifetime must be >= 1 second, got {}".format(self.rule_lifetime)
            )
        if not self.dry_run and not os.path.isdir(self.rse_mount):
            raise ConfigError(
                "rse_mount '{}' is not a directory or does not exist. "
                "The RSE storage must be POSIX-mounted on this host.".format(self.rse_mount)
            )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _new_run_id():
        # type: () -> str
        """Generate a 12-character random hexadecimal run identifier."""
        return uuid.uuid4().hex[:12]

    def __repr__(self):
        return (
            "Config(scope={!r}, rse={!r}, num_files={}, file_size_bytes={}, "
            "run_id={!r}, dry_run={})".format(
                self.scope, self.rse, self.num_files, self.file_size_bytes,
                self.run_id, self.dry_run,
            )
        )
