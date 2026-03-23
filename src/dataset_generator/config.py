"""
config.py — Configuration for rucio-ds-generator.

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
from typing import Any, Dict, Optional

import yaml
from jinja2 import Template, TemplateError


def _human_size(size_bytes, si=False):
    # type: (int, bool) -> str
    """Return *size_bytes* as a compact human-readable string.

    When *si* is ``False`` (default, IEC): powers of 1024 with binary
    suffixes — e.g. ``512B``, ``2KiB``, ``64MiB``, ``1GiB``, ``2TiB``.

    When *si* is ``True`` (SI): powers of 1000 with decimal suffixes —
    e.g. ``512B``, ``2KB``, ``64MB``, ``1GB``, ``2TB``.

    Values are always formatted as integers (no decimal point) so the
    result is safe to embed in Rucio DID names.
    """
    if si:
        divisor = 1000.0
        units = ("B", "KB", "MB", "GB", "TB", "PB")
    else:
        divisor = 1024.0
        units = ("B", "KiB", "MiB", "GiB", "TiB", "PiB")
    value = float(size_bytes)
    for unit in units[:-1]:
        if value < divisor:
            return "{}{}".format(int(value), unit)
        value /= divisor
    return "{}{}".format(int(value), units[-1])


# SI (decimal, powers of 1000): KB, MB, GB, TB, PB
# IEC (binary, powers of 1024): KiB, MiB, GiB, TiB, PiB
# Bare letters (K, M, G, T, P) are treated as binary (IEC).
_SIZE_UNITS = {
    "B":   1,
    # IEC / bare
    "K":   1024,       "KIB": 1024,
    "M":   1024 ** 2,  "MIB": 1024 ** 2,
    "G":   1024 ** 3,  "GIB": 1024 ** 3,
    "T":   1024 ** 4,  "TIB": 1024 ** 4,
    "P":   1024 ** 5,  "PIB": 1024 ** 5,
    # SI
    "KB":  1000,
    "MB":  1000 ** 2,
    "GB":  1000 ** 3,
    "TB":  1000 ** 4,
    "PB":  1000 ** 5,
}


def _parse_size(value):
    # type: (object) -> int
    """
    Convert *value* to a byte count (``int``).

    Accepts:

    * An integer or a plain integer string: ``1073741824``, ``"1073741824"``
    * A human-readable string with an optional space before the unit:
      ``"1GiB"``, ``"512 MiB"``, ``"1GB"``, ``"64 KiB"``, ``"1.5 GiB"``

    Unit matching is case-insensitive for the letter case of the prefix, but
    the ``i`` in IEC units (``KiB``/``MiB``/``GiB``…) **is** significant:

    * SI  (decimal): ``KB`` = 10³,  ``MB`` = 10⁶,  ``GB`` = 10⁹, …
    * IEC (binary):  ``KiB`` = 1024, ``MiB`` = 1024², ``GiB`` = 1024³, …
    * Bare letters (``K``, ``M``, ``G``, …) are treated as binary (IEC).

    Raises ``ConfigError`` on unrecognised input.
    """
    import re
    if isinstance(value, int):
        return value
    s = str(value).strip()
    try:
        return int(s)
    except ValueError:
        pass
    m = re.match(r"^([0-9]+(?:\.[0-9]+)?)\s*([a-zA-Z]+)$", s)
    if not m:
        raise ConfigError(
            "Cannot parse file size {!r} — expected an integer or a value "
            "like '1GiB', '512MiB', '1 GB'.".format(value)
        )
    number_str, unit_str = m.group(1), m.group(2).upper()
    if unit_str not in _SIZE_UNITS:
        raise ConfigError(
            "Unknown size unit {!r} in {!r} — supported units: {}".format(
                unit_str, value, ", ".join(sorted(_SIZE_UNITS))
            )
        )
    return int(float(number_str) * _SIZE_UNITS[unit_str])

# Mapping from Config attribute name to the environment variable that
# supplies it.  Shell variables and .env file values share this namespace.
# Precedence (highest first): CLI arg > env var > YAML value > default.
_ENV_VAR_MAP = {
    "rucio_host":      "RUCIO_HOST",
    "rucio_auth_host": "RUCIO_AUTH_HOST",
    "rucio_account":   "RUCIO_ACCOUNT",
    "token_endpoint":  "OIDC_TOKEN_ENDPOINT",
    "client_id":       "OIDC_CLIENT_ID",
    "client_secret":   "OIDC_CLIENT_SECRET",
}


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
        "rse_uid": None,         # None means keep the process's own uid
        "rse_gid": None,         # None means keep the process's own gid
        "staging_dir": None,     # None → system temp dir (tempfile.gettempdir())
        "rse_pfn_prefix": None,  # None → use Rucio PFN path as-is
        "dataset_name": None,    # None → dynamic {prefix}_{date}_{run_id}
        "container_name": None,  # None → no container attachment
        "registry_file": None,   # None → DEFAULT_REGISTRY_FILE; "" → disabled
        "generation_mode": "csprng",        # FileWriter back-end; see writers.py
        "buffer_reuse_ring_size": "512MiB", # ring buffer size for buffer-reuse mode
        "xattr": True,                      # write XrdCks adler32 xattr after placement
        "size_label": "iec",                # unit system for {{ file_size }} template: "iec" or "si"
        "pool_chunksize": 0,                # imap chunksize; 0 = auto (N // threads*4)
    }

    def __init__(
        self,
        scope,           # type: str            Rucio scope for all DIDs
        rse,             # type: str            Target RSE name
        rse_mount,       # type: str            POSIX mount path of RSE storage on this host
        dataset_prefix,  # type: Optional[str]  Dataset DID name prefix (unused when dataset_name set)
        file_prefix,     # type: str            File LFN prefix (before checksum suffix)
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
        rse_uid=None,        # type: Optional[int]  uid for placed files/dirs; None = keep process uid
        rse_gid=None,        # type: Optional[int]  gid for placed files/dirs; None = keep process gid
        staging_dir=None,    # type: Optional[str]  staging area for file creation; None = system temp
        rse_pfn_prefix=None, # type: Optional[str]  PFN prefix to replace with rse_mount; None = no translation
        dataset_name=None,   # type: Optional[str]  fixed dataset name; None = dynamic {prefix}_{date}_{run_id}
        container_name=None,    # type: Optional[str]  container DID name; None = no container attachment
        registry_file=None,     # type: Optional[str]  registry path; None = default; "" = disabled
        generation_mode="csprng",          # type: str  FileWriter back-end key; see writers.py
        buffer_reuse_ring_size="512MiB",   # type: object  Ring size for buffer-reuse mode
        xattr=True,                        # type: bool  Write XrdCks adler32 xattr after placement
        size_label="iec",                  # type: str   Unit system for {{ file_size }}: "iec" or "si"
        pool_chunksize=0,                  # type: int   imap chunksize; 0 = auto
    ):
        self.scope = scope
        self.rse = rse
        self.rse_mount = rse_mount
        self.dataset_prefix = dataset_prefix or ""
        self._dataset_name_override = dataset_name or None
        # Store raw (unrendered) templates; properties expose rendered values.
        self._file_prefix_raw = file_prefix
        self._file_prefix_rendered = None   # lazy cache; see file_prefix property
        self.num_files = int(num_files)
        self.file_size_bytes = _parse_size(file_size_bytes)
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
        self.rse_uid = int(rse_uid) if rse_uid is not None else None
        self.rse_gid = int(rse_gid) if rse_gid is not None else None
        self.staging_dir = staging_dir or None
        self.rse_pfn_prefix = rse_pfn_prefix or None
        self._container_name_raw = container_name or None
        self._state_file = state_file
        # registry_file: None means use default path; "" means disabled
        self.registry_file = registry_file if registry_file is not None else None
        self.generation_mode = generation_mode or "csprng"
        self.buffer_reuse_ring_size = _parse_size(
            buffer_reuse_ring_size if buffer_reuse_ring_size is not None else "512MiB"
        )
        self.xattr = bool(xattr) if xattr is not None else True
        self.size_label = (size_label or "iec").lower()
        self.pool_chunksize = int(pool_chunksize) if pool_chunksize is not None else 0

    # ------------------------------------------------------------------
    # Computed properties
    # ------------------------------------------------------------------

    @property
    def file_prefix(self):
        # type: () -> str
        """
        Rendered file LFN prefix.

        The raw value from config is treated as a Jinja2 template.  The
        rendered result is cached on first access so all files within a run
        share an identical prefix regardless of when they are generated.

        Call ``invalidate_name_cache()`` after updating ``run_id`` (e.g. on
        resume) to force a re-render with the correct run identifier before
        any threads are spawned.
        """
        if self._file_prefix_rendered is None:
            self._file_prefix_rendered = self._render(self._file_prefix_raw)
        return self._file_prefix_rendered

    @property
    def dataset_name(self):
        # type: () -> str
        """
        Dataset DID *name* component (without scope prefix).

        If ``dataset_name`` was supplied explicitly it is treated as a Jinja2
        template — e.g. ``{{ dataset_prefix }}_{{ date }}_stress`` — and
        rendered against the current context.  This allows a fixed, persistent
        dataset whose name still encodes useful metadata.

        Without an explicit override the name is generated dynamically:
        ``{dataset_prefix}_{YYYYMMDD}_{run_id}``
        (same as the Jinja2 default ``{{ dataset_prefix }}_{{ date }}_{{ run_id }}``).
        """
        if self._dataset_name_override:
            return self._render(self._dataset_name_override)
        date_str = datetime.now(timezone.utc).strftime("%Y%m%d")
        return "{}_{}_{}".format(self.dataset_prefix, date_str, self.run_id)

    @property
    def container_name(self):
        # type: () -> Optional[str]
        """Rendered container name, or ``None`` if not configured."""
        return self._render(self._container_name_raw) if self._container_name_raw else None

    @property
    def dataset_did(self):
        # type: () -> str
        """Full dataset DID: ``{scope}:{dataset_name}``."""
        return "{}:{}".format(self.scope, self.dataset_name)

    @property
    def container_did(self):
        # type: () -> Optional[str]
        """Full container DID ``{scope}:{container_name}``, or ``None`` if not configured."""
        if not self.container_name:
            return None
        return "{}:{}".format(self.scope, self.container_name)

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
    # Jinja2 template rendering
    # ------------------------------------------------------------------

    def _template_context(self):
        # type: () -> Dict[str, object]
        """
        Build the Jinja2 template context from the current config state.

        Available variables
        -------------------
        ``date``
            UTC date string, ``YYYYMMDD`` (e.g. ``20260321``).
        ``datetime``
            UTC datetime string, ``YYYYMMDD_HHMMSS`` (e.g. ``20260321_090000``).
        ``timestamp``
            UTC Unix timestamp as an integer.
        ``run_id``
            12-character hex run identifier.
        ``scope``
            Rucio scope.
        ``rse``
            Target RSE name.
        ``num_files``
            Number of files to generate (integer).
        ``file_size``
            Human-readable file size using the unit system selected by
            ``size_label`` (default ``iec``).  E.g. ``1GiB`` (IEC) or
            ``1GB`` (SI).
        ``file_size_iec``
            Human-readable file size always in IEC binary units
            (KiB / MiB / GiB / TiB / PiB).  Available regardless of
            the ``size_label`` setting.
        ``file_size_si``
            Human-readable file size always in SI decimal units
            (KB / MB / GB / TB / PB).  Available regardless of the
            ``size_label`` setting.
        ``file_size_bytes``
            Raw file size in bytes (integer).
        ``dataset_prefix``
            Dataset prefix string (useful in ``dataset_name`` templates).
        """
        now = datetime.now(timezone.utc)
        _iec = _human_size(self.file_size_bytes, si=False)
        _si  = _human_size(self.file_size_bytes, si=True)
        return {
            "date":            now.strftime("%Y%m%d"),
            "datetime":        now.strftime("%Y%m%d_%H%M%S"),
            "timestamp":       int(now.timestamp()),
            "run_id":          self.run_id,
            "scope":           self.scope,
            "rse":             self.rse,
            "num_files":       self.num_files,
            "file_size":       _si if self.size_label == "si" else _iec,
            "file_size_iec":   _iec,
            "file_size_si":    _si,
            "file_size_bytes": self.file_size_bytes,
            "dataset_prefix":  self.dataset_prefix,
        }

    def _render(self, template_str):
        # type: (str) -> str
        """
        Render *template_str* as a Jinja2 template against the current context.

        Strings that contain no ``{{`` are returned unchanged without
        importing Jinja2, so there is no overhead for plain values.

        Raises ``ConfigError`` on template syntax or rendering errors.
        """
        if not template_str or "{{" not in template_str:
            return template_str
        try:
            return Template(template_str).render(**self._template_context())
        except TemplateError as exc:
            raise ConfigError(
                "Template rendering failed for {!r}: {}".format(template_str, exc)
            )

    def invalidate_name_cache(self):
        # type: () -> None
        """
        Invalidate the cached rendered ``file_prefix``.

        Must be called after ``run_id`` is updated (e.g. on resume from a
        state file) and before any threads are spawned, so that the cached
        prefix is re-rendered with the correct ``run_id``.
        """
        self._file_prefix_rendered = None

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
            """Resolve a single key: CLI arg → env var → YAML → default."""
            cli_val = getattr(args, key, None)
            if cli_val is not None:
                return cli_val
            env_var = _ENV_VAR_MAP.get(key)
            if env_var:
                env_val = os.environ.get(env_var)
                if env_val is not None:
                    return env_val
            if key in yaml_cfg and yaml_cfg[key] is not None:
                return yaml_cfg[key]
            if key in cls._DEFAULTS:
                return cls._DEFAULTS[key]
            if required:
                raise ConfigError(
                    "Required setting '{}' not found in CLI args, "
                    "environment variables ({}), or YAML config".format(
                        key, _ENV_VAR_MAP.get(key, "n/a")
                    )
                )
            return None

        dataset_name_override = get("dataset_name")
        return cls(
            scope=get("scope", required=True),
            rse=get("rse", required=True),
            rse_mount=get("rse_mount", required=True),
            dataset_prefix=get("dataset_prefix", required=(dataset_name_override is None)),
            file_prefix=get("file_prefix", required=True),
            num_files=int(get("num_files", required=True)),
            file_size_bytes=get("file_size_bytes", required=True),
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
            rse_uid=get("rse_uid"),
            rse_gid=get("rse_gid"),
            staging_dir=get("staging_dir"),
            rse_pfn_prefix=get("rse_pfn_prefix"),
            dataset_name=dataset_name_override,
            container_name=get("container_name"),
            registry_file=get("registry_file"),
            generation_mode=get("generation_mode"),
            buffer_reuse_ring_size=get("buffer_reuse_ring_size"),
            xattr=get("xattr"),
            size_label=get("size_label"),
            pool_chunksize=get("pool_chunksize"),
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
        if self.size_label not in ("iec", "si"):
            raise ConfigError(
                "size_label must be 'iec' or 'si', got {!r}".format(self.size_label)
            )
        if self.pool_chunksize < 0:
            raise ConfigError(
                "pool_chunksize must be >= 0, got {}".format(self.pool_chunksize)
            )
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
        if self.generation_mode == "buffer-reuse":
            _min_ring = 128 * 1024 * 1024  # must match CHUNK_SIZE in writers.py
            if self.buffer_reuse_ring_size < _min_ring:
                raise ConfigError(
                    "buffer_reuse_ring_size ({} bytes) must be >= 128 MiB ({} bytes) "
                    "when using buffer-reuse mode".format(
                        self.buffer_reuse_ring_size, _min_ring
                    )
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
