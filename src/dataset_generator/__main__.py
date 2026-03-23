"""
__main__.py — CLI entry point for rucio-ds-generator.

Usage
-----
.. code-block:: bash

    dataset-generator --help
    dataset-generator --config config.yaml
    dataset-generator --config config.yaml --dry-run
    dataset-generator --config config.yaml --register-only --state-file state_abc123.json
    dataset-generator --config config.yaml --cleanup --state-file state_abc123.json

Configuration precedence (highest first)
-----------------------------------------
1. CLI flags
2. YAML config file (``--config``)
3. Built-in defaults (see ``config.py``)

Operational modes
-----------------
full (default)
    Generate files → register replicas → attach to dataset → create rule.
--create-only
    Generate and place files; stop before any Rucio registration.
--register-only
    Skip file generation; register files already present in state file.
--cleanup
    Remove placed files from the RSE filesystem and delete their replica
    records from Rucio.  Acts on ``failed_*`` and ``created`` (unregistered)
    entries in the state file.

Resume behaviour
----------------
Pass ``--state-file state_{run_id}.json`` to resume an interrupted run.
The run_id is read from the state file and used to reconstruct the dataset
DID name.  Files already in ``CREATED``, ``REGISTERED``, or ``RULED`` state
are skipped.  Files in ``FAILED_*`` or ``PENDING`` state are retried.

Exit codes
----------
0   All files successfully processed (or dry-run completed).
1   One or more files failed; details in the log.
2   Configuration or environment error (bad YAML, missing RSE mount, etc.).
"""

import argparse
import atexit
import logging
import os
import shutil
import sys
import tempfile
from typing import Optional

from dotenv import load_dotenv

from .config import Config, ConfigError
from .generator import run_generation
from .registry import DatasetRegistry, DEFAULT_REGISTRY_FILE
from .rucio_client import RucioManager
from .state import FileStatus, StateFile

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CLI argument parser
# ---------------------------------------------------------------------------

def _build_parser():
    # type: () -> argparse.ArgumentParser
    """Return the argument parser for the ``dataset-generator`` command."""
    parser = argparse.ArgumentParser(
        prog="dataset-generator",
        description=(
            "Generate dummy test datasets and register them into Rucio. "
            "The RSE storage must be POSIX-mounted on this host."
        ),
    )

    parser.add_argument(
        "--config", metavar="PATH",
        help="Path to YAML configuration file.",
    )
    parser.add_argument(
        "--dry-run", action="store_true", default=None,
        dest="dry_run",
        help="Log all actions without executing them.",
    )
    parser.add_argument(
        "--threads", type=int, metavar="N",
        help="Number of file-generation threads (Pool A). Default: 4.",
    )
    parser.add_argument(
        "--log-level", metavar="LEVEL",
        dest="log_level",
        help="Python logging level (DEBUG, INFO, WARNING, ERROR). Default: INFO.",
    )
    parser.add_argument(
        "--state-file", metavar="PATH",
        dest="state_file",
        help="Override state file path. Default: ./state_{run_id}.json.",
    )
    parser.add_argument(
        "--run-id", metavar="ID",
        dest="run_id",
        help=(
            "Override the run identifier (12-char hex). "
            "Use to resume a run whose state file you have specified."
        ),
    )

    parser.add_argument(
        "--check-auth", action="store_true", default=False,
        dest="check_auth",
        help=(
            "Test OIDC token acquisition and Rucio connectivity, then exit. "
            "Exits 0 on success, 2 on any failure. "
            "Does not require --config if credentials are supplied via env vars."
        ),
    )

    # Mode flags (mutually exclusive groups checked in Config.validate)
    mode_group = parser.add_argument_group("Operational mode (mutually exclusive)")
    mode_group.add_argument(
        "--create-only", action="store_true", default=None,
        dest="create_only",
        help="Generate and place files; skip Rucio registration.",
    )
    mode_group.add_argument(
        "--register-only", action="store_true", default=None,
        dest="register_only",
        help="Register already-placed files from state file; skip generation.",
    )
    mode_group.add_argument(
        "--cleanup", action="store_true", default=None,
        help=(
            "Remove placed files from the RSE and delete replica records "
            "for failed/orphaned state entries."
        ),
    )

    # Config overrides (all optional; YAML is preferred for these)
    ovr = parser.add_argument_group("Config overrides (override YAML values)")
    ovr.add_argument("--scope", help="Rucio scope.")
    ovr.add_argument("--rse", help="Target RSE name.")
    ovr.add_argument(
        "--dataset-name", dest="dataset_name", metavar="NAME",
        help=(
            "Fixed dataset DID name (without scope). When set, the same dataset "
            "is reused across runs (files are appended). "
            "Default: dynamic {dataset_prefix}_{date}_{run_id}."
        ),
    )
    ovr.add_argument("--rse-mount", dest="rse_mount", help="POSIX mount path of RSE storage.")
    ovr.add_argument("--dataset-prefix", dest="dataset_prefix", help="Dataset DID name prefix.")
    ovr.add_argument("--file-prefix", dest="file_prefix", help="File LFN prefix.")
    ovr.add_argument("--num-files", dest="num_files", type=int, help="Number of files to generate.")
    ovr.add_argument("--file-size-bytes", dest="file_size_bytes",
                     help="File size — integer bytes or human-readable: 1GiB, 512MiB, 1GB.")
    ovr.add_argument("--rucio-host", dest="rucio_host", help="Rucio server URL.")
    ovr.add_argument("--rucio-auth-host", dest="rucio_auth_host", help="Rucio auth server URL.")
    ovr.add_argument("--rucio-account", dest="rucio_account", help="Rucio account name.")
    ovr.add_argument("--token-endpoint", dest="token_endpoint", help="OIDC token endpoint URL.")
    ovr.add_argument("--client-id", dest="client_id", help="OIDC client ID.")
    ovr.add_argument("--client-secret", dest="client_secret", help="OIDC client secret.")
    ovr.add_argument(
        "--staging-dir", dest="staging_dir", metavar="PATH",
        help=(
            "Directory for temporary file creation before placement on the RSE. "
            "May be on a different filesystem from rse_mount. "
            "Default: system temp dir (tempfile.gettempdir())."
        ),
    )
    ovr.add_argument(
        "--rse-pfn-prefix", dest="rse_pfn_prefix", metavar="PREFIX",
        help=(
            "Path prefix that Rucio uses for this RSE (e.g. /data/rucio). "
            "Stripped from Rucio PFNs and replaced with rse_mount to give "
            "the local filesystem path. Omit if Rucio already returns local paths."
        ),
    )
    ovr.add_argument(
        "--rse-uid", dest="rse_uid", type=int, metavar="UID",
        help=(
            "User ID to assign to placed files and any newly created "
            "RSE directories. Omit to keep the process's own uid."
        ),
    )
    ovr.add_argument(
        "--rse-gid", dest="rse_gid", type=int, metavar="GID",
        help=(
            "Group ID to assign to placed files and any newly created "
            "RSE directories. Omit to keep the process's own gid."
        ),
    )
    ovr.add_argument(
        "--rule-lifetime", dest="rule_lifetime", type=int, metavar="SECONDS",
        help=(
            "Replication rule lifetime in seconds. "
            "Omit (or set to null in YAML) for a permanent rule."
        ),
    )
    ovr.add_argument(
        "--container-name", dest="container_name", metavar="NAME",
        help=(
            "Name of a Rucio container DID to attach the dataset to after "
            "each successful run. The container is created if it does not "
            "exist; duplicate attachment is silently ignored. "
            "Omit (or leave unset in YAML) to skip container attachment."
        ),
    )
    ovr.add_argument(
        "--generation-mode", dest="generation_mode", metavar="MODE",
        help=(
            "File-generation back-end. Default: csprng. "
            "Available: csprng (streaming pseudo-random), "
            "buffer-reuse (pre-filled ring buffer; disk/memory-bandwidth-limited)."
        ),
    )
    ovr.add_argument(
        "--buffer-reuse-ring-size", dest="buffer_reuse_ring_size", metavar="SIZE",
        help=(
            "Ring buffer size for buffer-reuse mode. "
            "Accepts an integer (bytes) or human-readable string: 512MiB, 1GiB, 2GB. "
            "Must be >= 128 MiB (CHUNK_SIZE). Default: 512MiB."
        ),
    )
    ovr.add_argument(
        "--registry-file", dest="registry_file", metavar="PATH",
        help=(
            "Path to the global dataset registry JSON file. "
            "Default: {}. "
            "Set to empty string to disable registry recording.".format(DEFAULT_REGISTRY_FILE)
        ),
    )
    ovr.add_argument(
        "--size-label", dest="size_label", choices=["iec", "si"], metavar="SYSTEM",
        help=(
            "Unit system for the {{ file_size }} template variable used in dataset "
            "and file prefix names. "
            "'iec' (default): binary powers of 1024 — KiB, MiB, GiB, TiB. "
            "'si': decimal powers of 1000 — KB, MB, GB, TB. "
            "Both {{ file_size_iec }} and {{ file_size_si }} are always available "
            "in templates regardless of this setting."
        ),
    )
    ovr.add_argument(
        "--pool-chunksize", dest="pool_chunksize", type=int, metavar="N",
        help=(
            "Number of file-generation tasks sent to each worker per IPC round-trip. "
            "0 (default): auto-computed as max(1, num_files // (threads * 4)). "
            "Increase for large file counts (>10k) to reduce IPC overhead."
        ),
    )
    ovr.add_argument(
        "--state-flush-interval", dest="state_flush_interval", type=int, metavar="N",
        help=(
            "Write the state file to disk every N update() calls (default: 100). "
            "1 flushes on every update (safe but slow at 100k files). "
            "Higher values reduce I/O at the cost of losing more progress on crash."
        ),
    )
    ovr.add_argument(
        "--pfn-batch-size", dest="pfn_batch_size", type=int, metavar="N",
        help=(
            "Number of LFNs resolved per lfns2pfns Rucio API call. "
            "0: resolve all at once (one call total). "
            "Default: 1000. Larger values reduce HTTP round-trips but "
            "increase memory pressure if the Rucio server is slow."
        ),
    )
    ovr.add_argument(
        "--no-xattr", dest="xattr", action="store_false", default=None,
        help=(
            "Disable writing the XrdCks adler32 extended attribute "
            "(user.XrdCks.adler32) to placed files. "
            "Default: xattr writing is enabled when supported by the OS."
        ),
    )

    return parser


# ---------------------------------------------------------------------------
# Registry helper
# ---------------------------------------------------------------------------

def _open_registry(config):
    # type: (Config) -> Optional[DatasetRegistry]
    """
    Return an open ``DatasetRegistry``, or ``None`` if registry recording is
    disabled (``registry_file`` set to empty string or ``"null"``).
    """
    rf = config.registry_file
    if rf is None:
        rf = DEFAULT_REGISTRY_FILE
    elif rf.strip().lower() in ("", "null", "none"):
        log.debug("Registry recording disabled")
        return None
    return DatasetRegistry(rf)


def _update_registry(config, registry, rule_id, num_files):
    # type: (Config, Optional[DatasetRegistry], object, int) -> None
    """Record the dataset in the global registry if recording is enabled."""
    if registry is None:
        return
    try:
        registry.record(
            dataset_did=config.dataset_did,
            rse=config.rse,
            rule_id=rule_id,
            num_files=num_files,
        )
        log.debug("Registry updated for %s", config.dataset_did)
    except Exception as exc:
        log.warning("Failed to update registry: %s", exc)


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------

def _attach_to_container(config, rucio_manager):
    # type: (Config, RucioManager) -> None
    """
    Ensure the container exists and attach the current dataset to it.

    No-op when ``config.container_name`` is not set.  Duplicate attachment
    is silently ignored by ``RucioManager.attach_dataset_to_container``.
    """
    if not config.container_name:
        return
    try:
        rucio_manager.add_container(config.scope, config.container_name)
        rucio_manager.attach_dataset_to_container(
            config.scope, config.container_name, config.dataset_name,
        )
        log.info(
            "Dataset %s attached to container %s",
            config.dataset_did, config.container_did,
        )
    except Exception as exc:
        log.warning("Container attachment failed: %s", exc)


def _run_full_pipeline(config, state, rucio_manager, registry=None):
    # type: (Config, StateFile, RucioManager, Optional[DatasetRegistry]) -> int
    """
    Execute the complete pipeline:

    1. Create the dataset DID.
    2. Generate files (Pool A).
    3. Register replicas in Rucio (batch ``add_replicas``).
    4. Attach file DIDs to the dataset.
    5. Create a replication rule on the dataset DID.

    Returns the number of files that failed at any stage.
    """
    # Step 1: Ensure the dataset DID exists before generation starts so that
    # any resume can safely re-call this (Rucio ignores DataIdentifierAlreadyExists).
    rucio_manager.add_dataset(config.scope, config.dataset_name)

    # Step 2: Determine how many new files to generate.
    #
    # num_files is the *target total* for the dataset — not necessarily the
    # number to generate this run.  Subtract files already in Rucio (from
    # previous runs) and files already tracked in the state file (from this
    # run: generated but not yet registered) to get the remaining shortfall.
    existing_in_rucio = rucio_manager.count_dataset_files(
        config.scope, config.dataset_name
    )
    current_state_count = state.count()
    new_files_needed = max(0, config.num_files - existing_in_rucio - current_state_count)

    log.info(
        "Dataset target: %d file(s) | in Rucio: %d | in state: %d | to generate: %d",
        config.num_files, existing_in_rucio, current_state_count, new_files_needed,
    )

    # If the dataset is already at (or above) the target and there is nothing
    # in the local state file to finish registering, just ensure the rule exists.
    if new_files_needed == 0 and current_state_count == 0:
        log.info(
            "Dataset %s already has %d/%d file(s) — skipping generation",
            config.dataset_did, existing_in_rucio, config.num_files,
        )
        rule_id = rucio_manager.add_replication_rule(
            scope=config.scope,
            dataset_name=config.dataset_name,
            rse=config.rse,
            lifetime=config.rule_lifetime,
        )
        _update_registry(config, registry, rule_id, 0)
        _attach_to_container(config, rucio_manager)
        return 0

    # Step 3: Generate files (skips already-created files on resume).
    generated = run_generation(config, state, rucio_manager, new_count=new_files_needed)

    if not generated:
        log.warning("No files were generated — nothing to register")
        return 0

    # Step 4: Register replicas in a single batch call.
    failures = _register_replicas(config, state, rucio_manager, generated)

    # Step 5: Attach all successfully registered file DIDs to the dataset.
    # Fetch both REGISTERED and RULED so the rule-creation step below can
    # mark the REGISTERED ones as RULED.  Only REGISTERED files are passed to
    # attach_dids — RULED files are already attached; including them would cause
    # Rucio to raise DuplicateContent on any resumed run.
    registered_entries = state.get_files_by_status(FileStatus.REGISTERED, FileStatus.RULED)
    newly_registered = [e for e in registered_entries
                        if e.get("status") == FileStatus.REGISTERED]
    if newly_registered:
        file_dids = [
            {"scope": config.scope, "name": e["lfn_name"]}
            for e in newly_registered
        ]
        rucio_manager.attach_dids(config.scope, config.dataset_name, file_dids)

    # Step 6: Create the replication rule on the dataset DID.
    if registered_entries:
        try:
            rule_id = rucio_manager.add_replication_rule(
                scope=config.scope,
                dataset_name=config.dataset_name,
                rse=config.rse,
                lifetime=config.rule_lifetime,
            )
        except Exception as exc:
            log.error("add_replication_rule failed: %s", exc)
            # Count the rule failure once, then mark all REGISTERED files as
            # FAILED_RULE so they can be retried.  RULED files are left as-is
            # (they already have a rule from a prior run).
            failures += 1
            for entry in registered_entries:
                if entry.get("status") == FileStatus.REGISTERED:
                    state.update(entry["key"], status=FileStatus.FAILED_RULE)
            return failures
        # Mark all newly-registered files as ruled.
        for entry in newly_registered:
            state.update(entry["key"], status=FileStatus.RULED, rule_id=rule_id)
        log.info("Pipeline complete: rule_id=%s", rule_id)
        _update_registry(config, registry, rule_id, len(registered_entries))
        _attach_to_container(config, rucio_manager)

    return failures


def _run_create_only(config, state, rucio_manager):
    # type: (Config, StateFile, RucioManager) -> int
    """Generate and place files; skip Rucio registration."""
    generated = run_generation(config, state, rucio_manager)
    failures = sum(
        1 for e in state.get_files_by_status(FileStatus.FAILED_CREATION)
    )
    log.info("Create-only complete: %d placed, %d failed", len(generated), failures)
    return failures


def _run_register_only(config, state, rucio_manager, registry=None):
    # type: (Config, StateFile, RucioManager, Optional[DatasetRegistry]) -> int
    """Register files already present in the state file; skip generation."""
    rucio_manager.add_dataset(config.scope, config.dataset_name)

    created_entries = state.get_files_by_status(FileStatus.CREATED)
    if not created_entries:
        log.info("No files in CREATED state — nothing to register")
        return 0

    failures = _register_replicas(config, state, rucio_manager, created_entries)

    registered_entries = state.get_files_by_status(FileStatus.REGISTERED)
    if registered_entries:
        file_dids = [
            {"scope": config.scope, "name": e["lfn_name"]}
            for e in registered_entries
        ]
        rucio_manager.attach_dids(config.scope, config.dataset_name, file_dids)
        try:
            rule_id = rucio_manager.add_replication_rule(
                config.scope, config.dataset_name, config.rse,
                lifetime=config.rule_lifetime,
            )
        except Exception as exc:
            log.error("add_replication_rule failed: %s", exc)
            for entry in registered_entries:
                state.update(entry["key"], status=FileStatus.FAILED_RULE)
            return failures + len(registered_entries)
        for entry in registered_entries:
            state.update(entry["key"], status=FileStatus.RULED, rule_id=rule_id)
        _update_registry(config, registry, rule_id, len(registered_entries))
        _attach_to_container(config, rucio_manager)

    return failures


def _run_cleanup(config, state, rucio_manager):
    # type: (Config, StateFile, RucioManager) -> int
    """
    Remove placed files and delete their Rucio replica records.

    Acts on entries in ``FAILED_CREATION``, ``FAILED_REGISTRATION``,
    ``FAILED_RULE``, and ``CREATED`` (placed but unregistered) states.
    ``RULED`` and ``REGISTERED`` entries are skipped — they are considered
    production data and must be handled via Rucio rules, not by this tool.
    """
    cleanup_statuses = (
        FileStatus.FAILED_CREATION,
        FileStatus.FAILED_REGISTRATION,
        FileStatus.FAILED_RULE,
        FileStatus.CREATED,
    )
    entries = state.get_files_by_status(*cleanup_statuses)

    if not entries:
        log.info("No failed/orphaned entries to clean up")
        return 0

    log.info("Cleaning up %d state entry/entries", len(entries))
    failures = 0

    # Remove physical files from the RSE mount.
    for entry in entries:
        pfn = entry.get("pfn")
        if pfn and os.path.exists(pfn):
            if config.dry_run:
                log.info("[DRY-RUN] Would remove: %s", pfn)
            else:
                try:
                    os.unlink(pfn)
                    log.info("Removed: %s", pfn)
                except OSError as exc:
                    log.error("Failed to remove %s: %s", pfn, exc)
                    failures += 1

    # Delete Rucio replica records only for entries that made it as far as
    # registration.  FAILED_CREATION and CREATED entries never had a replica
    # record written to Rucio, so delete_replicas is not applicable to them.
    registered_entries = [
        e for e in entries
        if e.get("status") in (FileStatus.FAILED_REGISTRATION, FileStatus.FAILED_RULE)
        and e.get("lfn_name")
    ]
    if registered_entries:
        replica_files = [
            {"scope": config.scope, "name": e["lfn_name"]}
            for e in registered_entries
        ]
        try:
            rucio_manager.delete_replicas(config.rse, replica_files)
        except Exception as exc:
            log.error("delete_replicas failed: %s", exc)
            failures += 1

    return failures


def _register_replicas(config, state, rucio_manager, file_entries):
    # type: (Config, StateFile, RucioManager, list) -> int
    """
    Batch-register *file_entries* as Rucio replicas.

    Builds the ``files`` list for ``RucioManager.add_replicas`` from the
    supplied entry dicts.  On success updates each entry's state to
    ``REGISTERED``.  On failure updates to ``FAILED_REGISTRATION``.

    Parameters
    ----------
    file_entries:
        List of file metadata dicts (each must have ``key``, ``lfn_name``,
        ``bytes``, ``adler32``, and optionally ``pfn``).

    Returns
    -------
    int
        Number of entries that could not be registered.
    """
    # Exclude entries already beyond CREATED (e.g. resumed REGISTERED/RULED).
    # e.get("status") returns None for entries without a status key, and None
    # is in the tuple, so the condition handles both cases correctly.
    to_register = [
        e for e in file_entries
        if e.get("status") in (FileStatus.CREATED, FileStatus.FAILED_REGISTRATION, None)
    ]
    if not to_register:
        return 0

    # For deterministic RSEs Rucio derives the PFN from the LFN automatically.
    # Passing an explicit pfn (e.g. a local POSIX mount path) would cause
    # "RSE does not support requested protocol" because it would not match
    # the RSE's registered protocol (e.g. davs://).
    replica_batch = [
        {
            "scope": config.scope,
            "name": entry["lfn_name"],
            "bytes": entry["bytes"],
            "adler32": entry["adler32"],
        }
        for entry in to_register
    ]

    try:
        rucio_manager.add_replicas(config.rse, replica_batch)
        for entry in to_register:
            state.update(entry["key"], status=FileStatus.REGISTERED)
        return 0
    except Exception as exc:
        log.error("add_replicas failed for batch of %d: %s", len(to_register), exc)
        for entry in to_register:
            state.update(entry["key"], status=FileStatus.FAILED_REGISTRATION)
        return len(to_register)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    # type: () -> None
    """CLI entry point — called by the ``dataset-generator`` console script."""
    parser = _build_parser()
    args = parser.parse_args()

    # Load .env file if present.  Variables already set in the shell are
    # not overridden (override=False is the default).  Silently ignored
    # when no .env file exists so CI and container environments work
    # without one.
    load_dotenv()

    # ------------------------------------------------------------------
    # Bootstrap logging before Config so errors are visible.
    # ------------------------------------------------------------------
    log_level_str = args.log_level or "INFO"
    logging.basicConfig(
        level=getattr(logging, log_level_str.upper(), logging.INFO),
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )

    # ------------------------------------------------------------------
    # Build and validate config.
    # ------------------------------------------------------------------
    try:
        config = Config.from_yaml_and_args(args.config, args)
    except (ConfigError, IOError, ValueError) as exc:
        log.error("Configuration error: %s", exc)
        sys.exit(2)

    # Apply the resolved log level (may differ from pre-bootstrap level).
    logging.getLogger().setLevel(getattr(logging, config.log_level, logging.INFO))

    # ------------------------------------------------------------------
    # --check-auth: verify credentials and exit without running the pipeline.
    # We skip config.validate() so that a missing/unmounted rse_mount does
    # not block an auth check on a machine that lacks the RSE filesystem.
    # ------------------------------------------------------------------
    if args.check_auth:
        rucio_manager = RucioManager(config=config, dry_run=False)
        try:
            info = rucio_manager.check_auth()
            print("Auth OK")
            print("  account : {}".format(info.get("account", "?")))
            print("  status  : {}".format(info.get("status", "?")))
            print("  type    : {}".format(info.get("account_type", info.get("type", "?"))))
        except Exception as exc:
            log.error("Auth check failed: %s", exc)
            sys.exit(2)
        sys.exit(0)

    try:
        config.validate()
    except ConfigError as exc:
        log.error("Invalid configuration: %s", exc)
        sys.exit(2)

    log.info("Starting dataset-generator run_id=%s dry_run=%s", config.run_id, config.dry_run)
    log.info("Dataset DID : %s", config.dataset_did)
    log.info("Dataset name: %s  (%s)",
             config.dataset_name,
             "static" if config._dataset_name_override else "dynamic")
    if config.container_name:
        log.info("Container   : %s", config.container_did)
    log.debug("Config: %r", config)

    # ------------------------------------------------------------------
    # Load or create state file.
    # ------------------------------------------------------------------
    try:
        state = StateFile(
            path=config.state_file_path,
            run_id=config.run_id,
            flush_interval=config.state_flush_interval,
        )
    except Exception as exc:
        log.error("State file error: %s", exc)
        sys.exit(2)

    # If the state file contains a different run_id (legitimate resume), use it.
    if state.run_id != config.run_id:
        log.info("Resuming run_id=%s from state file", state.run_id)
        config.run_id = state.run_id
        # Invalidate the file_prefix cache so it is re-rendered with the
        # correct run_id before worker threads access it.
        config.invalidate_name_cache()

    # Pre-warm the file_prefix cache now (single-threaded context) so all
    # Pool A worker threads see a consistent, already-rendered value.
    _ = config.file_prefix

    # ------------------------------------------------------------------
    # Create a unique per-run staging directory and register cleanup.
    #
    # tempfile.mkdtemp() returns an exclusive, randomly-named directory so
    # parallel invocations never share staging space even on the same host.
    # atexit ensures the directory (and any leftover temp files from an
    # interrupted run) is removed when the process exits normally.
    # ------------------------------------------------------------------
    base_staging = config.staging_dir or tempfile.gettempdir()
    try:
        os.makedirs(base_staging, exist_ok=True)
        run_staging = tempfile.mkdtemp(
            prefix="rucio-gen-{}-".format(config.run_id),
            dir=base_staging,
        )
    except OSError as exc:
        log.error("Cannot create staging directory under %r: %s", base_staging, exc)
        sys.exit(2)

    atexit.register(shutil.rmtree, run_staging, True)   # True = ignore_errors
    config.staging_dir = run_staging
    log.debug("Staging directory: %s", run_staging)

    # ------------------------------------------------------------------
    # Initialise Rucio manager and assert RSE is deterministic.
    # ------------------------------------------------------------------
    rucio_manager = RucioManager(config=config, dry_run=config.dry_run)

    try:
        rucio_manager.assert_rse_deterministic(config.rse)
    except Exception as exc:
        log.error("RSE check failed: %s", exc)
        sys.exit(2)

    # ------------------------------------------------------------------
    # Open registry (disabled when registry_file is empty/null).
    # ------------------------------------------------------------------
    registry = _open_registry(config)
    if registry is not None:
        log.debug("Registry file: %s", config.registry_file or DEFAULT_REGISTRY_FILE)

    # ------------------------------------------------------------------
    # Dispatch to the requested mode.
    # ------------------------------------------------------------------
    try:
        if config.cleanup:
            failures = _run_cleanup(config, state, rucio_manager)
        elif config.create_only:
            failures = _run_create_only(config, state, rucio_manager)
        elif config.register_only:
            failures = _run_register_only(config, state, rucio_manager, registry=registry)
        else:
            failures = _run_full_pipeline(config, state, rucio_manager, registry=registry)
    except KeyboardInterrupt:
        log.warning("Interrupted — state saved to %s", config.state_file_path)
        sys.exit(1)
    except Exception as exc:
        log.exception("Unhandled error: %s", exc)
        sys.exit(1)

    if failures:
        log.warning("Run finished with %d failure(s). State: %s", failures, config.state_file_path)
        sys.exit(1)
    else:
        log.info("Run finished successfully. State: %s", config.state_file_path)
        sys.exit(0)


if __name__ == "__main__":
    main()
