"""
__main__.py — CLI entry point for claude-dataset-generator.

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
import logging
import os
import sys

from .config import Config, ConfigError
from .generator import run_generation
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
    ovr.add_argument("--rse-mount", dest="rse_mount", help="POSIX mount path of RSE storage.")
    ovr.add_argument("--dataset-prefix", dest="dataset_prefix", help="Dataset DID name prefix.")
    ovr.add_argument("--file-prefix", dest="file_prefix", help="File LFN prefix.")
    ovr.add_argument("--num-files", dest="num_files", type=int, help="Number of files to generate.")
    ovr.add_argument("--file-size-bytes", dest="file_size_bytes", type=int, help="File size in bytes.")
    ovr.add_argument("--rucio-host", dest="rucio_host", help="Rucio server URL.")
    ovr.add_argument("--rucio-auth-host", dest="rucio_auth_host", help="Rucio auth server URL.")
    ovr.add_argument("--rucio-account", dest="rucio_account", help="Rucio account name.")
    ovr.add_argument("--token-endpoint", dest="token_endpoint", help="OIDC token endpoint URL.")
    ovr.add_argument("--client-id", dest="client_id", help="OIDC client ID.")
    ovr.add_argument("--client-secret", dest="client_secret", help="OIDC client secret.")
    ovr.add_argument(
        "--rule-lifetime", dest="rule_lifetime", type=int, metavar="SECONDS",
        help=(
            "Replication rule lifetime in seconds. "
            "Omit (or set to null in YAML) for a permanent rule."
        ),
    )

    return parser


# ---------------------------------------------------------------------------
# Pipeline stages
# ---------------------------------------------------------------------------

def _run_full_pipeline(config, state, rucio_manager):
    # type: (Config, StateFile, RucioManager) -> int
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

    # Step 2: Generate files (skips already-created files on resume).
    generated = run_generation(config, state, rucio_manager)

    if not generated:
        log.warning("No files were generated — nothing to register")
        return 0

    # Step 3: Register replicas in a single batch call.
    failures = _register_replicas(config, state, rucio_manager, generated)

    # Step 4: Attach all successfully registered file DIDs to the dataset.
    registered_entries = state.get_files_by_status(FileStatus.REGISTERED, FileStatus.RULED)
    if registered_entries:
        file_dids = [
            {"scope": config.scope, "name": e["lfn_name"]}
            for e in registered_entries
        ]
        rucio_manager.attach_dids(config.scope, config.dataset_name, file_dids)

    # Step 5: Create the replication rule on the dataset DID.
    if registered_entries:
        rule_id = rucio_manager.add_replication_rule(
            scope=config.scope,
            dataset_name=config.dataset_name,
            rse=config.rse,
            lifetime=config.rule_lifetime,
        )
        # Mark all registered files as ruled.
        for entry in registered_entries:
            if entry.get("status") == FileStatus.REGISTERED:
                state.update(entry["key"], status=FileStatus.RULED, rule_id=rule_id)
        log.info("Pipeline complete: rule_id=%s", rule_id)

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


def _run_register_only(config, state, rucio_manager):
    # type: (Config, StateFile, RucioManager) -> int
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
        rule_id = rucio_manager.add_replication_rule(
            config.scope, config.dataset_name, config.rse,
            lifetime=config.rule_lifetime,
        )
        for entry in registered_entries:
            state.update(entry["key"], status=FileStatus.RULED, rule_id=rule_id)

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
    # Exclude entries already beyond CREATED (e.g. resumed REGISTERED/RULED)
    to_register = [
        e for e in file_entries
        if e.get("status") in (FileStatus.CREATED, FileStatus.FAILED_REGISTRATION, None)
        or "status" not in e  # freshly generated (status not in dict yet)
    ]
    if not to_register:
        return 0

    replica_batch = []
    for entry in to_register:
        rec = {
            "scope": config.scope,
            "name": entry["lfn_name"],
            "bytes": entry["bytes"],
            "adler32": entry["adler32"],
        }
        if entry.get("pfn"):
            rec["pfn"] = entry["pfn"]
        replica_batch.append(rec)

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

    try:
        config.validate()
    except ConfigError as exc:
        log.error("Invalid configuration: %s", exc)
        sys.exit(2)

    log.info("Starting dataset-generator run_id=%s dry_run=%s", config.run_id, config.dry_run)
    log.debug("Config: %r", config)

    # ------------------------------------------------------------------
    # Load or create state file.
    # ------------------------------------------------------------------
    try:
        state = StateFile(path=config.state_file_path, run_id=config.run_id)
    except Exception as exc:
        log.error("State file error: %s", exc)
        sys.exit(2)

    # If the state file contains a different run_id (legitimate resume), use it.
    if state.run_id != config.run_id:
        log.info("Resuming run_id=%s from state file", state.run_id)
        config.run_id = state.run_id

    # ------------------------------------------------------------------
    # Initialise Rucio manager.
    # ------------------------------------------------------------------
    rucio_manager = RucioManager(config=config, dry_run=config.dry_run)

    # ------------------------------------------------------------------
    # Dispatch to the requested mode.
    # ------------------------------------------------------------------
    try:
        if config.cleanup:
            failures = _run_cleanup(config, state, rucio_manager)
        elif config.create_only:
            failures = _run_create_only(config, state, rucio_manager)
        elif config.register_only:
            failures = _run_register_only(config, state, rucio_manager)
        else:
            failures = _run_full_pipeline(config, state, rucio_manager)
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
