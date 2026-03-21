# claude-dataset-generator

A CLI tool for generating dummy test datasets and registering them into
[Rucio](https://rucio.cern.ch) for file transfer testing.

The tool writes random binary files directly onto a POSIX-mounted RSE storage
volume, registers them in-place (no data movement through Rucio), attaches them
to a dataset DID, and creates a replication rule to retain the data at the
target site.

---

## Table of contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Your first run](#your-first-run)
- [Operational modes](#operational-modes)
- [Resuming an interrupted run](#resuming-an-interrupted-run)
- [Running the test suite](#running-the-test-suite)
- [CLI reference](#cli-reference)
- [Troubleshooting](#troubleshooting)

---

## Prerequisites

| Requirement | Notes |
|-------------|-------|
| Python 3.6+ | 3.8+ recommended |
| POSIX-mounted RSE | The RSE storage directory must be directly accessible as a local or NFS path on the host running this tool. XRootD, WebDAV, S3, and dCache RSEs are **not** supported without `gfal2`. |
| Rucio server access | Network access to your site's Rucio and Rucio-auth endpoints |
| OIDC client credentials | A `client_id` + `client_secret` pair with permission to register replicas and create rules on the target RSE |

---

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/your-org/claude-dataset-generator.git
cd claude-dataset-generator
```

### 2. Create and activate a virtual environment

```bash
python3 -m venv .venv
source .venv/bin/activate        # Linux / macOS
# .venv\Scripts\activate.bat    # Windows (cmd)
# .venv\Scripts\Activate.ps1   # Windows (PowerShell)
```

> **Using uv?**  `uv venv .venv && source .venv/bin/activate`

### 3. Install the package

```bash
# Runtime only
pip install -e .

# Runtime + development tools (pytest, flake8, black, mypy)
pip install -e ".[dev]"
```

Verify the CLI entry point is available:

```bash
dataset-generator --help
```

---

## Configuration

All settings live in a YAML file. Copy the annotated example and edit for your
site:

```bash
cp config.example.yaml my_site.yaml
```

The minimal required fields are:

```yaml
# Rucio identity
scope: cms                                    # Rucio scope for all DIDs
rse: T2_US_Wisconsin                          # Target RSE name
rse_mount: /mnt/hadoop/store/rucio            # POSIX path to RSE storage on this host

# Dataset and file naming
dataset_prefix: transfer_test                 # Final name: {prefix}_{date}_{run_id}
file_prefix: dummy                            # Final name: {prefix}_{adler32hex}

# Generation parameters
num_files: 10
file_size_bytes: 1073741824                   # 1 GiB per file

# OIDC credentials
token_endpoint: https://cms-iam.cern.ch/token
client_id: my-robot-client
client_secret: s3cr3t

# Rucio endpoints
rucio_host: https://cms-rucio.cern.ch
rucio_auth_host: https://cms-rucio-auth.cern.ch
rucio_account: robot_account
```

Optional settings (shown with their defaults):

```yaml
threads: 4                # File-generation threads (Pool A)
log_level: INFO           # DEBUG | INFO | WARNING | ERROR
rule_lifetime: ~          # Rule lifetime in seconds; null = permanent
                          # e.g. 604800 = 7 days, 2592000 = 30 days
```

> **Security note:** `client_secret` is sensitive. Do not commit `my_site.yaml`
> to version control. Use environment-specific files or a secrets manager.

Any YAML value can be overridden on the command line — see
[CLI reference](#cli-reference).

---

## Your first run

### Step 1 — Dry run (always do this first)

A dry run logs every action without writing files or calling Rucio:

```bash
dataset-generator --config my_site.yaml --dry-run
```

Expected output:

```
2026-03-21T09:00:01 INFO  Starting dataset-generator run_id=a1b2c3d4e5f6 dry_run=True
2026-03-21T09:00:01 INFO  [DRY-RUN] add_dataset('cms', 'transfer_test_20260321_a1b2c3d4e5f6')
Generating: 100%|████████████████| 10/10 [00:00<00:00, file]
2026-03-21T09:00:01 INFO  [DRY-RUN] add_replicas('T2_US_Wisconsin', 10 files)
2026-03-21T09:00:01 INFO  [DRY-RUN] attach_dids('cms':'transfer_test_20260321_a1b2c3d4e5f6', 10 files)
2026-03-21T09:00:01 INFO  [DRY-RUN] add_replication_rule('cms':'transfer_test_20260321_a1b2c3d4e5f6')
2026-03-21T09:00:01 INFO  Run finished successfully. State: state_a1b2c3d4e5f6.json
```

A state file is created (tracking the run ID) but no files are written.

### Step 2 — Live run

```bash
dataset-generator --config my_site.yaml
```

The tool:

1. Obtains an OIDC bearer token (client credentials grant).
2. Creates the dataset DID in Rucio (`add_dataset`).
3. Generates files in parallel (Pool A, `--threads` workers):
   - Writes each file to `{rse_mount}/.gen_tmp/` in 64 MiB chunks.
   - Accumulates an adler32 checksum over every chunk.
   - Resolves the final physical path via `lfns2pfns`.
   - Atomically renames the temp file into position (`os.rename`).
4. Registers all replicas in a single batch call (`add_replicas`).
5. Attaches all file DIDs to the dataset (`attach_dids`).
6. Creates one replication rule on the dataset DID (`add_replication_rule`).

Progress is shown per file and the final state is written to
`state_{run_id}.json` in the working directory.

### Step 3 — Verify in Rucio

```bash
rucio ls cms:transfer_test_20260321_a1b2c3d4e5f6
rucio list-files cms:transfer_test_20260321_a1b2c3d4e5f6
rucio list-rules --did cms:transfer_test_20260321_a1b2c3d4e5f6
```

---

## Operational modes

### Full (default)

Generate → register → attach → rule.

```bash
dataset-generator --config my_site.yaml
```

### Generate files only (`--create-only`)

Write files to the RSE; skip all Rucio calls. Useful for pre-staging before
a network maintenance window.

```bash
dataset-generator --config my_site.yaml --create-only
```

### Register only (`--register-only`)

Register files that are already on the RSE and recorded in a state file.
Use after a `--create-only` run or when Rucio was unavailable during the
original run.

```bash
dataset-generator --config my_site.yaml \
  --register-only \
  --state-file state_a1b2c3d4e5f6.json
```

### Cleanup (`--cleanup`)

Remove failed or orphaned files from the RSE filesystem and delete their
replica records from Rucio. Acts on entries in `failed_creation`,
`failed_registration`, `failed_rule`, and `created` (placed but unregistered)
states. Does **not** touch entries in `registered` or `ruled` state — those
are live production data.

```bash
dataset-generator --config my_site.yaml \
  --cleanup \
  --state-file state_a1b2c3d4e5f6.json
```

---

## Resuming an interrupted run

Every state transition is written atomically to `state_{run_id}.json` before
the next step begins. If the tool is interrupted (network error, OOM kill,
`Ctrl-C`), resume by passing the same state file:

```bash
dataset-generator --config my_site.yaml \
  --state-file state_a1b2c3d4e5f6.json
```

- Files in `created`, `registered`, or `ruled` state are **skipped**.
- Files in `pending`, `failed_creation`, `failed_registration`, or
  `failed_rule` state are **retried**.

The `run_id` (and therefore the dataset DID name) is read from the state file
automatically, so the resume joins the existing dataset.

---

## Running the test suite

The tests use only the standard library plus `pytest` — they mock all Rucio
and HTTP calls, so no Rucio server or RSE mount is needed.

```bash
# Activate the virtual environment if not already active
source .venv/bin/activate

# Install dev dependencies (includes pytest)
pip install -e ".[dev]"

# Run all tests
pytest

# Verbose output with short tracebacks
pytest -v --tb=short

# Run a single test file
pytest tests/test_generator.py

# Run tests matching a name pattern
pytest -k "test_adler32 or test_checksum"

# Run with coverage report (requires pytest-cov)
pip install pytest-cov
pytest --cov=dataset_generator --cov-report=term-missing
```

The suite currently contains **108 tests** across four files:

| File | What it covers |
|------|---------------|
| `tests/test_config.py` | Config construction, YAML+CLI merge, validation |
| `tests/test_state.py` | State file lifecycle, atomicity, thread safety |
| `tests/test_generator.py` | File write, adler32 correctness, atomic rename, Pool A, resume |
| `tests/test_rucio_client.py` | OIDC token lifecycle, retry logic, all Rucio API calls |

All tests run in under one second. No network access or RSE mount is required.

---

## CLI reference

```
usage: dataset-generator [--config PATH] [--dry-run] [--threads N]
                         [--log-level LEVEL] [--state-file PATH] [--run-id ID]
                         [--create-only | --register-only | --cleanup]
                         [config overrides ...]

General:
  --config PATH          YAML configuration file
  --dry-run              Log all actions without executing them
  --threads N            File-generation thread count (Pool A). Default: 4
  --log-level LEVEL      DEBUG | INFO | WARNING | ERROR. Default: INFO
  --state-file PATH      Override state file path. Default: ./state_{run_id}.json
  --run-id ID            Override run identifier (12-char hex); used for resume

Operational mode (mutually exclusive):
  --create-only          Generate and place files; skip Rucio registration
  --register-only        Register already-placed files from state file
  --cleanup              Remove orphaned files and delete replica records

Config overrides (all settable in YAML; CLI wins):
  --scope                Rucio scope
  --rse                  Target RSE name
  --rse-mount            POSIX mount path of RSE storage
  --dataset-prefix       Dataset DID name prefix
  --file-prefix          File LFN prefix
  --num-files N          Number of files to generate
  --file-size-bytes N    File size in bytes
  --rule-lifetime SECS   Replication rule lifetime in seconds (omit = permanent)
  --rucio-host           Rucio server URL
  --rucio-auth-host      Rucio auth server URL
  --rucio-account        Rucio account name
  --token-endpoint       OIDC token endpoint URL
  --client-id            OIDC client ID
  --client-secret        OIDC client secret

Exit codes:
  0   All files successfully processed (or dry run completed)
  1   One or more files failed; see log for details
  2   Configuration or environment error (bad YAML, missing RSE mount, etc.)
```

---

## Troubleshooting

**`ConfigError: rse_mount '/mnt/rse' is not a directory`**

The RSE storage volume is not mounted on this host. Verify the mount:

```bash
ls /mnt/rse
df -h /mnt/rse
```

**`RuntimeError: OIDC token request to '...' failed`**

The token endpoint is unreachable or returned an error. Check:

- Network connectivity to the IAM host.
- That `client_id` and `client_secret` are correct.
- That the client has the `rucio` scope or equivalent on your IAM instance.

**`StateError: State file '...' has unsupported version`**

The state file was written by an incompatible version of this tool. Do not
mix state files across major releases.

**Files created but not registered after interruption**

Resume the run with `--state-file`. Files in `created` state will be picked
up and registered. Alternatively, use `--register-only` with the same state
file.

**Rule lifetime not taking effect**

Check `rucio list-rules --did cms:your_dataset` — the `EXPIRES AT` column
should reflect the configured lifetime. If it shows `None`, `rule_lifetime`
was not set or was `null` in the YAML (which means a permanent rule,
intentionally).
