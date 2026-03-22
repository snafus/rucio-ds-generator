# rucio-ds-generator

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
- [Static datasets and container grouping](#static-datasets-and-container-grouping)
- [Dataset registry](#dataset-registry)
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
git clone https://github.com/snafus/rucio-ds-generator.git
cd rucio-ds-generator
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

Configuration has three layers, applied in order of precedence (highest first):

1. **CLI flags** — `--rucio-host`, `--client-secret`, etc.
2. **Environment variables** — loaded from a `.env` file or the shell environment
3. **YAML file** — `--config my_site.yaml`

### Step 1 — Rucio client config (`rucio.cfg`)

Rucio's client library requires a `rucio.cfg` before it will start.
Create a minimal one and point `RUCIO_CONFIG` at it (see Step 2):

```ini
[client]
rucio_host  = https://rucio.example.org
auth_host   = https://rucio-auth.example.org
account     = your-account
auth_type   = oidc
oidc_scope  = openid profile offline_access
oidc_audience = rucio
auth_oidc_refresh_active     = True
auth_oidc_refresh_before_exp = 20
request_retries = 3
```

`rucio.cfg` is git-ignored. Place it anywhere and set `RUCIO_CONFIG` to its
absolute path, or put it at `$VIRTUAL_ENV/etc/rucio.cfg`.

**OIDC and token refresh:**

| Setting | Value | Purpose |
|---|---|---|
| `auth_type` | `oidc` | Use OIDC bearer tokens |
| `oidc_scope` | `openid profile offline_access` | Enables refresh tokens |
| `auth_oidc_refresh_active` | `True` | Auto-refresh before expiry |
| `auth_oidc_refresh_before_exp` | `20` | Refresh 20 min before expiry |

For service-account / robot use, this tool obtains tokens via the OAuth 2.0
**client_credentials** grant and re-fetches automatically when the token
expires — long-running jobs stay authenticated without manual intervention.

### Step 2 — Sensitive credentials (`.env`)

Endpoints, account name, and OIDC client credentials belong in `.env`:

```bash
cp .env.template .env
$EDITOR .env
```

`.env` is git-ignored. `.env.template` (with placeholder values) is committed so
the required variable names are always visible. The variables that are read:

| Variable | Purpose |
|---|---|
| `RUCIO_CONFIG` | Path to your `rucio.cfg` |
| `RUCIO_HOST` | Rucio server URL |
| `RUCIO_AUTH_HOST` | Rucio auth server URL |
| `RUCIO_ACCOUNT` | Your Rucio account name |
| `OIDC_TOKEN_ENDPOINT` | IAM token endpoint (client credentials grant) |
| `OIDC_CLIENT_ID` | OIDC client ID |
| `OIDC_CLIENT_SECRET` | OIDC client secret |

Variables already set in the shell take precedence over `.env` (standard
`python-dotenv` behaviour with `override=False`).

### Verify auth

Once both files are in place, verify connectivity before running a real job:

```bash
dataset-generator --check-auth
# Auth OK
#   account : myaccount
#   status  : ACTIVE
#   type    : SERVICE
```

### YAML config

Copy the annotated example and edit for your site:

```bash
cp config.example.yaml my_site.yaml
```

The minimal required fields are:

```yaml
# Rucio identity
scope: cms                                    # Rucio scope for all DIDs
rse: T2_US_Wisconsin                          # Target RSE name
rse_mount: /mnt/hadoop/store/rucio            # POSIX path to RSE storage on this host

# Dataset and file naming  (Jinja2 templates supported — see below)
dataset_prefix: transfer_test                 # Default name: {prefix}_{date}_{run_id}
file_prefix: dummy                            # Final filename: {prefix}_{adler32hex}

# Generation parameters
num_files: 10
file_size_bytes: 1GiB          # raw integer bytes, or human-readable:
                               #   IEC (binary): KiB/MiB/GiB/TiB/PiB
                               #   SI  (decimal): KB/MB/GB/TB/PB
                               #   Bare K/M/G/T/P = binary
```

### Jinja2 naming templates

`dataset_name`, `file_prefix`, and `container_name` all support Jinja2
template syntax.  Plain strings without `{{` are used as-is with no overhead.

Available template variables:

| Variable | Example value | Description |
|---|---|---|
| `{{ date }}` | `20260321` | UTC date, `YYYYMMDD` |
| `{{ datetime }}` | `20260321_090000` | UTC datetime, `YYYYMMDD_HHMMSS` |
| `{{ timestamp }}` | `1742547600` | UTC Unix timestamp (integer) |
| `{{ run_id }}` | `a1b2c3d4e5f6` | 12-character hex run identifier |
| `{{ scope }}` | `cms` | Rucio scope |
| `{{ rse }}` | `T2_US_Wisconsin` | Target RSE name |
| `{{ num_files }}` | `10` | Number of files (integer) |
| `{{ file_size }}` | `1GiB` | Human-readable file size |
| `{{ file_size_bytes }}` | `1073741824` | File size in bytes (integer) |
| `{{ dataset_prefix }}` | `transfer_test` | Dataset prefix string |

Example templates:

```yaml
# Dataset name encoding the RSE and date
dataset_name: "{{ dataset_prefix }}_{{ rse }}_{{ date }}"
# → transfer_test_T2_US_Wisconsin_20260321

# File prefix encoding size and scope
file_prefix: "{{ scope }}_{{ file_size }}_dummy"
# → cms_1GiB_dummy  (final filename: cms_1GiB_dummy_1a2b3c4d)

# Container grouping by RSE
container_name: "{{ scope }}_{{ rse }}_tests"
# → cms_T2_US_Wisconsin_tests
```

> **Note on `{{ timestamp }}` in `file_prefix`:** the rendered value is
> cached on first access and reused for all files within a run, so every file
> shares an identical prefix even when generation spans many seconds.

Optional settings (shown with their defaults):

```yaml
threads: 4                # File-generation worker processes (Pool A). Default: 4
log_level: INFO           # DEBUG | INFO | WARNING | ERROR
rule_lifetime: ~          # Rule lifetime in seconds; null = permanent
                          # e.g. 604800 = 7 days, 2592000 = 30 days

# Static or template dataset name — reuse the same dataset DID across runs (see below)
# dataset_name: "{{ dataset_prefix }}_{{ rse }}_{{ date }}"

# Attach datasets to a named Rucio container after each run (see below)
# container_name: "{{ scope }}_{{ rse }}_tests"

# Staging directory for temporary file creation before placement on the RSE.
# A unique per-run subdirectory is created here automatically.
# Default: system temp dir.
# staging_dir: /scratch/rucio-staging

# PFN prefix translation: if Rucio returns PFNs with a different path root
# from your local mount, set both of these:
#   rse_pfn_prefix: davs://xrootd.example.org:1094/store/rucio
#   rse_mount:      /mnt/rse
# rse_pfn_prefix: ~

# Ownership for placed files and any hash directories created on the RSE.
# Omit to keep the process's own uid/gid.
# rse_uid: 1000
# rse_gid: 1000

# Global dataset registry file. Default: ~/.rucio-ds-generator/registry.json
# Set to empty string to disable.
# registry_file: ~/.rucio-ds-generator/registry.json

# File-generation back-end.
# csprng (default): streaming pseudo-random data — fastest available stdlib
#   PRNG (random.randbytes on Python 3.9+, os.urandom on 3.6-3.8).
# buffer-reuse: pre-fills a fixed ring buffer with random data once at startup.
#   Each chunk write reads from a random offset in the ring with no further
#   PRNG calls, shifting the bottleneck from PRNG throughput to disk bandwidth.
#   Configure the ring size with buffer_reuse_ring_size.
# generation_mode: csprng

# Ring buffer size for buffer-reuse mode.
# Accepts a raw integer (bytes) or human-readable string (same units as file_size_bytes).
# Must be >= 128 MiB (the write chunk size). Peak RSS during construction ≈ 2 × ring size.
# Memory footprint per worker process ≈ ring_size + 128 MiB; at 4 workers + 512 MiB ≈ 2.5 GiB total.
# Default: 512MiB. Larger values provide greater data variety across files.
# buffer_reuse_ring_size: 512MiB

# Disable fallocate() pre-allocation when placing files on the RSE.
# fallocate is enabled by default and tells the OS to reserve disk space before writing,
# reducing fragmentation and avoiding mid-write ENOSPC on most filesystems.
# Disable on CephFS (and similar distributed filesystems) where fallocate() fills the
# allocated space with zeroes instead of merely reserving it, causing unnecessary I/O.
# fallocate: true
```

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
2026-03-21T09:00:01 INFO  Dataset DID : cms:transfer_test_20260321_a1b2c3d4e5f6
2026-03-21T09:00:01 INFO  [DRY-RUN] add_dataset('cms', 'transfer_test_20260321_a1b2c3d4e5f6')
Generating: 100%|████████████████| 10/10 [00:00<00:00, file]
2026-03-21T09:00:01 INFO  [DRY-RUN] add_replicas('T2_US_Wisconsin', 10 files)
2026-03-21T09:00:01 INFO  [DRY-RUN] attach_dids('cms':'transfer_test_20260321_a1b2c3d4e5f6', 10 files)
2026-03-21T09:00:01 INFO  [DRY-RUN] add_replication_rule('cms':'transfer_test_20260321_a1b2c3d4e5f6')
2026-03-21T09:00:01 INFO  Run finished successfully. State: state_a1b2c3d4e5f6.json
```

### Step 2 — Live run

```bash
dataset-generator --config my_site.yaml
```

The tool:

1. Obtains an OIDC bearer token (client credentials grant).
2. Asserts the target RSE is deterministic.
3. Creates the dataset DID in Rucio (`add_dataset`).
4. Counts files already in the dataset (`list_files`) and computes how many
   new files to generate: `max(0, num_files − existing_in_rucio − state_count)`.
   If the dataset already has enough files, generation is skipped entirely.
5. Generates files in parallel (Pool A, `--threads` worker processes):
   - Writes each file to a unique per-run staging directory in 128 MiB chunks.
   - Accumulates an adler32 checksum over every chunk.
   - Resolves the final physical path via `lfns2pfns`.
   - Copies the file to `{final_pfn}.part.{pid}.{tid}` on the RSE filesystem
     using `os.sendfile` (kernel zero-copy) where available, or an 8 MiB
     read/write loop otherwise.  Sets ownership (`rse_uid`/`rse_gid` if
     configured), then atomically renames to the final path.
   - Placements run concurrently with the next write via a dedicated
     `ThreadPoolExecutor` (Pool B), so disk I/O and RSE copy overlap.
6. Registers all replicas in a single batch call (`add_replicas`).
7. Attaches all file DIDs to the dataset (`attach_dids`).
8. Creates one replication rule on the dataset DID (`add_replication_rule`).
9. If `container_name` is set, attaches the dataset to the container.
10. Updates the dataset registry file.

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

## Static datasets and container grouping

### Static dataset name

By default each run creates a new dataset DID:
`{dataset_prefix}_{YYYYMMDD}_{run_id}`.

To accumulate files into the same dataset across multiple runs, set a fixed
name:

```yaml
dataset_name: my_persistent_test_dataset
```

or pass `--dataset-name my_persistent_test_dataset` on the CLI.

On subsequent runs the tool queries the current file count in Rucio and only
generates the shortfall: `max(0, num_files − existing)`. If the dataset
already contains at least `num_files` files, generation is skipped entirely
and the tool proceeds directly to ensuring the replication rule exists.
A `DuplicateRule` response is caught and logged as a warning.

### Container grouping

To group multiple datasets under a single Rucio container, set `container_name`:

```yaml
container_name: my_test_container
```

After each successful run the tool will:

1. Create the container if it does not exist (`add_container`).
2. Attach the dataset to the container (`attach_dids`).

Attaching a dataset that is already a member of the container
(`DuplicateContent`) is silently ignored, so re-runs and static dataset names
are safe.

Verify the container in Rucio:

```bash
rucio list-dids cms:my_test_container --type container
rucio list-content cms:my_test_container
```

---

## Dataset registry

After each successful pipeline run a record is written to a persistent JSON
registry file (default: `~/.rucio-ds-generator/registry.json`). The registry
accumulates entries across runs and sites — it is useful for tracking what test
data exists and where.

Each entry records:

| Field | Description |
|-------|-------------|
| `dataset_did` | Full dataset DID (`scope:name`) |
| `rse` | RSE the data was placed on |
| `rule_id` | Rucio replication rule ID (most recent run) |
| `num_files` | Cumulative file count across all runs targeting this dataset |
| `first_seen` | UTC timestamp of the first run |
| `last_updated` | UTC timestamp of the most recent run |

```json
{
  "version": 1,
  "datasets": {
    "cms:my_persistent_test_dataset": {
      "dataset_did":  "cms:my_persistent_test_dataset",
      "rse":          "T2_US_Wisconsin",
      "rule_id":      "a1b2c3d4e5f6...",
      "num_files":    30,
      "first_seen":   "2026-03-21T09:00:00Z",
      "last_updated": "2026-03-21T11:00:00Z"
    }
  }
}
```

Override the registry path:

```bash
dataset-generator --config my_site.yaml --registry-file /shared/registry.json
```

Disable registry recording entirely:

```bash
dataset-generator --config my_site.yaml --registry-file ""
# or in YAML:
# registry_file: ""
```

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

The suite currently contains **274 tests** across seven files:

| File | What it covers |
|------|---------------|
| `tests/test_config.py` | Config construction, YAML+CLI merge, validation, dataset/container naming |
| `tests/test_state.py` | State file lifecycle, atomicity, thread safety |
| `tests/test_generator.py` | File write, adler32 correctness, atomic rename, Pool A, resume |
| `tests/test_rucio_client.py` | OIDC token lifecycle, retry logic, all Rucio API calls, container ops |
| `tests/test_registry.py` | Registry upsert, persistence, thread safety, atomic write |
| `tests/test_writers.py` | FileWriter ABC, CsprngFileWriter, BufferReuseFileWriter, factory |
| `tests/test_main.py` | Pipeline error paths: rule-creation failure, status transitions |

All tests run in under ten seconds. No network access or RSE mount is required.

---

## CLI reference

```
usage: dataset-generator [--config PATH] [--dry-run] [--threads N]
                         [--log-level LEVEL] [--state-file PATH] [--run-id ID]
                         [--check-auth] [--no-fallocate]
                         [--create-only | --register-only | --cleanup]
                         [config overrides ...]

General:
  --config PATH          YAML configuration file
  --dry-run              Log all actions without executing them
  --threads N            File-generation worker process count (Pool A). Default: 4
  --log-level LEVEL      DEBUG | INFO | WARNING | ERROR. Default: INFO
  --state-file PATH      Override state file path. Default: ./state_{run_id}.json
  --run-id ID            Override run identifier (12-char hex); used for resume
  --check-auth           Test OIDC token acquisition and Rucio connectivity, then exit

Operational mode (mutually exclusive):
  --create-only          Generate and place files; skip Rucio registration
  --register-only        Register already-placed files from state file
  --cleanup              Remove orphaned files and delete replica records

Config overrides (all settable in YAML; CLI wins):
  --scope                Rucio scope
  --rse                  Target RSE name
  --rse-mount            POSIX mount path of RSE storage
  --dataset-prefix       Dataset DID name prefix (used for dynamic names)
  --dataset-name NAME    Fixed dataset DID name; reused across runs
  --file-prefix          File LFN prefix
  --num-files N          Number of files to generate
  --file-size-bytes SIZE File size — raw integer bytes or human-readable string.
                         IEC (binary): KiB, MiB, GiB, TiB, PiB (powers of 1024).
                         SI (decimal): KB, MB, GB, TB, PB (powers of 1000).
                         Bare K/M/G/T/P treated as binary. Examples: 1GiB, 512MiB, 1GB, 500MB
  --rule-lifetime SECS   Replication rule lifetime in seconds (omit = permanent)
  --staging-dir PATH     Directory for temporary file creation (default: system temp)
  --rse-pfn-prefix PFX   Full PFN prefix returned by Rucio (stripped and replaced
                         with rse-mount to get the local filesystem path)
  --rse-uid UID          uid for placed files and newly created RSE directories
  --rse-gid GID          gid for placed files and newly created RSE directories
  --container-name NAME  Rucio container to attach the dataset to after each run
  --generation-mode MODE File-generation back-end: csprng (default) or buffer-reuse.
                         buffer-reuse pre-fills a ring with random data; writes become
                         disk/memory-bandwidth-limited rather than PRNG-limited.
  --buffer-reuse-ring-size SIZE
                         Ring buffer size for buffer-reuse mode (default: 512MiB).
                         Same unit syntax as --file-size-bytes. Must be >= 128 MiB.
                         Memory footprint per worker process ≈ ring_size + 128 MiB.
  --no-fallocate         Disable fallocate() pre-allocation when placing files on the RSE.
                         Recommended on CephFS where fallocate() writes zeros instead of
                         reserving space. Enabled by default on supporting filesystems.
  --registry-file PATH   Global registry JSON path (default: ~/.rucio-ds-generator/registry.json;
                         set to "" to disable)
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

Use `--check-auth` to test credentials in isolation before a full run.

**`RuntimeError: PFN '...' does not start with rse_pfn_prefix`**

Rucio returned a PFN that doesn't match your configured `rse_pfn_prefix`.
Run with `--log-level DEBUG` to see the raw PFN returned by `lfns2pfns`, then
set `rse_pfn_prefix` to the full URL prefix Rucio uses for this RSE
(e.g. `davs://xrootd.example.org:1094/store/rucio`).

**`StateError: State file '...' has unsupported version`**

The state file was written by an incompatible version of this tool. Do not
mix state files across major releases.

**Files created but not registered after interruption**

Resume the run with `--state-file`. Files in `created` state will be picked
up and registered. Alternatively, use `--register-only` with the same state
file.

**`DuplicateRule` warning on re-run with static dataset name**

This is expected behaviour. When `dataset_name` is set and you run the tool
more than once, Rucio rejects a second rule on the same dataset+RSE
combination. The tool catches this, logs a warning, and continues — the
existing rule remains in effect.

**Rule lifetime not taking effect**

Check `rucio list-rules --did cms:your_dataset` — the `EXPIRES AT` column
should reflect the configured lifetime. If it shows `None`, `rule_lifetime`
was not set or was `null` in the YAML (which means a permanent rule,
intentionally).
