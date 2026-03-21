"""
rucio_client.py — Rucio operations and inline OIDC token management.

Design
------
``RucioManager`` owns all Rucio interactions and the OIDC token lifecycle.

OIDC token acquisition
~~~~~~~~~~~~~~~~~~~~~~
We perform a standard OAuth 2.0 client-credentials grant against the
configured ``token_endpoint`` using ``requests``.  The resulting bearer
token is cached in memory with its absolute expiry time
(``expires_at = time.time() + expires_in - 30``).  A 30-second buffer
prevents using a token that expires mid-request.  Token refresh is
triggered before each batch of Rucio calls and is protected by a
``threading.Lock`` so only one thread performs the HTTP grant at a time.

Rucio client construction
~~~~~~~~~~~~~~~~~~~~~~~~~
We construct a ``rucio.client.Client`` instance per Rucio operation batch
and inject the pre-obtained token by setting ``client.auth_token`` directly.
Rucio's ``BaseClient`` uses lazy authentication: it checks ``self.auth_token``
on the first HTTP request and only calls its own auth flow if the attribute
is ``None``.  Setting it before any call bypasses Rucio's OIDC flow entirely.

If a future Rucio release changes this behaviour, replace the attribute
assignment with whichever mechanism that version supports (e.g. a
``creds`` dict or token file path).

Retry strategy
~~~~~~~~~~~~~~
Every Rucio API call is wrapped in ``_retry``, which performs up to
``MAX_RETRIES`` attempts with exponential back-off (2 s, 4 s, 8 s).
``rucio.common.exception.RucioException`` subclasses and ``requests``
exceptions are both retried.  Other exceptions propagate immediately.

Rucio API notes (38.3+)
~~~~~~~~~~~~~~~~~~~~~~~
* ``lfns2pfns(rse, lfns)`` — ``lfns`` is a list of ``"scope:name"`` strings.
  Returns ``{"scope:name": "/physical/path"}``.
* ``add_replicas(rse, files)`` — ``files`` is a list of dicts with required
  keys ``scope``, ``name``, ``bytes``, ``adler32``, and optional ``pfn``
  (required for non-deterministic RSEs).
* ``add_dataset(scope, name)`` — must be called before ``attach_dids``.
* ``add_container(scope, name)`` — creates a container DID.
* ``attach_dids(scope, name, dids)`` — ``dids`` is a list of
  ``{"scope": ..., "name": ...}`` dicts; works for both datasets→files
  and containers→datasets.
* ``add_replication_rule(dids, copies, rse_expression)`` — call on the
  *dataset* DID, not per-file DIDs.
* ``delete_replicas(rse, files)`` — ``files`` is a list of
  ``{"scope": ..., "name": ...}`` dicts.
"""

import logging
import threading
import time
from typing import Dict, List, Optional

import requests
from rucio.client import Client

log = logging.getLogger(__name__)

MAX_RETRIES = 3
RETRY_DELAYS = (2, 4, 8)  # seconds; index matches attempt number (0-based)


# ---------------------------------------------------------------------------
# OIDC token container
# ---------------------------------------------------------------------------

class _OIDCToken(object):
    """In-memory container for a bearer token and its absolute expiry."""

    def __init__(self, access_token, expires_at):
        # type: (str, float) -> None
        self.access_token = access_token   # raw token string (no "Bearer " prefix)
        self.expires_at = expires_at       # absolute UNIX timestamp

    def is_fresh(self):
        # type: () -> bool
        """Return True if the token will not expire within the safety buffer."""
        return time.time() < self.expires_at


# ---------------------------------------------------------------------------
# RucioManager
# ---------------------------------------------------------------------------

class RucioManager(object):
    """
    Facade for all Rucio operations needed by this tool.

    Thread-safe: multiple threads may call ``lfns2pfn`` concurrently;
    the token refresh lock ensures only one thread fetches a new token at a
    time, while others wait for the result.

    Parameters
    ----------
    config:
        ``Config`` instance from ``config.py``.
    dry_run:
        When ``True`` all mutating Rucio calls are logged but not executed.
        ``lfns2pfn`` is always executed regardless of this flag (it is a
        read-only query needed to determine file placement paths).
    """

    def __init__(self, config, dry_run=False):
        # type: (object, bool) -> None
        self._cfg = config
        self._dry_run = dry_run
        self._token = None        # type: Optional[_OIDCToken]
        self._token_lock = threading.Lock()

    # ------------------------------------------------------------------
    # Token management
    # ------------------------------------------------------------------

    def _get_token(self):
        # type: () -> str
        """
        Return a valid bearer token, refreshing via OIDC client-credentials
        grant if the cached token is absent or expiring.

        Thread-safe: at most one thread performs the HTTP grant at a time.
        """
        with self._token_lock:
            if self._token is None or not self._token.is_fresh():
                self._token = self._fetch_token()
            return self._token.access_token

    def _fetch_token(self):
        # type: () -> _OIDCToken
        """
        Perform an OAuth 2.0 client-credentials grant and return an
        ``_OIDCToken``.

        Raises ``RuntimeError`` if the token endpoint returns a non-2xx
        response or the response body is malformed.
        """
        log.debug("Fetching new OIDC token from %s", self._cfg.token_endpoint)
        try:
            resp = requests.post(
                self._cfg.token_endpoint,
                data={
                    "grant_type": "client_credentials",
                    "client_id": self._cfg.client_id,
                    "client_secret": self._cfg.client_secret,
                },
                timeout=30,
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            raise RuntimeError(
                "OIDC token request to {!r} failed: {}".format(
                    self._cfg.token_endpoint, exc
                )
            )

        try:
            body = resp.json()
            access_token = body["access_token"]
            expires_in = int(body.get("expires_in", 3600))
        except (ValueError, KeyError) as exc:
            raise RuntimeError(
                "Malformed token response from {!r}: {}".format(
                    self._cfg.token_endpoint, exc
                )
            )

        expires_at = time.time() + expires_in - 30  # 30 s safety buffer
        log.debug("Obtained OIDC token; valid for %d s (buffer applied)", expires_in)
        return _OIDCToken(access_token, expires_at)

    # ------------------------------------------------------------------
    # Rucio client construction
    # ------------------------------------------------------------------

    def _make_client(self):
        # type: () -> object
        """
        Construct a ``rucio.client.Client`` pre-loaded with the current
        bearer token.

        We set ``client.auth_token`` directly after construction.  Rucio's
        ``BaseClient`` uses lazy authentication and only calls its own auth
        flow when ``self.auth_token`` is ``None``.  Pre-setting the attribute
        routes all requests through the token we obtained ourselves.
        """
        token = self._get_token()
        client = Client(
            rucio_host=self._cfg.rucio_host,
            auth_host=self._cfg.rucio_auth_host,
            account=self._cfg.rucio_account,
            auth_type="oidc",
        )
        # Inject pre-obtained token — bypasses Rucio's own OIDC acquisition.
        client.auth_token = token
        return client

    # ------------------------------------------------------------------
    # Retry wrapper
    # ------------------------------------------------------------------

    def _retry(self, fn, description):
        # type: (callable, str) -> object
        """
        Call *fn()* with up to ``MAX_RETRIES`` attempts and exponential
        back-off.

        *description* is a human-readable label used in log messages.
        Retries on ``rucio.common.exception.RucioException`` and
        ``requests.RequestException``; other exceptions propagate immediately.
        """
        last_exc = None
        for attempt in range(MAX_RETRIES):
            try:
                return fn()
            except Exception as exc:
                # Detect retryable exception types by class name so we don't
                # need a hard import of rucio.common.exception at module load.
                # Only requests errors and RucioException subclasses are retried;
                # all other exceptions (including Python built-ins) propagate
                # immediately without consuming retry attempts.
                exc_type = type(exc).__name__
                is_retryable = (
                    isinstance(exc, requests.RequestException)
                    or "RucioException" in exc_type
                )
                if not is_retryable or attempt == MAX_RETRIES - 1:
                    raise
                delay = RETRY_DELAYS[attempt]
                log.warning(
                    "%s failed (attempt %d/%d): %s — retrying in %d s",
                    description, attempt + 1, MAX_RETRIES, exc, delay,
                )
                time.sleep(delay)
                last_exc = exc
        raise RuntimeError("Exhausted retries for: {}".format(description))  # unreachable

    # ------------------------------------------------------------------
    # PFN resolution
    # ------------------------------------------------------------------

    def lfns2pfn(self, rse, lfn):
        # type: (str, str) -> str
        """
        Resolve a single LFN string (``"scope:name"``) to its PFN on *rse*.

        Uses ``lfns2pfns`` on the Rucio ``ReplicaClient`` (plural method,
        single-element list).  For deterministic RSEs the PFN is derived
        server-side from the RSE protocol configuration.

        Parameters
        ----------
        rse:
            RSE name.
        lfn:
            LFN string in ``"scope:name"`` format.

        Returns
        -------
        str
            Physical file name (absolute path) on the RSE.
        """
        client = self._make_client()

        def _call():
            # lfns2pfns expects a list; returns {"scope:name": "/pfn/path"}
            result = client.lfns2pfns(rse=rse, lfns=[lfn])
            if lfn not in result:
                raise RuntimeError(
                    "lfns2pfns returned no entry for {!r}: {!r}".format(lfn, result)
                )
            return result[lfn]

        return self._retry(_call, "lfns2pfns({!r}, {!r})".format(rse, lfn))

    # ------------------------------------------------------------------
    # Dataset operations
    # ------------------------------------------------------------------

    def count_dataset_files(self, scope, name):
        # type: (str, str) -> int
        """
        Return the number of files currently attached to the dataset DID.

        Returns ``0`` if the dataset does not yet exist
        (``DataIdentifierNotFound``).  This is used to determine how many
        additional files need to be generated to reach ``num_files``.

        Parameters
        ----------
        scope:
            Rucio scope.
        name:
            Dataset name component (no scope prefix).
        """
        if self._dry_run:
            log.info("[DRY-RUN] Skipping file count for %r:%r — returning 0", scope, name)
            return 0

        client = self._make_client()

        def _call():
            try:
                return sum(1 for _ in client.list_files(scope=scope, name=name))
            except Exception as exc:
                if "DataIdentifierNotFound" in type(exc).__name__:
                    return 0
                raise

        return self._retry(_call, "count_dataset_files({!r}:{!r})".format(scope, name))

    def add_dataset(self, scope, name):
        # type: (str, str) -> None
        """
        Create a Rucio dataset DID.

        Must be called before ``attach_dids``.  If the dataset already exists
        (e.g. on resume), the ``DataIdentifierAlreadyExists`` exception from
        Rucio is caught and logged as a warning — the run continues.

        Parameters
        ----------
        scope:
            Rucio scope.
        name:
            Dataset name (without scope prefix).
        """
        if self._dry_run:
            log.info("[DRY-RUN] add_dataset(%r, %r)", scope, name)
            return

        client = self._make_client()

        def _call():
            try:
                client.add_dataset(scope=scope, name=name)
                log.info("Created dataset DID: %s:%s", scope, name)
            except Exception as exc:
                if "DataIdentifierAlreadyExists" in type(exc).__name__:
                    log.warning("Dataset %s:%s already exists — continuing", scope, name)
                else:
                    raise

        self._retry(_call, "add_dataset({!r}:{!r})".format(scope, name))

    def add_container(self, scope, name):
        # type: (str, str) -> None
        """
        Create a Rucio container DID.

        If the container already exists (``DataIdentifierAlreadyExists``) the
        exception is silently ignored — the run continues normally.

        Parameters
        ----------
        scope:
            Rucio scope.
        name:
            Container name (without scope prefix).
        """
        if self._dry_run:
            log.info("[DRY-RUN] add_container(%r, %r)", scope, name)
            return

        client = self._make_client()

        def _call():
            try:
                client.add_container(scope=scope, name=name)
                log.info("Created container DID: %s:%s", scope, name)
            except Exception as exc:
                if "DataIdentifierAlreadyExists" in type(exc).__name__:
                    log.debug("Container %s:%s already exists — continuing", scope, name)
                else:
                    raise

        self._retry(_call, "add_container({!r}:{!r})".format(scope, name))

    def attach_dataset_to_container(self, scope, container_name, dataset_name):
        # type: (str, str, str) -> None
        """
        Attach a dataset DID to a container DID.

        If the dataset is already a member of the container
        (``DuplicateContent``), the exception is silently ignored.

        Parameters
        ----------
        scope:
            Scope shared by both the container and the dataset.
        container_name:
            Container name component (no scope prefix).
        dataset_name:
            Dataset name component (no scope prefix) to attach.
        """
        if self._dry_run:
            log.info(
                "[DRY-RUN] attach_dataset_to_container(%r:%r ← dataset %r)",
                scope, container_name, dataset_name,
            )
            return

        client = self._make_client()

        def _call():
            try:
                client.attach_dids(
                    scope=scope,
                    name=container_name,
                    dids=[{"scope": scope, "name": dataset_name}],
                )
                log.info(
                    "Attached dataset %s:%s to container %s:%s",
                    scope, dataset_name, scope, container_name,
                )
            except Exception as exc:
                if "DuplicateContent" in type(exc).__name__:
                    log.debug(
                        "Dataset %s:%s already in container %s:%s — skipping",
                        scope, dataset_name, scope, container_name,
                    )
                else:
                    raise

        self._retry(
            _call,
            "attach_dataset_to_container({!r}:{!r} ← {!r})".format(
                scope, container_name, dataset_name,
            ),
        )

    # ------------------------------------------------------------------
    # Replica operations
    # ------------------------------------------------------------------

    def add_replicas(self, rse, files):
        # type: (str, List[dict]) -> None
        """
        Register a batch of file replicas in Rucio.

        Issues a single ``add_replicas`` call (one HTTP request) for the
        entire batch.  Each entry in *files* must contain:

        * ``scope``   — Rucio scope string
        * ``name``    — LFN name component (no scope prefix)
        * ``bytes``   — file size in bytes (``int``)
        * ``adler32`` — lowercase 8-char hex checksum string
        * ``pfn``     — (optional) explicit PFN; required for non-deterministic RSEs

        Parameters
        ----------
        rse:
            Target RSE name.
        files:
            List of file descriptor dicts (see above).
        """
        if not files:
            return
        if self._dry_run:
            log.info("[DRY-RUN] add_replicas(%r, %d files)", rse, len(files))
            return

        client = self._make_client()

        def _call():
            client.add_replicas(rse=rse, files=files)
            log.info("Registered %d replica(s) on %s", len(files), rse)

        self._retry(_call, "add_replicas({!r}, {} files)".format(rse, len(files)))

    def delete_replicas(self, rse, files):
        # type: (str, List[dict]) -> None
        """
        Remove replica records from Rucio and, where possible, the physical
        files from the RSE.

        Used by cleanup mode to remove failed or orphaned entries.

        Parameters
        ----------
        rse:
            RSE name.
        files:
            List of ``{"scope": ..., "name": ...}`` dicts.
        """
        if not files:
            return
        if self._dry_run:
            log.info("[DRY-RUN] delete_replicas(%r, %d files)", rse, len(files))
            return

        client = self._make_client()

        def _call():
            client.delete_replicas(rse=rse, files=files)
            log.info("Deleted %d replica record(s) from %s", len(files), rse)

        self._retry(_call, "delete_replicas({!r}, {} files)".format(rse, len(files)))

    # ------------------------------------------------------------------
    # DID attachment
    # ------------------------------------------------------------------

    def attach_dids(self, scope, dataset_name, file_dids):
        # type: (str, str, List[dict]) -> None
        """
        Attach file DIDs to a dataset DID.

        The dataset must already exist (created by ``add_dataset``).

        Parameters
        ----------
        scope:
            Dataset scope.
        dataset_name:
            Dataset name component (no scope prefix).
        file_dids:
            List of ``{"scope": ..., "name": ...}`` dicts for each file.
        """
        if not file_dids:
            return
        if self._dry_run:
            log.info(
                "[DRY-RUN] attach_dids(%r:%r, %d files)",
                scope, dataset_name, len(file_dids),
            )
            return

        client = self._make_client()

        def _call():
            client.attach_dids(scope=scope, name=dataset_name, dids=file_dids)
            log.info(
                "Attached %d file(s) to dataset %s:%s",
                len(file_dids), scope, dataset_name,
            )

        self._retry(
            _call,
            "attach_dids({!r}:{!r}, {} files)".format(scope, dataset_name, len(file_dids)),
        )

    # ------------------------------------------------------------------
    # RSE checks
    # ------------------------------------------------------------------

    def assert_rse_deterministic(self, rse):
        # type: (str) -> None
        """
        Assert that *rse* is a deterministic RSE, raising ``RuntimeError`` if not.

        Deterministic RSEs derive the PFN from the LFN using a fixed path
        scheme — no explicit PFN should be passed to ``add_replicas``.
        Non-deterministic RSEs require an explicit PFN for each replica, which
        this tool does not support.

        The check is a single lightweight ``get_rse`` call and is performed
        once at startup (before any file generation) so the pipeline fails
        fast rather than after writing files.

        Parameters
        ----------
        rse:
            RSE name.

        Raises
        ------
        RuntimeError
            If the RSE is not deterministic or the query fails.
        """
        if self._dry_run:
            log.info("[DRY-RUN] Skipping RSE deterministic check for %r", rse)
            return

        client = self._make_client()

        def _call():
            info = client.get_rse(rse)
            if not info.get("deterministic", False):
                raise RuntimeError(
                    "RSE {!r} is not deterministic. This tool only supports "
                    "deterministic RSEs (PFN derived from LFN). "
                    "Non-deterministic RSEs require explicit PFNs which are "
                    "protocol-specific and not supported here.".format(rse)
                )
            log.debug("RSE %r confirmed deterministic", rse)

        self._retry(_call, "get_rse({!r})".format(rse))

    # ------------------------------------------------------------------
    # Auth check
    # ------------------------------------------------------------------

    def check_auth(self):
        # type: () -> dict
        """
        Verify OIDC token acquisition and Rucio server connectivity.

        1. Fetches a fresh OIDC token (raises ``RuntimeError`` on failure).
        2. Calls ``whoami()`` on the Rucio server — a lightweight, read-only
           endpoint that returns the authenticated account info dict.

        Returns
        -------
        dict
            Rucio account info, e.g.
            ``{"account": "robot", "status": "ACTIVE", ...}``.

        Raises
        ------
        RuntimeError
            If the OIDC token grant fails.
        Exception
            If the Rucio ``whoami()`` call fails after all retries.
        """
        log.debug("Fetching OIDC token …")
        self._get_token()   # raises on OIDC failure
        log.debug("OIDC token acquired — testing Rucio connectivity …")

        client = self._make_client()

        def _call():
            return client.whoami()

        info = self._retry(_call, "whoami()")
        log.info(
            "Auth OK: account=%r status=%r",
            info.get("account"), info.get("status"),
        )
        return info

    # ------------------------------------------------------------------
    # Replication rule
    # ------------------------------------------------------------------

    def add_replication_rule(self, scope, dataset_name, rse, copies=1, lifetime=None):
        # type: (str, str, str, int, Optional[int]) -> Optional[str]
        """
        Create a Rucio replication rule on the *dataset* DID.

        The rule is placed on the dataset (not per-file) so that Rucio
        manages the set as a unit.

        Parameters
        ----------
        scope:
            Dataset scope.
        dataset_name:
            Dataset name component (no scope prefix).
        rse:
            RSE expression for the rule (e.g. the RSE name itself).
        copies:
            Number of replica copies to maintain (default 1).
        lifetime:
            Optional rule lifetime in seconds.  ``None`` means no expiry.

        Returns
        -------
        str or None
            Rule ID string returned by Rucio, or ``None`` in dry-run mode.
        """
        if self._dry_run:
            log.info(
                "[DRY-RUN] add_replication_rule(%r:%r, copies=%d, rse=%r)",
                scope, dataset_name, copies, rse,
            )
            return None

        client = self._make_client()
        did = {"scope": scope, "name": dataset_name}

        def _call():
            try:
                rule_ids = client.add_replication_rule(
                    dids=[did],
                    copies=copies,
                    rse_expression=rse,
                    grouping="DATASET",
                    lifetime=lifetime,
                )
            except Exception as exc:
                if "DuplicateRule" in type(exc).__name__:
                    log.warning(
                        "Replication rule for %s:%s → %s already exists — skipping",
                        scope, dataset_name, rse,
                    )
                    return None
                raise
            rule_id = rule_ids[0] if isinstance(rule_ids, (list, tuple)) else rule_ids
            log.info(
                "Created replication rule %s on %s:%s → %s",
                rule_id, scope, dataset_name, rse,
            )
            return rule_id

        return self._retry(
            _call,
            "add_replication_rule({!r}:{!r})".format(scope, dataset_name),
        )


