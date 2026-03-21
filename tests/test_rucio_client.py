"""
Tests for rucio_client.py — OIDC token management, Rucio API calls,
retry logic, and dry-run behaviour.

Rucio's Python client and the OIDC endpoint are mocked throughout so no
real network calls are made.
"""

import time
from unittest.mock import MagicMock, patch, call

import pytest
import requests

from dataset_generator.config import Config
from dataset_generator.rucio_client import (
    MAX_RETRIES,
    RETRY_DELAYS,
    RucioManager,
    _OIDCToken,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def config():
    return Config(
        scope="test",
        rse="TEST_RSE",
        rse_mount="/tmp",
        dataset_prefix="ds",
        file_prefix="file",
        num_files=2,
        file_size_bytes=512,
        token_endpoint="https://iam.example.org/token",
        client_id="cid",
        client_secret="csec",
        rucio_host="https://rucio.example.org",
        rucio_auth_host="https://rucio-auth.example.org",
        rucio_account="acct",
        run_id="test000000",
    )


def _make_token_response(access_token="mytoken123", expires_in=3600):
    """Fake requests.Response for a successful token endpoint."""
    resp = MagicMock()
    resp.raise_for_status.return_value = None
    resp.json.return_value = {"access_token": access_token, "expires_in": expires_in}
    return resp


def _make_manager(config, **kwargs):
    return RucioManager(config=config, **kwargs)


# ---------------------------------------------------------------------------
# _OIDCToken
# ---------------------------------------------------------------------------

class TestOIDCToken:
    def test_is_fresh_within_window(self):
        token = _OIDCToken("tok", time.time() + 60)
        assert token.is_fresh() is True

    def test_is_fresh_expired(self):
        token = _OIDCToken("tok", time.time() - 1)
        assert token.is_fresh() is False

    def test_is_fresh_at_boundary(self):
        # expires_at == now — considered expired
        token = _OIDCToken("tok", time.time())
        assert token.is_fresh() is False


# ---------------------------------------------------------------------------
# OIDC token acquisition
# ---------------------------------------------------------------------------

class TestTokenAcquisition:
    @patch("dataset_generator.rucio_client.requests.post")
    def test_fetch_token_calls_endpoint(self, mock_post, config):
        mock_post.return_value = _make_token_response()
        manager = _make_manager(config)
        token = manager._get_token()

        mock_post.assert_called_once_with(
            config.token_endpoint,
            data={
                "grant_type": "client_credentials",
                "client_id": config.client_id,
                "client_secret": config.client_secret,
            },
            timeout=30,
        )
        assert token == "mytoken123"

    @patch("dataset_generator.rucio_client.requests.post")
    def test_token_cached_second_call(self, mock_post, config):
        mock_post.return_value = _make_token_response()
        manager = _make_manager(config)
        manager._get_token()
        manager._get_token()
        assert mock_post.call_count == 1   # only one HTTP call

    @patch("dataset_generator.rucio_client.requests.post")
    def test_expired_token_is_refreshed(self, mock_post, config):
        mock_post.return_value = _make_token_response(expires_in=0)
        manager = _make_manager(config)
        manager._get_token()
        # Token is immediately stale (expires_in=0 → expires_at ≈ now - 30)
        manager._get_token()
        assert mock_post.call_count == 2

    @patch("dataset_generator.rucio_client.requests.post")
    def test_http_error_raises_runtime_error(self, mock_post, config):
        mock_post.side_effect = requests.ConnectionError("refused")
        manager = _make_manager(config)
        with pytest.raises(RuntimeError, match="OIDC token request"):
            manager._get_token()

    @patch("dataset_generator.rucio_client.requests.post")
    def test_missing_access_token_raises(self, mock_post, config):
        resp = MagicMock()
        resp.raise_for_status.return_value = None
        resp.json.return_value = {"token_type": "Bearer"}   # no access_token
        mock_post.return_value = resp
        manager = _make_manager(config)
        with pytest.raises(RuntimeError, match="Malformed token response"):
            manager._get_token()

    @patch("dataset_generator.rucio_client.requests.post")
    def test_expires_at_has_30s_buffer(self, mock_post, config):
        mock_post.return_value = _make_token_response(expires_in=3600)
        manager = _make_manager(config)
        before = time.time()
        manager._get_token()
        after = time.time()
        # expires_at should be roughly (now + 3600 - 30)
        expected_min = before + 3600 - 30 - 1
        expected_max = after + 3600 - 30 + 1
        assert expected_min <= manager._token.expires_at <= expected_max


# ---------------------------------------------------------------------------
# Rucio client construction
# ---------------------------------------------------------------------------

class TestMakeClient:
    @patch("dataset_generator.rucio_client.requests.post")
    def test_auth_token_set_on_client(self, mock_post, config):
        mock_post.return_value = _make_token_response(access_token="bearerXYZ")

        mock_client_instance = MagicMock()
        mock_client_class = MagicMock(return_value=mock_client_instance)

        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client", mock_client_class):
            client = manager._make_client()

        assert client.auth_token == "bearerXYZ"


# ---------------------------------------------------------------------------
# Retry wrapper
# ---------------------------------------------------------------------------

class TestRetry:
    def test_no_retry_on_success(self, config):
        manager = _make_manager(config)
        calls = [0]

        def fn():
            calls[0] += 1
            return "ok"

        result = manager._retry(fn, "test-op")
        assert result == "ok"
        assert calls[0] == 1

    @patch("dataset_generator.rucio_client.time.sleep")
    def test_retries_on_requests_exception(self, mock_sleep, config):
        manager = _make_manager(config)
        calls = [0]

        def fn():
            calls[0] += 1
            if calls[0] < MAX_RETRIES:
                raise requests.ConnectionError("flaky")
            return "recovered"

        result = manager._retry(fn, "test-op")
        assert result == "recovered"
        assert calls[0] == MAX_RETRIES
        assert mock_sleep.call_count == MAX_RETRIES - 1

    @patch("dataset_generator.rucio_client.time.sleep")
    def test_all_retries_exhausted_raises(self, mock_sleep, config):
        manager = _make_manager(config)

        def fn():
            raise requests.ConnectionError("always fails")

        with pytest.raises(requests.ConnectionError):
            manager._retry(fn, "fail-op")

        assert mock_sleep.call_count == MAX_RETRIES - 1

    def test_non_retryable_exception_propagates_immediately(self, config):
        manager = _make_manager(config)
        calls = [0]

        def fn():
            calls[0] += 1
            raise ValueError("logic error")

        with pytest.raises(ValueError, match="logic error"):
            manager._retry(fn, "bad-op")

        assert calls[0] == 1   # no retries

    @patch("dataset_generator.rucio_client.time.sleep")
    def test_retry_delays_are_exponential(self, mock_sleep, config):
        manager = _make_manager(config)

        def fn():
            raise requests.ConnectionError("always")

        with pytest.raises(requests.ConnectionError):
            manager._retry(fn, "delay-test")

        delays = [c[0][0] for c in mock_sleep.call_args_list]  # c[0] = positional args tuple; .args only exists in Python 3.8+
        assert delays == list(RETRY_DELAYS[: MAX_RETRIES - 1])


# ---------------------------------------------------------------------------
# add_dataset
# ---------------------------------------------------------------------------

class TestAddDataset:
    @patch("dataset_generator.rucio_client.requests.post")
    def test_calls_rucio_add_dataset(self, mock_post, config):
        mock_post.return_value = _make_token_response()
        mock_client = MagicMock()
        manager = _make_manager(config)

        with patch("dataset_generator.rucio_client.Client", return_value=mock_client):
            manager.add_dataset("cms", "my_dataset")

        mock_client.add_dataset.assert_called_once_with(scope="cms", name="my_dataset")

    def test_dry_run_does_not_call_rucio(self, config):
        manager = _make_manager(config, dry_run=True)
        with patch("dataset_generator.rucio_client.Client") as mock_cls:
            manager.add_dataset("cms", "ds")
            mock_cls.assert_not_called()

    @patch("dataset_generator.rucio_client.requests.post")
    def test_already_exists_does_not_raise(self, mock_post, config):
        mock_post.return_value = _make_token_response()
        mock_client = MagicMock()

        class FakeAlreadyExists(Exception):
            pass
        FakeAlreadyExists.__name__ = "DataIdentifierAlreadyExists"

        mock_client.add_dataset.side_effect = FakeAlreadyExists("exists")
        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client", return_value=mock_client):
            manager.add_dataset("cms", "ds")  # should NOT raise


# ---------------------------------------------------------------------------
# add_replicas
# ---------------------------------------------------------------------------

class TestAddReplicas:
    @patch("dataset_generator.rucio_client.requests.post")
    def test_calls_rucio_with_full_batch(self, mock_post, config):
        mock_post.return_value = _make_token_response()
        mock_client = MagicMock()
        files = [
            {"scope": "test", "name": "f1", "bytes": 100, "adler32": "aabbccdd"},
            {"scope": "test", "name": "f2", "bytes": 200, "adler32": "11223344"},
        ]
        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client", return_value=mock_client):
            manager.add_replicas("TEST_RSE", files)

        mock_client.add_replicas.assert_called_once_with(rse="TEST_RSE", files=files)

    def test_empty_list_is_no_op(self, config):
        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client") as mock_cls:
            manager.add_replicas("TEST_RSE", [])
            mock_cls.assert_not_called()

    def test_dry_run_skips_call(self, config):
        manager = _make_manager(config, dry_run=True)
        with patch("dataset_generator.rucio_client.Client") as mock_cls:
            manager.add_replicas("TEST_RSE", [{"scope": "s", "name": "n", "bytes": 1, "adler32": "x"}])
            mock_cls.assert_not_called()


# ---------------------------------------------------------------------------
# delete_replicas
# ---------------------------------------------------------------------------

class TestDeleteReplicas:
    @patch("dataset_generator.rucio_client.requests.post")
    def test_calls_rucio_delete_replicas(self, mock_post, config):
        mock_post.return_value = _make_token_response()
        mock_client = MagicMock()
        files = [{"scope": "test", "name": "f1"}, {"scope": "test", "name": "f2"}]
        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client", return_value=mock_client):
            manager.delete_replicas("TEST_RSE", files)
        mock_client.delete_replicas.assert_called_once_with(rse="TEST_RSE", files=files)

    def test_dry_run_skips_call(self, config):
        manager = _make_manager(config, dry_run=True)
        with patch("dataset_generator.rucio_client.Client") as mock_cls:
            manager.delete_replicas("TEST_RSE", [{"scope": "s", "name": "n"}])
            mock_cls.assert_not_called()


# ---------------------------------------------------------------------------
# attach_dids
# ---------------------------------------------------------------------------

class TestAttachDids:
    @patch("dataset_generator.rucio_client.requests.post")
    def test_calls_rucio_attach_dids(self, mock_post, config):
        mock_post.return_value = _make_token_response()
        mock_client = MagicMock()
        dids = [{"scope": "test", "name": "f1"}, {"scope": "test", "name": "f2"}]
        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client", return_value=mock_client):
            manager.attach_dids("test", "my_ds", dids)

        mock_client.attach_dids.assert_called_once_with(
            scope="test", name="my_ds", dids=dids
        )

    def test_empty_dids_is_no_op(self, config):
        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client") as mock_cls:
            manager.attach_dids("test", "ds", [])
            mock_cls.assert_not_called()

    def test_dry_run_skips_call(self, config):
        manager = _make_manager(config, dry_run=True)
        with patch("dataset_generator.rucio_client.Client") as mock_cls:
            manager.attach_dids("test", "ds", [{"scope": "s", "name": "n"}])
            mock_cls.assert_not_called()


# ---------------------------------------------------------------------------
# add_replication_rule — called on DATASET DID
# ---------------------------------------------------------------------------

class TestAddReplicationRule:
    @patch("dataset_generator.rucio_client.requests.post")
    def test_calls_rule_on_dataset_did(self, mock_post, config):
        mock_post.return_value = _make_token_response()
        mock_client = MagicMock()
        mock_client.add_replication_rule.return_value = ["rule-id-001"]

        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client", return_value=mock_client):
            rule_id = manager.add_replication_rule("test", "my_ds", "TEST_RSE")

        # Verify the rule is placed on the dataset DID, not per-file DIDs
        mock_client.add_replication_rule.assert_called_once_with(
            dids=[{"scope": "test", "name": "my_ds"}],
            copies=1,
            rse_expression="TEST_RSE",
            grouping="DATASET",
            lifetime=None,
        )
        assert rule_id == "rule-id-001"

    @patch("dataset_generator.rucio_client.requests.post")
    def test_handles_scalar_rule_id_return(self, mock_post, config):
        """Some Rucio versions may return a string instead of a list."""
        mock_post.return_value = _make_token_response()
        mock_client = MagicMock()
        mock_client.add_replication_rule.return_value = "rule-id-scalar"
        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client", return_value=mock_client):
            rule_id = manager.add_replication_rule("test", "my_ds", "TEST_RSE")
        assert rule_id == "rule-id-scalar"

    @patch("dataset_generator.rucio_client.requests.post")
    def test_lifetime_forwarded_to_rucio(self, mock_post, config):
        mock_post.return_value = _make_token_response()
        mock_client = MagicMock()
        mock_client.add_replication_rule.return_value = ["rule-id-ltm"]
        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client", return_value=mock_client):
            manager.add_replication_rule("test", "my_ds", "TEST_RSE", lifetime=604800)

        _, kwargs = mock_client.add_replication_rule.call_args
        assert kwargs["lifetime"] == 604800

    @patch("dataset_generator.rucio_client.requests.post")
    def test_none_lifetime_forwarded_as_none(self, mock_post, config):
        mock_post.return_value = _make_token_response()
        mock_client = MagicMock()
        mock_client.add_replication_rule.return_value = ["rule-id-perm"]
        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client", return_value=mock_client):
            manager.add_replication_rule("test", "my_ds", "TEST_RSE", lifetime=None)

        _, kwargs = mock_client.add_replication_rule.call_args
        assert kwargs["lifetime"] is None

    def test_dry_run_returns_none(self, config):
        manager = _make_manager(config, dry_run=True)
        with patch("dataset_generator.rucio_client.Client") as mock_cls:
            result = manager.add_replication_rule("test", "ds", "RSE")
            assert result is None
            mock_cls.assert_not_called()

    @patch("dataset_generator.rucio_client.requests.post")
    def test_duplicate_rule_returns_none_with_warning(self, mock_post, config):
        """DuplicateRule on a static dataset should warn, not fail."""
        mock_post.return_value = _make_token_response()

        class DuplicateRule(Exception):
            pass

        mock_client = MagicMock()
        mock_client.add_replication_rule.side_effect = DuplicateRule("duplicate")

        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client", return_value=mock_client):
            result = manager.add_replication_rule("test", "my_ds", "TEST_RSE")

        assert result is None   # not an error


# ---------------------------------------------------------------------------
# lfns2pfn
# ---------------------------------------------------------------------------

class TestLfns2pfn:
    @patch("dataset_generator.rucio_client.requests.post")
    def test_resolves_single_lfn(self, mock_post, config):
        mock_post.return_value = _make_token_response()
        mock_client = MagicMock()
        mock_client.lfns2pfns.return_value = {"test:myfile": "/mnt/rse/test/myfile"}

        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client", return_value=mock_client):
            pfn = manager.lfns2pfn("TEST_RSE", "test:myfile")

        mock_client.lfns2pfns.assert_called_once_with(rse="TEST_RSE", lfns=["test:myfile"])
        assert pfn == "/mnt/rse/test/myfile"

    @patch("dataset_generator.rucio_client.requests.post")
    def test_missing_lfn_in_response_raises(self, mock_post, config):
        mock_post.return_value = _make_token_response()
        mock_client = MagicMock()
        mock_client.lfns2pfns.return_value = {}   # empty response

        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client", return_value=mock_client):
            with pytest.raises(RuntimeError, match="lfns2pfns returned no entry"):
                manager.lfns2pfn("TEST_RSE", "test:myfile")


# ---------------------------------------------------------------------------
# check_auth
# ---------------------------------------------------------------------------

class TestCheckAuth:
    @patch("dataset_generator.rucio_client.requests.post")
    def test_returns_whoami_dict(self, mock_post, config):
        mock_post.return_value = _make_token_response()
        mock_client = MagicMock()
        mock_client.whoami.return_value = {
            "account": "robot",
            "status": "ACTIVE",
            "account_type": "SERVICE",
        }

        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client", return_value=mock_client):
            result = manager.check_auth()

        mock_client.whoami.assert_called_once_with()
        assert result["account"] == "robot"
        assert result["status"] == "ACTIVE"

    @patch("dataset_generator.rucio_client.requests.post")
    def test_oidc_failure_raises(self, mock_post, config):
        mock_post.side_effect = requests.RequestException("connection refused")

        manager = _make_manager(config)
        with pytest.raises(RuntimeError, match="OIDC token request"):
            manager.check_auth()

    @patch("dataset_generator.rucio_client.requests.post")
    def test_rucio_failure_raises(self, mock_post, config):
        mock_post.return_value = _make_token_response()
        mock_client = MagicMock()
        mock_client.whoami.side_effect = Exception("ServiceUnavailable")

        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client", return_value=mock_client):
            with pytest.raises(Exception, match="ServiceUnavailable"):
                manager.check_auth()


# ---------------------------------------------------------------------------
# assert_rse_deterministic
# ---------------------------------------------------------------------------

class TestAssertRseDeterministic:
    @patch("dataset_generator.rucio_client.requests.post")
    def test_passes_for_deterministic_rse(self, mock_post, config):
        mock_post.return_value = _make_token_response()
        mock_client = MagicMock()
        mock_client.get_rse.return_value = {"deterministic": True, "rse": "TEST_RSE"}

        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client", return_value=mock_client):
            manager.assert_rse_deterministic("TEST_RSE")  # must not raise

        mock_client.get_rse.assert_called_once_with("TEST_RSE")

    @patch("dataset_generator.rucio_client.requests.post")
    def test_raises_for_non_deterministic_rse(self, mock_post, config):
        mock_post.return_value = _make_token_response()
        mock_client = MagicMock()
        mock_client.get_rse.return_value = {"deterministic": False, "rse": "TEST_RSE"}

        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client", return_value=mock_client):
            with pytest.raises(RuntimeError, match="not deterministic"):
                manager.assert_rse_deterministic("TEST_RSE")

    def test_dry_run_skips_check(self, config):
        manager = _make_manager(config, dry_run=True)
        with patch("dataset_generator.rucio_client.Client") as mock_cls:
            manager.assert_rse_deterministic("TEST_RSE")
            mock_cls.assert_not_called()


# ---------------------------------------------------------------------------
# TestAddContainer
# ---------------------------------------------------------------------------

class TestAddContainer:
    @patch("dataset_generator.rucio_client.requests.post")
    def test_creates_container(self, mock_post, config):
        mock_post.return_value = _make_token_response()
        mock_client = MagicMock()
        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client", return_value=mock_client):
            manager.add_container("test", "my_container")
        mock_client.add_container.assert_called_once_with(scope="test", name="my_container")

    @patch("dataset_generator.rucio_client.requests.post")
    def test_already_exists_does_not_raise(self, mock_post, config):
        mock_post.return_value = _make_token_response()
        mock_client = MagicMock()

        class DataIdentifierAlreadyExists(Exception):
            pass

        mock_client.add_container.side_effect = DataIdentifierAlreadyExists("exists")
        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client", return_value=mock_client):
            manager.add_container("test", "my_container")  # must not raise

    def test_dry_run_skips_call(self, config):
        manager = _make_manager(config, dry_run=True)
        with patch("dataset_generator.rucio_client.Client") as mock_cls:
            manager.add_container("test", "my_container")
            mock_cls.assert_not_called()


# ---------------------------------------------------------------------------
# TestAttachDatasetToContainer
# ---------------------------------------------------------------------------

class TestAttachDatasetToContainer:
    @patch("dataset_generator.rucio_client.requests.post")
    def test_attaches_dataset(self, mock_post, config):
        mock_post.return_value = _make_token_response()
        mock_client = MagicMock()
        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client", return_value=mock_client):
            manager.attach_dataset_to_container("test", "my_container", "my_dataset")
        mock_client.attach_dids.assert_called_once_with(
            scope="test",
            name="my_container",
            dids=[{"scope": "test", "name": "my_dataset"}],
        )

    @patch("dataset_generator.rucio_client.requests.post")
    def test_duplicate_content_does_not_raise(self, mock_post, config):
        mock_post.return_value = _make_token_response()
        mock_client = MagicMock()

        class DuplicateContent(Exception):
            pass

        mock_client.attach_dids.side_effect = DuplicateContent("already attached")
        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client", return_value=mock_client):
            # must not raise
            manager.attach_dataset_to_container("test", "my_container", "my_dataset")

    def test_dry_run_skips_call(self, config):
        manager = _make_manager(config, dry_run=True)
        with patch("dataset_generator.rucio_client.Client") as mock_cls:
            manager.attach_dataset_to_container("test", "my_container", "my_dataset")
            mock_cls.assert_not_called()

    @patch("dataset_generator.rucio_client.requests.post")
    def test_other_exception_propagates(self, mock_post, config):
        mock_post.return_value = _make_token_response()
        mock_client = MagicMock()
        mock_client.attach_dids.side_effect = RuntimeError("unexpected")
        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client", return_value=mock_client):
            with pytest.raises(RuntimeError, match="unexpected"):
                manager.attach_dataset_to_container("test", "my_container", "my_dataset")


# ---------------------------------------------------------------------------
# TestCountDatasetFiles
# ---------------------------------------------------------------------------

class TestCountDatasetFiles:
    @patch("dataset_generator.rucio_client.requests.post")
    def test_counts_files_in_dataset(self, mock_post, config):
        mock_post.return_value = _make_token_response()
        mock_client = MagicMock()
        mock_client.list_files.return_value = iter([{"name": "f1"}, {"name": "f2"}, {"name": "f3"}])
        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client", return_value=mock_client):
            count = manager.count_dataset_files("cms", "my_dataset")
        assert count == 3
        mock_client.list_files.assert_called_once_with(scope="cms", name="my_dataset")

    @patch("dataset_generator.rucio_client.requests.post")
    def test_returns_zero_for_nonexistent_dataset(self, mock_post, config):
        mock_post.return_value = _make_token_response()
        mock_client = MagicMock()

        class DataIdentifierNotFound(Exception):
            pass

        mock_client.list_files.side_effect = DataIdentifierNotFound("no such DID")
        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client", return_value=mock_client):
            count = manager.count_dataset_files("cms", "missing_dataset")
        assert count == 0

    def test_dry_run_returns_zero(self, config):
        manager = _make_manager(config, dry_run=True)
        with patch("dataset_generator.rucio_client.Client") as mock_cls:
            count = manager.count_dataset_files("cms", "my_dataset")
            mock_cls.assert_not_called()
        assert count == 0

    @patch("dataset_generator.rucio_client.requests.post")
    def test_empty_dataset_returns_zero(self, mock_post, config):
        mock_post.return_value = _make_token_response()
        mock_client = MagicMock()
        mock_client.list_files.return_value = iter([])
        manager = _make_manager(config)
        with patch("dataset_generator.rucio_client.Client", return_value=mock_client):
            count = manager.count_dataset_files("cms", "empty_dataset")
        assert count == 0
