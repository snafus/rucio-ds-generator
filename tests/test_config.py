"""
Tests for config.py — Config class construction, YAML+CLI merge, and validation.
"""

import os
import textwrap

import pytest
import yaml

from dataset_generator.config import Config, ConfigError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _minimal_kwargs(**overrides):
    """Return a dict of minimal valid Config constructor kwargs."""
    base = dict(
        scope="test_scope",
        rse="TEST_RSE",
        rse_mount="/tmp",          # /tmp always exists
        dataset_prefix="ds",
        file_prefix="file",
        num_files=2,
        file_size_bytes=1024,
        token_endpoint="https://iam.example.org/token",
        client_id="client",
        client_secret="secret",
        rucio_host="https://rucio.example.org",
        rucio_auth_host="https://rucio-auth.example.org",
        rucio_account="myaccount",
    )
    base.update(overrides)
    return base


def _make_config(**overrides):
    return Config(**_minimal_kwargs(**overrides))


# ---------------------------------------------------------------------------
# Construction
# ---------------------------------------------------------------------------

class TestConfigConstruction:
    def test_defaults_applied(self):
        cfg = _make_config()
        assert cfg.threads == 4
        assert cfg.log_level == "INFO"
        assert cfg.dry_run is False
        assert cfg.create_only is False
        assert cfg.register_only is False
        assert cfg.cleanup is False

    def test_run_id_auto_generated(self):
        cfg = _make_config()
        assert len(cfg.run_id) == 12
        assert cfg.run_id.isalnum()

    def test_run_id_explicit(self):
        cfg = _make_config(run_id="abc123def456")
        assert cfg.run_id == "abc123def456"

    def test_num_files_coerced_to_int(self):
        cfg = _make_config(num_files="5")
        assert cfg.num_files == 5
        assert isinstance(cfg.num_files, int)

    def test_file_size_bytes_coerced_to_int(self):
        cfg = _make_config(file_size_bytes="2048")
        assert cfg.file_size_bytes == 2048

    def test_log_level_uppercased(self):
        cfg = _make_config(log_level="debug")
        assert cfg.log_level == "DEBUG"

    def test_dry_run_bool_coercion(self):
        cfg = _make_config(dry_run=1)
        assert cfg.dry_run is True

    def test_rule_lifetime_none_by_default(self):
        cfg = _make_config()
        assert cfg.rule_lifetime is None

    def test_rule_lifetime_coerced_to_int(self):
        cfg = _make_config(rule_lifetime="604800")
        assert cfg.rule_lifetime == 604800
        assert isinstance(cfg.rule_lifetime, int)

    def test_rule_lifetime_none_stays_none(self):
        cfg = _make_config(rule_lifetime=None)
        assert cfg.rule_lifetime is None


# ---------------------------------------------------------------------------
# Computed properties
# ---------------------------------------------------------------------------

class TestComputedProperties:
    def test_dataset_name_format(self):
        cfg = _make_config(dataset_prefix="testds", run_id="aabbccddeeff")
        # Pattern: {prefix}_{YYYYMMDD}_{run_id}
        parts = cfg.dataset_name.split("_")
        assert parts[0] == "testds"
        assert len(parts[1]) == 8       # date portion
        assert parts[2] == "aabbccddeeff"

    def test_dataset_did_format(self):
        cfg = _make_config(scope="cms", run_id="aabbccddeeff")
        assert cfg.dataset_did.startswith("cms:")
        assert cfg.dataset_did == "cms:{}".format(cfg.dataset_name)

    def test_state_file_path_default(self):
        cfg = _make_config(run_id="abc123")
        assert cfg.state_file_path == "state_abc123.json"

    def test_state_file_path_override(self):
        cfg = _make_config(state_file="/tmp/my_state.json")
        assert cfg.state_file_path == "/tmp/my_state.json"


# ---------------------------------------------------------------------------
# from_yaml_and_args factory
# ---------------------------------------------------------------------------

class FakeArgs(object):
    """Minimal stand-in for argparse.Namespace."""

    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)


class TestFromYamlAndArgs:
    def test_yaml_only(self, tmp_path):
        yaml_content = textwrap.dedent("""\
            scope: cms
            rse: T2_US_TEST
            rse_mount: /tmp
            dataset_prefix: ds
            file_prefix: file
            num_files: 3
            file_size_bytes: 512
            token_endpoint: https://iam.example.org/token
            client_id: cid
            client_secret: csecret
            rucio_host: https://rucio.example.org
            rucio_auth_host: https://rucio-auth.example.org
            rucio_account: myacct
        """)
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text(yaml_content)

        args = FakeArgs()  # no attributes set
        cfg = Config.from_yaml_and_args(str(yaml_file), args)

        assert cfg.scope == "cms"
        assert cfg.rse == "T2_US_TEST"
        assert cfg.num_files == 3
        assert cfg.threads == 4        # default
        assert cfg.dry_run is False    # default

    def test_cli_overrides_yaml(self, tmp_path):
        yaml_content = textwrap.dedent("""\
            scope: from_yaml
            rse: T2_US_TEST
            rse_mount: /tmp
            dataset_prefix: ds
            file_prefix: file
            num_files: 3
            file_size_bytes: 512
            token_endpoint: https://iam.example.org/token
            client_id: cid
            client_secret: csecret
            rucio_host: https://rucio.example.org
            rucio_auth_host: https://rucio-auth.example.org
            rucio_account: myacct
            threads: 2
        """)
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text(yaml_content)

        args = FakeArgs(scope="from_cli", threads=8)
        cfg = Config.from_yaml_and_args(str(yaml_file), args)

        assert cfg.scope == "from_cli"   # CLI wins
        assert cfg.threads == 8          # CLI wins over YAML 2
        assert cfg.rse == "T2_US_TEST"   # from YAML (not overridden)

    def test_required_key_missing_raises(self, tmp_path):
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text("scope: cms\n")  # missing almost everything

        # 'scope' is present; the first missing required key is 'rse'
        with pytest.raises(ConfigError, match="rse"):
            Config.from_yaml_and_args(str(yaml_file), FakeArgs())

    def test_no_yaml_all_from_cli(self):
        args = FakeArgs(**_minimal_kwargs())
        cfg = Config.from_yaml_and_args(None, args)
        assert cfg.scope == "test_scope"

    def test_yaml_none_values_ignored(self, tmp_path):
        yaml_content = textwrap.dedent("""\
            scope: cms
            rse: ~
            rse_mount: /tmp
            dataset_prefix: ds
            file_prefix: file
            num_files: 1
            file_size_bytes: 512
            token_endpoint: https://iam.example.org/token
            client_id: cid
            client_secret: csecret
            rucio_host: https://rucio.example.org
            rucio_auth_host: https://rucio-auth.example.org
            rucio_account: myacct
        """)
        yaml_file = tmp_path / "config.yaml"
        yaml_file.write_text(yaml_content)

        # rse=None in YAML; CLI supplies it
        args = FakeArgs(rse="T2_US_CLI")
        cfg = Config.from_yaml_and_args(str(yaml_file), args)
        assert cfg.rse == "T2_US_CLI"


# ---------------------------------------------------------------------------
# Validation
# ---------------------------------------------------------------------------

class TestValidation:
    def test_valid_config_passes(self):
        cfg = _make_config()
        cfg.validate()  # should not raise

    def test_create_only_and_register_only_raises(self):
        cfg = _make_config(create_only=True, register_only=True)
        with pytest.raises(ConfigError, match="mutually exclusive"):
            cfg.validate()

    def test_cleanup_and_create_only_raises(self):
        cfg = _make_config(cleanup=True, create_only=True)
        with pytest.raises(ConfigError, match="--cleanup"):
            cfg.validate()

    def test_threads_zero_raises(self):
        cfg = _make_config(threads=0)
        with pytest.raises(ConfigError, match="threads"):
            cfg.validate()

    def test_num_files_zero_raises(self):
        cfg = _make_config(num_files=0)
        with pytest.raises(ConfigError, match="num_files"):
            cfg.validate()

    def test_file_size_zero_raises(self):
        cfg = _make_config(file_size_bytes=0)
        with pytest.raises(ConfigError, match="file_size_bytes"):
            cfg.validate()

    def test_rule_lifetime_zero_raises(self):
        cfg = _make_config(rule_lifetime=0)
        with pytest.raises(ConfigError, match="rule_lifetime"):
            cfg.validate()

    def test_rule_lifetime_negative_raises(self):
        cfg = _make_config(rule_lifetime=-1)
        with pytest.raises(ConfigError, match="rule_lifetime"):
            cfg.validate()

    def test_rule_lifetime_positive_passes(self):
        cfg = _make_config(rule_lifetime=86400)
        cfg.validate()  # should not raise

    def test_missing_rse_mount_raises(self):
        cfg = _make_config(rse_mount="/nonexistent_path_xyz")
        with pytest.raises(ConfigError, match="rse_mount"):
            cfg.validate()

    def test_dry_run_skips_mount_check(self):
        cfg = _make_config(rse_mount="/nonexistent_path_xyz", dry_run=True)
        cfg.validate()  # should not raise — dry_run skips mount check


# ---------------------------------------------------------------------------
# Repr
# ---------------------------------------------------------------------------

class TestRepr:
    def test_repr_contains_key_fields(self):
        cfg = _make_config(run_id="abc123")
        r = repr(cfg)
        assert "abc123" in r
        assert "test_scope" in r
        assert "TEST_RSE" in r
