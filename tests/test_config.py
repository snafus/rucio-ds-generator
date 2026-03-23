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
        assert cfg.pool_chunksize == 0

    def test_pool_chunksize_default_is_zero(self):
        cfg = _make_config()
        assert cfg.pool_chunksize == 0

    def test_pool_chunksize_explicit_stored(self):
        cfg = _make_config(pool_chunksize=25)
        assert cfg.pool_chunksize == 25

    def test_pool_chunksize_coerced_to_int(self):
        cfg = _make_config(pool_chunksize="10")
        assert cfg.pool_chunksize == 10
        assert isinstance(cfg.pool_chunksize, int)

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


class TestEnvVarLookup:
    """Environment variables supply credentials between CLI and YAML in precedence."""

    def test_env_var_supplies_missing_value(self, monkeypatch, tmp_path):
        yaml_content = (
            "scope: cms\nrse: T2\nrse_mount: /tmp\ndataset_prefix: ds\n"
            "file_prefix: f\nnum_files: 1\nfile_size_bytes: 512\n"
            "token_endpoint: https://iam/token\nclient_id: cid\nclient_secret: s\n"
            "rucio_host: https://rucio\nrucio_auth_host: https://rucio-auth\n"
            "rucio_account: fromyaml\n"
        )
        yaml_file = tmp_path / "c.yaml"
        yaml_file.write_text(yaml_content)
        monkeypatch.setenv("RUCIO_ACCOUNT", "from_env")
        cfg = Config.from_yaml_and_args(str(yaml_file), FakeArgs())
        assert cfg.rucio_account == "from_env"   # env wins over YAML

    def test_cli_beats_env_var(self, monkeypatch, tmp_path):
        yaml_content = (
            "scope: cms\nrse: T2\nrse_mount: /tmp\ndataset_prefix: ds\n"
            "file_prefix: f\nnum_files: 1\nfile_size_bytes: 512\n"
            "token_endpoint: https://iam/token\nclient_id: cid\nclient_secret: s\n"
            "rucio_host: https://rucio\nrucio_auth_host: https://rucio-auth\n"
            "rucio_account: fromyaml\n"
        )
        yaml_file = tmp_path / "c.yaml"
        yaml_file.write_text(yaml_content)
        monkeypatch.setenv("RUCIO_ACCOUNT", "from_env")
        cfg = Config.from_yaml_and_args(str(yaml_file), FakeArgs(rucio_account="from_cli"))
        assert cfg.rucio_account == "from_cli"   # CLI beats env var

    def test_oidc_env_vars_mapped(self, monkeypatch, tmp_path):
        yaml_content = (
            "scope: cms\nrse: T2\nrse_mount: /tmp\ndataset_prefix: ds\n"
            "file_prefix: f\nnum_files: 1\nfile_size_bytes: 512\n"
            "rucio_host: https://rucio\nrucio_auth_host: https://rucio-auth\n"
            "rucio_account: acct\n"
        )
        yaml_file = tmp_path / "c.yaml"
        yaml_file.write_text(yaml_content)
        monkeypatch.setenv("OIDC_TOKEN_ENDPOINT", "https://env-iam/token")
        monkeypatch.setenv("OIDC_CLIENT_ID", "env-cid")
        monkeypatch.setenv("OIDC_CLIENT_SECRET", "env-secret")
        cfg = Config.from_yaml_and_args(str(yaml_file), FakeArgs())
        assert cfg.token_endpoint == "https://env-iam/token"
        assert cfg.client_id == "env-cid"
        assert cfg.client_secret == "env-secret"

    def test_error_message_names_env_var(self, tmp_path):
        yaml_file = tmp_path / "c.yaml"
        yaml_file.write_text("scope: cms\n")
        with pytest.raises(ConfigError, match="RUCIO_HOST"):
            Config.from_yaml_and_args(str(yaml_file), FakeArgs(scope="cms", rse="R",
                rse_mount="/tmp", dataset_prefix="d", file_prefix="f",
                num_files=1, file_size_bytes=512, rucio_account="a",
                rucio_auth_host="h", token_endpoint="t",
                client_id="c", client_secret="s"))


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

    def test_buffer_reuse_ring_size_too_small_raises(self):
        cfg = _make_config(generation_mode="buffer-reuse", buffer_reuse_ring_size=1024)
        with pytest.raises(ConfigError, match="buffer_reuse_ring_size"):
            cfg.validate()

    def test_buffer_reuse_ring_size_valid_passes(self):
        cfg = _make_config(generation_mode="buffer-reuse",
                           buffer_reuse_ring_size=128 * 1024 * 1024)
        cfg.validate()  # should not raise

    def test_buffer_reuse_ring_size_not_checked_for_csprng_mode(self):
        # A bad ring size is irrelevant when mode is csprng — must not raise.
        cfg = _make_config(generation_mode="csprng", buffer_reuse_ring_size=1024)
        cfg.validate()  # should not raise

    def test_missing_rse_mount_raises(self):
        cfg = _make_config(rse_mount="/nonexistent_path_xyz")
        with pytest.raises(ConfigError, match="rse_mount"):
            cfg.validate()

    def test_dry_run_skips_mount_check(self):
        cfg = _make_config(rse_mount="/nonexistent_path_xyz", dry_run=True)
        cfg.validate()  # should not raise — dry_run skips mount check

    def test_pool_chunksize_negative_raises(self):
        cfg = _make_config(pool_chunksize=-1)
        with pytest.raises(ConfigError, match="pool_chunksize"):
            cfg.validate()

    def test_pool_chunksize_zero_passes(self):
        cfg = _make_config(pool_chunksize=0)
        cfg.validate()  # 0 means auto-compute — must not raise

    def test_pool_chunksize_positive_passes(self):
        cfg = _make_config(pool_chunksize=50)
        cfg.validate()  # should not raise


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


# ---------------------------------------------------------------------------
# Static dataset name
# ---------------------------------------------------------------------------

class TestDatasetName:
    def test_dynamic_name_includes_prefix_date_run_id(self):
        cfg = _make_config(dataset_prefix="myds", run_id="aabbccddeeff")
        parts = cfg.dataset_name.split("_")
        assert parts[0] == "myds"
        assert len(parts[1]) == 8   # YYYYMMDD
        assert parts[2] == "aabbccddeeff"

    def test_static_name_returned_verbatim(self):
        cfg = _make_config(dataset_name="my_fixed_dataset")
        assert cfg.dataset_name == "my_fixed_dataset"

    def test_static_name_in_dataset_did(self):
        cfg = _make_config(scope="cms", dataset_name="my_fixed_dataset")
        assert cfg.dataset_did == "cms:my_fixed_dataset"

    def test_static_name_makes_dataset_prefix_optional(self):
        # dataset_prefix may be absent when dataset_name is set
        from dataset_generator.config import Config
        import textwrap, tempfile, os
        cfg = Config.from_yaml_and_args(None, FakeArgs(
            scope="cms", rse="RSE", rse_mount="/tmp",
            file_prefix="f", num_files=1, file_size_bytes=512,
            token_endpoint="https://t", client_id="c", client_secret="s",
            rucio_host="https://r", rucio_auth_host="https://ra",
            rucio_account="a", dataset_name="fixed",
        ))
        assert cfg.dataset_name == "fixed"


# ---------------------------------------------------------------------------
# TestNamingTemplates
# ---------------------------------------------------------------------------

class TestNamingTemplates:
    """Jinja2 template rendering for dataset_name, file_prefix, container_name."""

    def _cfg(self, **kwargs):
        defaults = dict(
            scope="cms", rse="T2_US_TEST", rse_mount="/tmp",
            dataset_prefix="ds", file_prefix="file",
            num_files=2, file_size_bytes=1073741824,
            token_endpoint="https://t", client_id="c", client_secret="s",
            rucio_host="https://r", rucio_auth_host="https://ra",
            rucio_account="a", run_id="aabbccddeeff",
        )
        defaults.update(kwargs)
        return Config(**defaults)

    # ------------------------------------------------------------------
    # Plain strings pass through unchanged
    # ------------------------------------------------------------------

    def test_plain_file_prefix_unchanged(self):
        cfg = self._cfg(file_prefix="testfile")
        assert cfg.file_prefix == "testfile"

    def test_plain_dataset_name_unchanged(self):
        cfg = self._cfg(dataset_name="my_dataset")
        assert cfg.dataset_name == "my_dataset"

    def test_plain_container_name_unchanged(self):
        cfg = self._cfg(container_name="my_container")
        assert cfg.container_name == "my_container"

    # ------------------------------------------------------------------
    # run_id
    # ------------------------------------------------------------------

    def test_file_prefix_run_id(self):
        cfg = self._cfg(file_prefix="file_{{ run_id }}")
        assert cfg.file_prefix == "file_aabbccddeeff"

    def test_dataset_name_run_id(self):
        cfg = self._cfg(dataset_name="{{ dataset_prefix }}_{{ run_id }}")
        assert cfg.dataset_name == "ds_aabbccddeeff"

    def test_container_name_run_id(self):
        cfg = self._cfg(container_name="ctr_{{ run_id }}")
        assert cfg.container_name == "ctr_aabbccddeeff"

    # ------------------------------------------------------------------
    # scope and rse
    # ------------------------------------------------------------------

    def test_file_prefix_scope(self):
        cfg = self._cfg(file_prefix="{{ scope }}_dummy")
        assert cfg.file_prefix == "cms_dummy"

    def test_dataset_name_rse(self):
        cfg = self._cfg(dataset_name="{{ dataset_prefix }}_{{ rse }}")
        assert cfg.dataset_name == "ds_T2_US_TEST"

    def test_container_name_scope_rse(self):
        cfg = self._cfg(container_name="{{ scope }}_{{ rse }}_tests")
        assert cfg.container_name == "cms_T2_US_TEST_tests"

    # ------------------------------------------------------------------
    # file_size (human-readable)
    # ------------------------------------------------------------------

    def test_file_prefix_file_size(self):
        cfg = self._cfg(file_prefix="f_{{ file_size }}", file_size_bytes=1073741824)
        assert cfg.file_prefix == "f_1GiB"

    def test_dataset_name_file_size(self):
        cfg = self._cfg(dataset_name="{{ dataset_prefix }}_{{ file_size }}",
                        file_size_bytes=536870912)
        assert cfg.dataset_name == "ds_512MiB"

    # ------------------------------------------------------------------
    # date / datetime variables present and correct format
    # ------------------------------------------------------------------

    def test_date_variable_format(self):
        from datetime import datetime, timezone
        cfg = self._cfg(dataset_name="{{ date }}")
        expected = datetime.now(timezone.utc).strftime("%Y%m%d")
        assert cfg.dataset_name == expected

    def test_datetime_variable_format(self):
        import re
        cfg = self._cfg(dataset_name="{{ datetime }}")
        assert re.match(r"^\d{8}_\d{6}$", cfg.dataset_name)

    def test_timestamp_is_integer_string(self):
        cfg = self._cfg(dataset_name="{{ timestamp }}")
        assert cfg.dataset_name.isdigit()

    # ------------------------------------------------------------------
    # file_size human-readable helper
    # ------------------------------------------------------------------

    def test_human_size_bytes(self):
        from dataset_generator.config import _human_size
        assert _human_size(512) == "512B"

    def test_human_size_kib(self):
        from dataset_generator.config import _human_size
        assert _human_size(2048) == "2KiB"

    def test_human_size_mib(self):
        from dataset_generator.config import _human_size
        assert _human_size(64 * 1024 * 1024) == "64MiB"

    def test_human_size_gib(self):
        from dataset_generator.config import _human_size
        assert _human_size(1024 ** 3) == "1GiB"

    def test_human_size_tib(self):
        from dataset_generator.config import _human_size
        assert _human_size(1024 ** 4) == "1TiB"

    # SI mode
    def test_human_size_si_bytes(self):
        from dataset_generator.config import _human_size
        assert _human_size(512, si=True) == "512B"

    def test_human_size_si_kb(self):
        from dataset_generator.config import _human_size
        assert _human_size(2000, si=True) == "2KB"

    def test_human_size_si_mb(self):
        from dataset_generator.config import _human_size
        assert _human_size(1_000_000, si=True) == "1MB"

    def test_human_size_si_gb(self):
        from dataset_generator.config import _human_size
        assert _human_size(1_000_000_000, si=True) == "1GB"

    def test_human_size_si_tb(self):
        from dataset_generator.config import _human_size
        assert _human_size(1_000_000_000_000, si=True) == "1TB"

    def test_human_size_si_gib_size_in_si(self):
        """1 GiB expressed in SI rounds to 1 GB (1073741824 / 10^9 = 1.07...)."""
        from dataset_generator.config import _human_size
        assert _human_size(1024 ** 3, si=True) == "1GB"

    # ------------------------------------------------------------------
    # size_label config and template variables
    # ------------------------------------------------------------------

    def test_size_label_default_is_iec(self):
        cfg = self._cfg()
        assert cfg.size_label == "iec"

    def test_file_size_template_default_is_iec(self):
        cfg = self._cfg(file_prefix="f_{{ file_size }}", file_size_bytes=1024 ** 3)
        assert cfg.file_prefix == "f_1GiB"

    def test_file_size_template_si(self):
        cfg = self._cfg(file_prefix="f_{{ file_size }}", file_size_bytes=1_000_000_000,
                        size_label="si")
        assert cfg.file_prefix == "f_1GB"

    def test_file_size_iec_always_available(self):
        """{{ file_size_iec }} is always IEC regardless of size_label."""
        cfg = self._cfg(file_prefix="{{ file_size_iec }}", file_size_bytes=1024 ** 3,
                        size_label="si")
        assert cfg.file_prefix == "1GiB"

    def test_file_size_si_always_available(self):
        """{{ file_size_si }} is always SI regardless of size_label."""
        cfg = self._cfg(file_prefix="{{ file_size_si }}", file_size_bytes=1_000_000_000,
                        size_label="iec")
        assert cfg.file_prefix == "1GB"

    def test_size_label_case_insensitive(self):
        cfg = self._cfg(size_label="SI")
        assert cfg.size_label == "si"

    def test_size_label_invalid_raises_on_validate(self):
        cfg = self._cfg(size_label="binary")
        with pytest.raises(ConfigError, match="size_label"):
            cfg.validate()

    # ------------------------------------------------------------------
    # file_prefix cache
    # ------------------------------------------------------------------

    def test_file_prefix_cached(self):
        """file_prefix should return the same object on repeated access."""
        cfg = self._cfg(file_prefix="file_{{ run_id }}")
        first = cfg.file_prefix
        second = cfg.file_prefix
        assert first is second

    def test_invalidate_name_cache_rerenders(self):
        """invalidate_name_cache causes file_prefix to re-render with new run_id."""
        cfg = self._cfg(file_prefix="f_{{ run_id }}")
        _ = cfg.file_prefix  # prime cache with original run_id
        cfg.run_id = "112233445566"
        cfg.invalidate_name_cache()
        assert cfg.file_prefix == "f_112233445566"

    # ------------------------------------------------------------------
    # Invalid templates raise ConfigError
    # ------------------------------------------------------------------

    def test_invalid_template_raises_config_error(self):
        from dataset_generator.config import ConfigError
        cfg = self._cfg(dataset_name="{{ unclosed")
        with pytest.raises(ConfigError, match="Template rendering failed"):
            _ = cfg.dataset_name

    # ------------------------------------------------------------------
    # num_files and file_size_bytes in context
    # ------------------------------------------------------------------

    def test_num_files_in_context(self):
        cfg = self._cfg(dataset_name="{{ num_files }}files", num_files=5)
        assert cfg.dataset_name == "5files"

    def test_file_size_bytes_in_context(self):
        cfg = self._cfg(dataset_name="{{ file_size_bytes }}b",
                        file_size_bytes=1073741824)
        assert cfg.dataset_name == "1073741824b"


# ---------------------------------------------------------------------------
# TestParseSize
# ---------------------------------------------------------------------------

class TestParseSize:
    """_parse_size accepts raw integers and human-readable strings."""

    def setup_method(self):
        from dataset_generator.config import _parse_size
        self.parse = _parse_size

    def test_plain_int(self):
        assert self.parse(1073741824) == 1073741824

    def test_plain_int_string(self):
        assert self.parse("1073741824") == 1073741824

    def test_gib(self):
        assert self.parse("1GiB") == 1024 ** 3

    def test_gib_with_space(self):
        assert self.parse("1 GiB") == 1024 ** 3

    def test_gb_si(self):
        # SI: GB = 10^9
        assert self.parse("1GB") == 1000 ** 3

    def test_mb_si(self):
        # SI: MB = 10^6
        assert self.parse("512MB") == 512 * 1000 ** 2

    def test_kb_si(self):
        # SI: KB = 10^3
        assert self.parse("64KB") == 64 * 1000

    def test_mib(self):
        assert self.parse("512MiB") == 512 * 1024 ** 2

    def test_kib(self):
        assert self.parse("64KiB") == 64 * 1024

    def test_tib(self):
        assert self.parse("2TiB") == 2 * 1024 ** 4

    def test_bytes_unit(self):
        assert self.parse("512B") == 512

    def test_case_insensitive(self):
        assert self.parse("1gib") == 1024 ** 3
        assert self.parse("1GIB") == 1024 ** 3
        assert self.parse("1Gib") == 1024 ** 3

    def test_fractional(self):
        assert self.parse("1.5GiB") == int(1.5 * 1024 ** 3)

    def test_unknown_unit_raises(self):
        from dataset_generator.config import ConfigError
        with pytest.raises(ConfigError, match="Unknown size unit"):
            self.parse("1ZiB")

    def test_bad_format_raises(self):
        from dataset_generator.config import ConfigError
        with pytest.raises(ConfigError, match="Cannot parse file size"):
            self.parse("big")

    def test_config_accepts_human_readable(self):
        """Config.__init__ resolves human-readable file_size_bytes to int."""
        cfg = Config(
            scope="cms", rse="T2", rse_mount="/tmp",
            dataset_prefix="ds", file_prefix="f",
            num_files=1, file_size_bytes="1GiB",
            token_endpoint="https://t", client_id="c", client_secret="s",
            rucio_host="https://r", rucio_auth_host="https://ra",
            rucio_account="a",
        )
        assert cfg.file_size_bytes == 1024 ** 3
