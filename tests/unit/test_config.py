"""prytt.config: sources, precedence and defaults."""

import pytest

from prytt import config


def test_defaults_with_no_env_and_no_files(tmp_path):
    cfg = config.load(env={}, cwd=tmp_path)

    assert cfg.disable is False
    assert cfg.log_level == "INFO"
    assert cfg.strict is False
    assert cfg.sail_port == 0
    assert cfg.s3_mode == "local"
    assert cfg.s3_root == "./.prytt/s3"
    assert cfg.catalog_on_missing == "error"
    assert cfg.catalog == {}
    assert cfg.iceberg_warehouse == tmp_path / ".prytt" / "warehouse"


@pytest.mark.parametrize(
    ("env", "strict"),
    [
        ({}, False),
        ({"CI": "true"}, True),
        ({"CI": "1"}, True),
        ({"CI": "YES"}, True),
        ({"CI": "false"}, False),
        ({"CI": "0"}, False),
        ({"CI": "true", "PRYTT_STRICT": "0"}, False),
        ({"CI": "true", "PRYTT_STRICT": "false"}, False),
        ({"PRYTT_STRICT": "1"}, True),
    ],
)
def test_strict_defaults_from_ci_unless_set_explicitly(tmp_path, env, strict):
    assert config.load(env=env, cwd=tmp_path).strict is strict


def test_precedence_is_env_then_prytt_toml_then_pyproject_per_key(tmp_path):
    (tmp_path / "pyproject.toml").write_text(
        """
[project]
name = "my-glue-jobs"

[tool.prytt]
log_level = "debug"

[tool.prytt.sail]
port = 1111

[tool.prytt.s3]
root = "from-pyproject"
mode = "passthrough"
"""
    )
    (tmp_path / "prytt.toml").write_text(
        """
[sail]
port = 2222

[s3]
root = "from-prytt-toml"
"""
    )

    cfg = config.load(env={"PRYTT_SAIL_PORT": "3333"}, cwd=tmp_path)

    assert cfg.sail_port == 3333
    assert cfg.s3_root == "from-prytt-toml"
    assert cfg.s3_mode == "passthrough"
    assert cfg.log_level == "DEBUG"


def test_explicit_strict_in_file_beats_ci_default(tmp_path):
    (tmp_path / "prytt.toml").write_text("strict = false\n")

    assert config.load(env={"CI": "true"}, cwd=tmp_path).strict is False


def test_files_are_found_by_searching_upward_from_cwd(tmp_path):
    (tmp_path / "pyproject.toml").write_text('[tool.prytt]\nlog_level = "WARN"\n')
    (tmp_path / "prytt.toml").write_text("[catalog]\non_missing = 'empty'\n")
    nested = tmp_path / "jobs" / "bronze"
    nested.mkdir(parents=True)

    cfg = config.load(env={}, cwd=nested)

    assert cfg.log_level == "WARN"
    assert cfg.catalog_on_missing == "empty"


def test_nearest_pyproject_wins_even_without_prytt_table(tmp_path):
    (tmp_path / "pyproject.toml").write_text('[tool.prytt]\nlog_level = "WARN"\n')
    inner = tmp_path / "inner"
    inner.mkdir()
    (inner / "pyproject.toml").write_text('[project]\nname = "inner"\n')

    assert config.load(env={}, cwd=inner).log_level == "INFO"


def test_catalog_tables_come_from_files_with_prytt_toml_overriding(tmp_path):
    (tmp_path / "pyproject.toml").write_text(
        """
[tool.prytt.catalog]
on_missing = "empty"

[tool.prytt.catalog."raw_db.orders"]
path = "s3://raw-bucket/orders/"
format = "json"
options = { multiLine = true }

[tool.prytt.catalog."raw_db.customers"]
path = "s3://raw-bucket/customers/"
format = "csv"
"""
    )
    (tmp_path / "prytt.toml").write_text(
        """
[catalog."raw_db.customers"]
path = "local/customers/"
format = "parquet"
"""
    )

    cfg = config.load(env={}, cwd=tmp_path)

    assert cfg.catalog_on_missing == "empty"
    assert cfg.catalog == {
        "raw_db.orders": {
            "path": "s3://raw-bucket/orders/",
            "format": "json",
            "options": {"multiLine": True},
        },
        "raw_db.customers": {"path": "local/customers/", "format": "parquet"},
    }


def test_iceberg_warehouse_is_made_absolute_against_cwd(tmp_path):
    cfg = config.load(env={"PRYTT_ICEBERG_WAREHOUSE": "wh/iceberg"}, cwd=tmp_path)

    assert cfg.iceberg_warehouse == tmp_path / "wh" / "iceberg"
    assert cfg.iceberg_warehouse.is_absolute()


def test_iceberg_warehouse_on_s3_is_kept_as_a_uri(tmp_path):
    cfg = config.load(env={"PRYTT_ICEBERG_WAREHOUSE": "s3://b/wh"}, cwd=tmp_path)

    assert cfg.iceberg_warehouse == "s3://b/wh"


@pytest.mark.parametrize(
    ("file", "text"),
    [
        ("prytt.toml", "allow_s3_writes = true\n"),
        ("prytt.toml", "[s3]\nallow_writes = true\n"),
        ("prytt.toml", "PRYTT_ALLOW_S3_WRITES = 1\n"),
        ("pyproject.toml", "[tool.prytt]\nallow_s3_writes = true\n"),
        ("pyproject.toml", "[tool.prytt.s3]\nallow_writes = true\n"),
    ],
)
def test_no_config_file_key_unlocks_s3_writes(tmp_path, file, text):
    (tmp_path / file).write_text(text)

    with pytest.raises(ValueError, match="unknown key"):
        config.load(env={}, cwd=tmp_path)


def test_get_loads_lazily_from_process_env_and_cwd_and_caches(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("PRYTT_LOG_LEVEL", "debug")
    config.reset()

    first = config.get()
    monkeypatch.setenv("PRYTT_LOG_LEVEL", "error")

    assert first.log_level == "DEBUG"
    assert config.get() is first

    config.reset()
    assert config.get().log_level == "ERROR"
    config.reset()


def test_unknown_key_in_config_file_is_an_error_naming_the_file(tmp_path):
    (tmp_path / "prytt.toml").write_text("[sail]\nprot = 1\n")

    with pytest.raises(ValueError, match=r"sail\.prot.*prytt\.toml"):
        config.load(env={}, cwd=tmp_path)


@pytest.mark.parametrize(
    ("env", "key"),
    [
        ({"PRYTT_S3_MODE": "cloud"}, "s3.mode"),
        ({"PRYTT_CATALOG_ON_MISSING": "ignore"}, "catalog.on_missing"),
    ],
)
def test_enum_values_are_checked_when_config_loads(tmp_path, env, key):
    with pytest.raises(ValueError, match=rf"{key} must be one of"):
        config.load(env=env, cwd=tmp_path)


def test_enum_values_are_accepted_from_files(tmp_path):
    (tmp_path / "prytt.toml").write_text(
        '[s3]\nmode = "passthrough"\n[catalog]\non_missing = "empty"\n'
    )

    cfg = config.load(env={}, cwd=tmp_path)

    assert (cfg.s3_mode, cfg.catalog_on_missing) == ("passthrough", "empty")


def test_job_args_default_to_empty(tmp_path):
    assert config.load(env={}, cwd=tmp_path).args == {}


def test_job_args_come_from_files_and_env_with_env_highest(tmp_path):
    (tmp_path / "pyproject.toml").write_text(
        """
[tool.prytt.args]
JOB_NAME = "from-pyproject"
source_path = "s3://raw/in/"
"""
    )
    (tmp_path / "prytt.toml").write_text(
        '[args]\nsource_path = "s3://raw/other/"\nwith_iceberg = true\n'
    )
    env = {"PRYTT_ARG_JOB_NAME": "from-env"}

    cfg = config.load(env=env, cwd=tmp_path)

    assert cfg.args == {
        "JOB_NAME": "from-env",
        "source_path": "s3://raw/other/",
        "with_iceberg": "true",
    }


def write_table(tmp_path, extra):
    (tmp_path / "prytt.toml").write_text(
        f'[catalog."raw_db.orders"]\npath = "s3://raw/orders/"\nformat = "json"\n{extra}\n'
    )


@pytest.mark.parametrize("mode", ["local", "passthrough"])
def test_catalog_table_accepts_a_mode(tmp_path, mode):
    write_table(tmp_path, f'mode = "{mode}"')

    cfg = config.load(env={}, cwd=tmp_path)

    assert cfg.catalog["raw_db.orders"]["mode"] == mode


def test_bad_catalog_table_mode_fails_at_load_naming_the_allowed_values(tmp_path):
    write_table(tmp_path, 'mode = "passthru"')

    with pytest.raises(ValueError, match=r"raw_db\.orders.*mode.*'local', 'passthrough'.*passthru"):
        config.load(env={}, cwd=tmp_path)


def test_other_unknown_catalog_table_key_still_fails_at_load(tmp_path):
    write_table(tmp_path, 'mode = "local"\nregion = "eu-west-1"')

    with pytest.raises(ValueError, match=r"raw_db\.orders.*unknown key.*region"):
        config.load(env={}, cwd=tmp_path)


def test_s3_profile_defaults_to_none(tmp_path):
    assert config.load(env={}, cwd=tmp_path).s3_profile is None


def test_s3_profile_is_read_from_the_file_and_env_overrides_it(tmp_path):
    (tmp_path / "prytt.toml").write_text('[s3]\nprofile = "team-sso"\n')

    assert config.load(env={}, cwd=tmp_path).s3_profile == "team-sso"
    assert config.load(env={"PRYTT_S3_PROFILE": "other"}, cwd=tmp_path).s3_profile == "other"
