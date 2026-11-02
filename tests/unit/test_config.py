"""prytt.config: sources, precedence and defaults (spec §11)."""

from pathlib import Path

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
    (tmp_path / "prytt.toml").write_text('[s3]\nmode = "passthrough"\n[catalog]\non_missing = "empty"\n')

    cfg = config.load(env={}, cwd=tmp_path)

    assert (cfg.s3_mode, cfg.catalog_on_missing) == ("passthrough", "empty")
