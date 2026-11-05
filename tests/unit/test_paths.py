"""prytt.paths: s3:// -> local mapping, passthrough mode."""

import pytest

from prytt import config, paths
from prytt.stub import GlueLocalUnsupported


@pytest.fixture(autouse=True)
def clean_env(monkeypatch, tmp_path):
    for name in ("CI", "PRYTT_STRICT", "PRYTT_LOG_LEVEL", "PRYTT_S3_MODE", "PRYTT_S3_ROOT"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.chdir(tmp_path)
    config.reset()
    yield
    config.reset()


def set_env(monkeypatch, **values):
    for name, value in values.items():
        monkeypatch.setenv(name, value)
    config.reset()


def test_s3_uri_maps_under_s3_root(monkeypatch, tmp_path):
    set_env(monkeypatch, PRYTT_S3_ROOT=str(tmp_path / "root"))

    assert (
        paths.map_path("s3://bucket/a/b.json")
        == (tmp_path / "root" / "bucket" / "a" / "b.json").as_posix()
    )


def test_default_root_is_dot_prytt_s3_under_cwd(tmp_path):
    assert paths.map_path("s3://b/k") == (tmp_path / ".prytt" / "s3" / "b" / "k").as_posix()


@pytest.mark.parametrize("scheme", ["s3a", "s3n"])
def test_s3a_and_s3n_map_like_s3(scheme, tmp_path):
    assert paths.map_path(f"{scheme}://b/k") == (tmp_path / ".prytt" / "s3" / "b" / "k").as_posix()


def test_trailing_slash_is_kept(tmp_path):
    assert (
        paths.map_path("s3://b/dir/") == (tmp_path / ".prytt" / "s3" / "b" / "dir").as_posix() + "/"
    )


def test_mapping_is_logged_as_map(capsys, tmp_path):
    paths.map_path("s3://b/k")

    assert (
        capsys.readouterr().err == f"[prytt] MAP  s3://b/k -> {tmp_path.as_posix()}/.prytt/s3/b/k\n"
    )


@pytest.mark.parametrize("uri", ["/abs/file.json", "rel/dir", "file:///x/y"])
def test_non_s3_paths_are_untouched_and_not_logged(uri, capsys):
    assert paths.map_path(uri) == uri
    assert capsys.readouterr().err == ""


def test_write_mapping_creates_the_local_directory(tmp_path):
    local = paths.map_path("s3://b/out/table/", for_write=True)

    assert (tmp_path / ".prytt" / "s3" / "b" / "out" / "table").is_dir()
    assert local.endswith("/b/out/table/")


def test_read_mapping_creates_nothing(tmp_path):
    paths.map_path("s3://b/in/")

    assert not (tmp_path / ".prytt").exists()


def test_map_paths_maps_a_list_or_single_string(tmp_path):
    root = tmp_path / ".prytt" / "s3"

    assert paths.map_paths(["s3://b/x", "s3://b/y"]) == [
        (root / "b" / "x").as_posix(),
        (root / "b" / "y").as_posix(),
    ]
    assert paths.map_paths("s3://b/x") == [(root / "b" / "x").as_posix()]


# --- passthrough --------------------------------------------------------------


def passthrough(monkeypatch, tmp_path, **env):
    for name in ("AWS_REGION", "AWS_DEFAULT_REGION", "AWS_PROFILE"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("AWS_CONFIG_FILE", str(tmp_path / "aws-config"))
    set_env(monkeypatch, **{"PRYTT_S3_MODE": "passthrough", **env})


def test_passthrough_leaves_uris_alone_without_needing_a_region(monkeypatch, tmp_path, capsys):
    passthrough(monkeypatch, tmp_path)

    assert paths.map_path("s3://b/k") == "s3://b/k"
    assert paths.map_path("s3a://b/k", for_write=True) == "s3a://b/k"

    assert capsys.readouterr().err == ""
    assert not (tmp_path / ".prytt").exists()


def test_unknown_s3_mode_is_rejected(monkeypatch):
    set_env(monkeypatch, PRYTT_S3_MODE="cloud")

    with pytest.raises(ValueError, match=r"s3\.mode"):
        paths.map_path("s3://b/k")


def write_prytt_toml(tmp_path, text):
    (tmp_path / "prytt.toml").write_text(text)
    config.reset()


def test_mapped_catalog_table_resolves_to_its_path_format_and_options(tmp_path):
    write_prytt_toml(
        tmp_path,
        """
[catalog."raw_db.orders"]
path = "s3://raw-bucket/orders/"
format = "json"
options = { multiLine = true }
""",
    )

    table = paths.resolve_table("raw_db", "orders")

    assert table == paths.CatalogTable("s3://raw-bucket/orders/", "json", {"multiLine": True})


def test_unmapped_catalog_table_error_shows_the_toml_to_add():
    with pytest.raises(paths.CatalogTableNotFound) as info:
        paths.resolve_table("raw_db", "orders")

    message = str(info.value)
    assert '[tool.prytt.catalog."raw_db.orders"]' in message
    assert 'path = "s3://' in message
    assert 'format = "' in message
    assert isinstance(info.value, LookupError)


def test_catalog_entry_without_path_or_format_is_a_config_error(tmp_path):
    write_prytt_toml(tmp_path, '[catalog."raw_db.orders"]\nformat = "json"\n')

    with pytest.raises(ValueError, match=r"raw_db\.orders.*path"):
        paths.resolve_table("raw_db", "orders")


class _NoSpark:
    """A session that must not be touched: config errors come first."""

    def __getattr__(self, name):
        raise AssertionError(f"spark.{name} used before config was checked")


@pytest.mark.parametrize(
    "toml, env, error",
    [
        ('[catalog."db.t"]\npath = "s3://b/t/"\nformat = "orc"\n', {}, GlueLocalUnsupported),
        (
            '[catalog."db.t"]\npath = "s3://b/t/"\nformat = "json"\n',
            {"PRYTT_S3_MODE": "cloud"},
            ValueError,
        ),
    ],
    ids=["orc", "bad-s3-mode"],
)
def test_temp_view_registration_raises_config_errors(monkeypatch, tmp_path, toml, env, error):
    (tmp_path / "prytt.toml").write_text(toml)
    set_env(monkeypatch, **env)

    with pytest.raises(error):
        paths.register_mapped_tables(_NoSpark())
