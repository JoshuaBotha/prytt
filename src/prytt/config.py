"""prytt configuration. Lazy: nothing is read until first use.

Interface:

- ``get() -> Config``: the process-wide config, loaded on first call, cached.
- ``reset()``: drop the cache so the next ``get()`` reloads (tests).
- ``load(env=None, cwd=None) -> Config``: pure, uncached load from an env
  mapping (default ``os.environ``) and a start dir (default cwd).

``Config`` is a frozen dataclass: ``disable``, ``log_level``, ``strict``,
``sail_port``, ``s3_mode``, ``s3_root`` (as configured, relative to cwd),
``catalog_on_missing``, ``catalog`` (``{"db.table": {path, format, ...}}``),
``iceberg_warehouse`` (absolute ``Path``, or an ``s3://`` URI kept as a
``str``) and ``args`` (``{name: str}``, local job arguments for
``getResolvedOptions``).

Sources, highest first: ``PRYTT_*`` env vars, the nearest ``prytt.toml``
(top-level keys), ``[tool.prytt]`` in the nearest ``pyproject.toml`` (both
searched upward from cwd), defaults. Keys use dotted names (``sail.port``);
in TOML write them as tables (``[sail] port = 0``). Higher sources override
per key, not per table. ``strict`` defaults to true when ``CI`` is truthy,
but an explicit ``strict`` from any source wins. ``s3.mode`` must be
``local``/``passthrough`` and ``catalog.on_missing`` ``error``/``empty``;
bad values (like bad booleans) raise ``ValueError`` when the config loads.
Job arguments come from an ``[args]`` table (any key, values made strings,
booleans as ``true``/``false``) and ``PRYTT_ARG_<name>`` env vars, merged
per name with the same precedence.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Config:
    disable: bool = False
    log_level: str = "INFO"
    strict: bool = False
    sail_port: int = 0
    s3_mode: str = "local"
    s3_root: str = "./.prytt/s3"
    catalog_on_missing: str = "error"
    catalog: dict = field(default_factory=dict)
    iceberg_warehouse: Path | str = Path(".prytt/warehouse")
    args: dict = field(default_factory=dict)


_TRUE = ("1", "true", "yes", "on")
_FALSE = ("0", "false", "no", "off", "")


def _bool(value) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in _TRUE:
        return True
    if text in _FALSE:
        return False
    raise ValueError(f"prytt config: expected a boolean, got {value!r}")


def _upper(value) -> str:
    return str(value).strip().upper()


S3_MODES = ("local", "passthrough")
ON_MISSING = ("error", "empty")


def _one_of(key: str, allowed: tuple):
    def parse(value) -> str:
        text = str(value)
        if text not in allowed:
            raise ValueError(f"prytt config: {key} must be one of {allowed}, got {value!r}")
        return text

    return parse


# dotted key -> (Config field, PRYTT_* env var, parser)
_KEYS = {
    "disable": ("disable", "PRYTT_DISABLE", _bool),
    "log_level": ("log_level", "PRYTT_LOG_LEVEL", _upper),
    "strict": ("strict", "PRYTT_STRICT", _bool),
    "sail.port": ("sail_port", "PRYTT_SAIL_PORT", int),
    "s3.mode": ("s3_mode", "PRYTT_S3_MODE", _one_of("s3.mode", S3_MODES)),
    "s3.root": ("s3_root", "PRYTT_S3_ROOT", str),
    "catalog.on_missing": (
        "catalog_on_missing",
        "PRYTT_CATALOG_ON_MISSING",
        _one_of("catalog.on_missing", ON_MISSING),
    ),
    "iceberg.warehouse": ("iceberg_warehouse", "PRYTT_ICEBERG_WAREHOUSE", str),
}


def _from_env(env) -> dict:
    return {key: env[var] for key, (_, var, _) in _KEYS.items() if var in env}


def _nearest(name: str, start: Path):
    for directory in (start, *start.parents):
        candidate = directory / name
        if candidate.is_file():
            return candidate
    return None


def _read_toml(path: Path) -> dict:
    import tomllib

    with path.open("rb") as fh:
        return tomllib.load(fh)


_ARG_ENV_PREFIX = "PRYTT_ARG_"


def _arg_value(value) -> str:
    """Glue passes every argument as a string."""
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _flatten(table: dict, origin: Path, catalog: dict, args: dict) -> dict:
    """Turn ``{"sail": {"port": 1}}`` into ``{"sail.port": 1}``.

    Sub-tables under ``catalog`` are table mappings; they go into ``catalog``.
    The ``args`` table holds job arguments; it goes into ``args``.
    """
    flat = {}
    for name, value in table.items():
        if name == "args" and isinstance(value, dict):
            args.update({key: _arg_value(arg) for key, arg in value.items()})
        elif isinstance(value, dict):
            for sub, subvalue in value.items():
                if name == "catalog" and isinstance(subvalue, dict):
                    catalog[sub] = subvalue
                else:
                    flat[f"{name}.{sub}"] = subvalue
        else:
            flat[name] = value
    unknown = sorted(set(flat) - set(_KEYS))
    if unknown:
        raise ValueError(f"prytt config: unknown key(s) {', '.join(unknown)} in {origin}")
    return flat


def _file_sources(cwd: Path, catalog: dict, args: dict) -> list:
    """File layers, lowest priority first. Fills ``catalog`` and ``args`` in the same order."""
    layers = []
    pyproject = _nearest("pyproject.toml", cwd)
    if pyproject is not None:
        table = _read_toml(pyproject).get("tool", {}).get("prytt", {})
        layers.append(_flatten(table, pyproject, catalog, args))
    prytt_toml = _nearest("prytt.toml", cwd)
    if prytt_toml is not None:
        layers.append(_flatten(_read_toml(prytt_toml), prytt_toml, catalog, args))
    return layers


def load(env=None, cwd=None) -> Config:
    env = os.environ if env is None else env
    cwd = Path.cwd() if cwd is None else Path(cwd)

    catalog = {}
    args = {}
    merged = {}
    for source in (*_file_sources(cwd, catalog, args), _from_env(env)):  # lowest first
        merged.update(source)
    for var, value in env.items():
        if var.startswith(_ARG_ENV_PREFIX) and len(var) > len(_ARG_ENV_PREFIX):
            args[var[len(_ARG_ENV_PREFIX) :]] = value

    values = {"catalog": catalog, "args": args}
    for key, raw in merged.items():
        name, _, parse = _KEYS[key]
        values[name] = parse(raw)
    if "strict" not in values:
        values["strict"] = env.get("CI", "").strip().lower() in ("1", "true", "yes")
    warehouse = values.get("iceberg_warehouse", ".prytt/warehouse")
    if warehouse.startswith(("s3://", "s3a://", "s3n://")):
        values["iceberg_warehouse"] = warehouse  # kept as is; the S3 write lock checks it
    else:
        values["iceberg_warehouse"] = (cwd / warehouse).resolve()
    return Config(**values)


_cached = None


def get() -> Config:
    global _cached
    if _cached is None:
        _cached = load()
    return _cached


def reset() -> None:
    global _cached
    _cached = None
