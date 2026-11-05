"""``awsglue.dynamicframe`` shim. A DynamicFrame wraps a DataFrame.

Interface (``DynamicFrame``; every frame-returning method returns a new
``DynamicFrame`` with the same ``glue_ctx``):

- ``DynamicFrame(df, glue_ctx=None, name="")``;
  ``DynamicFrame.fromDF(dataframe, glue_ctx, name)``; ``toDF()``.
- ``count()``, ``show(num_rows=20)``, ``printSchema()``, ``schema()``:
  delegate to the DataFrame (``schema()`` returns the Spark ``StructType``).
- Field paths are dotted (``address.city``); wrap a name that contains a
  literal dot in backticks (``"`a.b`"``), as in Glue.
- ``select_fields(paths)``: keep those fields in the given order; nested paths
  keep their struct (``address.city`` -> ``address: struct<city>``).
- ``drop_fields(paths)``: drop top-level or nested fields.
- ``rename_field(oldName, newName)``: top-level keeps position; nested paths
  re-add the field under its new path.
- ``apply_mapping(mappings)``: ``(src, src_type, tgt, tgt_type)`` tuples
  (3-tuples ``(src, tgt, tgt_type)`` are accepted too). Each source is cast
  to ``tgt_type`` (Glue names: ``long``/``int``/``short``/``byte``/...;
  complex types such as ``struct``/``array`` are not cast) and placed at
  ``tgt``; dotted targets build structs. Unmapped fields are dropped.
- ``resolveChoice(specs=None, choice="")``: ``(path, "cast:<type>")`` specs
  are real casts; every other action (and ``choice``) logs ``WARN`` and the
  frame passes through (there are no choice types locally).
- ``join(paths1, paths2, frame2)``: inner equi-join. With identical key
  names the key appears once (``on=``); otherwise both keys are kept.
- ``repartition(n)``, ``coalesce(n)``: via the DataFrame.
- ``filter(f)``, ``map(f)``: v1 log ``WARN`` and return the frame unchanged.
- ``errorsCount()``, ``stageErrorsCount()``: 0.
- Anything else (``relationalize``, ``unnest``, ``spigot``, ...): ``LogAny``
  catch-all (log ``STUB``, return ``self``; raises in strict mode).

``DynamicFrameCollection(dynamic_frames: dict, glue_ctx)``: ``keys()``,
``values()``, ``select(key)``, ``map(f)`` (``f(frame, transformation_ctx)``
per frame, returns a new collection), ``len()``, ``[key]``.

Module helper for transforms: ``glue_type(name) -> str | None``, the Spark
cast type for a Glue type name (``None`` = do not cast).
"""

from prytt.stub import LogAny, log

__all__ = ["DynamicFrame", "DynamicFrameCollection"]

_GLUE_TYPES = {
    "long": "bigint",
    "int": "int",
    "integer": "int",
    "short": "smallint",
    "byte": "tinyint",
    "bigint": "bigint",
    "smallint": "smallint",
    "tinyint": "tinyint",
    "string": "string",
    "double": "double",
    "float": "float",
    "boolean": "boolean",
    "bool": "boolean",
    "date": "date",
    "timestamp": "timestamp",
    "binary": "binary",
    "decimal": "decimal",
}
_NO_CAST = ("struct", "array", "map", "choice", "null", "object", "")


def glue_type(name):
    """Spark cast type for a Glue type name, or ``None`` when no cast applies."""
    text = str(name or "").strip().lower()
    if text.startswith("decimal("):
        return text
    if text in _NO_CAST or text.split("<")[0] in _NO_CAST:
        return None
    return _GLUE_TYPES.get(text, text)


def _split(path: str) -> list:
    """``a.b`` -> ``[a, b]``; backticks protect literal dots (`` `a.b`.c ``)."""
    parts, buf, quoted = [], [], False
    for ch in path:
        if ch == "`":
            quoted = not quoted
        elif ch == "." and not quoted:
            parts.append("".join(buf))
            buf = []
        else:
            buf.append(ch)
    parts.append("".join(buf))
    return parts


def _col(parts):
    from pyspark.sql import functions as F

    return F.col(".".join(f"`{p}`" for p in parts))


def _tree_insert(tree: dict, parts: list, expr) -> None:
    for part in parts[:-1]:
        tree = tree.setdefault(part, {})
    tree[parts[-1]] = expr


def _tree_columns(tree: dict) -> list:
    """Turn ``{name: expr | subtree}`` into aliased columns (structs for subtrees)."""
    from pyspark.sql import functions as F

    out = []
    for name, value in tree.items():
        expr = F.struct(*_tree_columns(value)) if isinstance(value, dict) else value
        out.append(expr.alias(name))
    return out


def _with_path(df, parts, expr):
    """Set field ``parts`` (top-level or nested) to ``expr``."""
    if len(parts) == 1:
        return df.withColumn(parts[0], expr)
    return df.withColumn(parts[0], _col(parts[:1]).withField(".".join(parts[1:]), expr))


def _drop_path(df, parts):
    if len(parts) == 1:
        return df.drop(parts[0])
    parent = _col(parts[:-1]).dropFields(parts[-1])
    if len(parts) == 2:
        return df.withColumn(parts[0], parent)
    return _with_path(df, parts[:-1], parent)


class DynamicFrame(LogAny):
    def __init__(self, df, glue_ctx=None, name=""):
        self._df = df
        self.glue_ctx = glue_ctx
        self.name = name

    @classmethod
    def fromDF(cls, dataframe, glue_ctx, name):
        return cls(dataframe, glue_ctx, name)

    def _wrap(self, df, name=None):
        return DynamicFrame(df, self.glue_ctx, self.name if name is None else name)

    def toDF(self, options=None):
        return self._df

    def count(self):
        return self._df.count()

    def schema(self):
        return self._df.schema

    def show(self, num_rows=20):
        self._df.show(num_rows)

    def printSchema(self):
        self._df.printSchema()

    def select_fields(
        self, paths, transformation_ctx="", info="", stageThreshold=0, totalThreshold=0
    ):
        tree = {}
        for path in paths:
            parts = _split(path)
            _tree_insert(tree, parts, _col(parts))
        return self._wrap(self._df.select(*_tree_columns(tree)))

    def drop_fields(
        self, paths, transformation_ctx="", info="", stageThreshold=0, totalThreshold=0
    ):
        df = self._df
        for path in paths:
            df = _drop_path(df, _split(path))
        return self._wrap(df)

    def rename_field(
        self, oldName, newName, transformation_ctx="", info="", stageThreshold=0, totalThreshold=0
    ):
        old, new = _split(oldName), _split(newName)
        if len(old) == 1 and len(new) == 1:
            return self._wrap(self._df.withColumnRenamed(old[0], new[0]))
        df = _with_path(self._df, new, _col(old))
        return self._wrap(_drop_path(df, old))

    def apply_mapping(
        self,
        mappings,
        case_sensitive=False,
        transformation_ctx="",
        info="",
        stageThreshold=0,
        totalThreshold=0,
    ):
        tree = {}
        for mapping in mappings:
            if len(mapping) == 4:
                src, _src_type, tgt, tgt_type = mapping
            elif len(mapping) == 3:
                src, tgt, tgt_type = mapping
            else:
                raise ValueError(
                    f"apply_mapping: expected (src, src_type, tgt, tgt_type), got {mapping!r}"
                )
            expr = _col(_split(src))
            cast = glue_type(tgt_type)
            if cast:
                expr = expr.cast(cast)
            _tree_insert(tree, _split(tgt), expr)
        return self._wrap(self._df.select(*_tree_columns(tree)))

    def resolveChoice(
        self,
        specs=None,
        choice="",
        database=None,
        table_name=None,
        transformation_ctx="",
        info="",
        stageThreshold=0,
        totalThreshold=0,
        catalog_id=None,
    ):
        df = self._df
        for path, action in specs or []:
            if str(action).startswith("cast:"):
                parts = _split(path)
                df = _with_path(df, parts, _col(parts).cast(glue_type(action[5:]) or action[5:]))
            else:
                log(
                    "WARN",
                    f"resolveChoice({path!r}, {action!r}) not applied locally; data passed through",
                )
        if choice:
            log(
                "WARN", f"resolveChoice(choice={choice!r}) not applied locally; data passed through"
            )
        return self._wrap(df)

    def join(
        self,
        paths1,
        paths2,
        frame2,
        transformation_ctx="",
        info="",
        stageThreshold=0,
        totalThreshold=0,
    ):
        left, right = self._df, frame2.toDF()
        if list(paths1) == list(paths2):
            return self._wrap(left.join(right, on=list(paths1), how="inner"))
        cond = [_col_of(left, a) == _col_of(right, b) for a, b in zip(paths1, paths2, strict=True)]
        return self._wrap(left.join(right, on=cond, how="inner"))

    def repartition(
        self, num_partitions, transformation_ctx="", info="", stageThreshold=0, totalThreshold=0
    ):
        return self._wrap(self._df.repartition(num_partitions))

    def coalesce(
        self,
        num_partitions,
        shuffle=False,
        transformation_ctx="",
        info="",
        stageThreshold=0,
        totalThreshold=0,
    ):
        return self._wrap(self._df.coalesce(num_partitions))

    def filter(self, f, transformation_ctx="", info="", stageThreshold=0, totalThreshold=0):
        log("WARN", "DynamicFrame.filter not applied locally (v1); data passed through unchanged")
        return self

    def map(self, f, transformation_ctx="", info="", stageThreshold=0, totalThreshold=0):
        log("WARN", "DynamicFrame.map not applied locally (v1); data passed through unchanged")
        return self

    def errorsCount(self):
        return 0

    def stageErrorsCount(self):
        return 0


def _col_of(df, path):
    parts = _split(path)
    expr = df[parts[0]]
    for part in parts[1:]:
        expr = expr.getField(part)
    return expr


class DynamicFrameCollection(LogAny):
    def __init__(self, dynamic_frames, glue_ctx=None):
        self._frames = dict(dynamic_frames)
        self.glue_ctx = glue_ctx

    def keys(self):
        return self._frames.keys()

    def values(self):
        return list(self._frames.values())

    def select(self, key, transformation_ctx=""):
        return self._frames[key]

    def __getitem__(self, key):
        return self._frames[key]

    def __len__(self):
        return len(self._frames)

    def __iter__(self):
        return iter(self._frames)

    def map(self, callable, transformation_ctx=""):
        return DynamicFrameCollection(
            {key: callable(frame, transformation_ctx) for key, frame in self._frames.items()},
            self.glue_ctx,
        )
