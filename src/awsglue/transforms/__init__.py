"""``awsglue.transforms`` shim.

Each transform is a class with a classmethod ``apply(...)`` taking Glue's
keyword names. Real ones are thin wrappers over ``DynamicFrame`` methods:

- ``ApplyMapping.apply(frame, mappings, case_sensitive=False, ...)``
- ``SelectFields.apply(frame, paths, ...)``, ``DropFields.apply(frame, paths, ...)``
- ``RenameField.apply(frame, old_name, new_name, ...)``
- ``Join.apply(frame1, frame2, keys1, keys2, ...)``
- ``DropNullFields.apply(frame, ...)``: drop top-level columns whose values are all null
- ``ResolveChoice.apply(frame, specs=None, choice="", ...)``: ``cast:`` specs only
- ``SelectFromCollection.apply(dfc, key, ...)``

Logged pass-through (``WARN``, data unchanged): ``Filter``, ``Map``,
``Relationalize``, ``Unbox``, ``UnnestFrame``, ``SplitFields``, ``SplitRows``,
``Spigot``, ``ErrorsAsDynamicFrame``. Transforms that return a collection in
Glue (``Relationalize``, ``SplitFields``, ``SplitRows``) return a
``DynamicFrameCollection`` holding the input frame under each output name.
``ErrorsAsDynamicFrame`` returns an empty frame (there are no error records).

Anything else (``FlatMap``, ``FillMissingValues``, ...): ``apply`` goes
through ``stub_log`` (``STUB``, raises in strict mode) and returns the frame.
Every transform also offers ``__call__`` (``ApplyMapping()(frame=...)``) and
``describe()``.
"""

from prytt.stub import log, stub_log

__all__ = [
    "GlueTransform",
    "ApplyMapping",
    "SelectFields",
    "DropFields",
    "RenameField",
    "Join",
    "DropNullFields",
    "ResolveChoice",
    "SelectFromCollection",
    "Filter",
    "Map",
    "Relationalize",
    "Unbox",
    "UnnestFrame",
    "SplitFields",
    "SplitRows",
    "Spigot",
    "ErrorsAsDynamicFrame",
    "FlatMap",
    "MapToCollection",
    "FillMissingValues",
    "FindMatches",
    "FindIncrementalMatches",
]

_GLUE_OPTS = ("transformation_ctx", "info", "stageThreshold", "totalThreshold")


def _opts(kwargs):
    return {k: v for k, v in kwargs.items() if k in _GLUE_OPTS}


class GlueTransform:
    """Base class. ``SomeTransform()(...)`` is the same as ``SomeTransform.apply(...)``."""

    def __call__(self, *args, **kwargs):
        return type(self).apply(*args, **kwargs)

    @classmethod
    def name(cls):
        return cls.__name__

    @classmethod
    def describe(cls):
        return {"name": cls.__name__, "description": f"prytt local shim for {cls.__name__}"}

    @classmethod
    def apply(cls, frame=None, *args, **kwargs):
        stub_log(cls.__name__, "apply", (frame, *args), kwargs)
        return frame


# Real transforms ---------------------------------------------------------


class ApplyMapping(GlueTransform):
    @classmethod
    def apply(cls, frame, mappings, case_sensitive=False, transformation_ctx="", info="", stageThreshold=0, totalThreshold=0):
        return frame.apply_mapping(mappings, case_sensitive=case_sensitive, transformation_ctx=transformation_ctx, info=info)


class SelectFields(GlueTransform):
    @classmethod
    def apply(cls, frame, paths, transformation_ctx="", info="", stageThreshold=0, totalThreshold=0):
        return frame.select_fields(paths, transformation_ctx=transformation_ctx, info=info)


class DropFields(GlueTransform):
    @classmethod
    def apply(cls, frame, paths, transformation_ctx="", info="", stageThreshold=0, totalThreshold=0):
        return frame.drop_fields(paths, transformation_ctx=transformation_ctx, info=info)


class RenameField(GlueTransform):
    @classmethod
    def apply(cls, frame, old_name, new_name, transformation_ctx="", info="", stageThreshold=0, totalThreshold=0):
        return frame.rename_field(old_name, new_name, transformation_ctx=transformation_ctx, info=info)


class Join(GlueTransform):
    @classmethod
    def apply(cls, frame1, frame2, keys1, keys2, transformation_ctx="", info="", stageThreshold=0, totalThreshold=0):
        return frame1.join(keys1, keys2, frame2, transformation_ctx=transformation_ctx, info=info)


def _has_null_type(dtype) -> bool:
    """True if ``dtype`` is, or holds where Glue looks, a ``NullType`` field."""
    from pyspark.sql.types import ArrayType, NullType, StructType

    if isinstance(dtype, NullType):
        return True
    if isinstance(dtype, StructType):
        return any(_has_null_type(f.dataType) for f in dtype.fields)
    if isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StructType):
        return _has_null_type(dtype.elementType)
    return False


def _without_null_types(expr, dtype):
    """Rebuild a struct (or array of structs) without its ``NullType`` fields."""
    from pyspark.sql import functions as F
    from pyspark.sql.types import ArrayType, NullType

    if isinstance(dtype, ArrayType):
        return F.transform(expr, lambda x: _without_null_types(x, dtype.elementType))
    fields = []
    for f in dtype.fields:
        if isinstance(f.dataType, NullType):
            continue
        child = expr.getField(f.name)
        if _has_null_type(f.dataType):
            child = _without_null_types(child, f.dataType)
        fields.append(child.alias(f.name))
    return F.when(expr.isNotNull(), F.struct(*fields))


class DropNullFields(GlueTransform):
    """Glue's rule: drop fields typed ``NullType`` (top-level, in structs, and
    in structs inside arrays). Values are never inspected, so a typed column
    that happens to be all null is kept. Maps are not walked."""

    @classmethod
    def apply(cls, frame, transformation_ctx="", info="", stageThreshold=0, totalThreshold=0):
        from pyspark.sql import functions as F
        from pyspark.sql.types import NullType

        df = frame.toDF()
        if not any(_has_null_type(f.dataType) for f in df.schema.fields):
            return frame._wrap(df)
        columns = []
        for f in df.schema.fields:
            if isinstance(f.dataType, NullType):
                continue
            column = F.col(f"`{f.name}`")
            if _has_null_type(f.dataType):
                column = _without_null_types(column, f.dataType).alias(f.name)
            columns.append(column)
        return frame._wrap(df.select(*columns))


class ResolveChoice(GlueTransform):
    @classmethod
    def apply(
        cls,
        frame,
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
        return frame.resolveChoice(
            specs=specs,
            choice=choice,
            database=database,
            table_name=table_name,
            transformation_ctx=transformation_ctx,
            info=info,
            catalog_id=catalog_id,
        )


class SelectFromCollection(GlueTransform):
    @classmethod
    def apply(cls, dfc, key, transformation_ctx=""):
        return dfc.select(key)


# Logged pass-through -----------------------------------------------------


def _pass(name, frame):
    log("WARN", f"{name} not applied locally (v1); data passed through unchanged")
    return frame


def _collection(frame, *names):
    from awsglue.dynamicframe import DynamicFrameCollection

    return DynamicFrameCollection({n: frame for n in names}, getattr(frame, "glue_ctx", None))


class Filter(GlueTransform):
    @classmethod
    def apply(cls, frame, f, transformation_ctx="", info="", stageThreshold=0, totalThreshold=0):
        return _pass("Filter", frame)


class Map(GlueTransform):
    @classmethod
    def apply(cls, frame, f, preservesPartitioning=False, transformation_ctx="", info="", stageThreshold=0, totalThreshold=0):
        return _pass("Map", frame)


class Relationalize(GlueTransform):
    @classmethod
    def apply(cls, frame, staging_path=None, name="roottable", options=None, transformation_ctx="", info="", stageThreshold=0, totalThreshold=0):
        return _collection(_pass("Relationalize", frame), name)


class Unbox(GlueTransform):
    @classmethod
    def apply(cls, frame, path, format, transformation_ctx="", info="", stageThreshold=0, totalThreshold=0, **options):
        return _pass("Unbox", frame)


class UnnestFrame(GlueTransform):
    @classmethod
    def apply(cls, frame, transformation_ctx="", info="", stageThreshold=0, totalThreshold=0):
        return _pass("UnnestFrame", frame)


class SplitFields(GlueTransform):
    @classmethod
    def apply(cls, frame, paths, name1=None, name2=None, transformation_ctx="", info="", stageThreshold=0, totalThreshold=0):
        name1 = name1 or f"{frame.name or 'frame'}1"
        name2 = name2 or f"{frame.name or 'frame'}2"
        return _collection(_pass("SplitFields", frame), name1, name2)


class SplitRows(GlueTransform):
    @classmethod
    def apply(cls, frame, comparison_dict, name1="frame1", name2="frame2", transformation_ctx="", info="", stageThreshold=0, totalThreshold=0):
        return _collection(_pass("SplitRows", frame), name1, name2)


class Spigot(GlueTransform):
    @classmethod
    def apply(cls, frame, path, options=None, transformation_ctx=""):
        return _pass("Spigot", frame)


class ErrorsAsDynamicFrame(GlueTransform):
    @classmethod
    def apply(cls, frame):
        log("WARN", "ErrorsAsDynamicFrame: no error records locally; returning an empty frame")
        return frame._wrap(frame.toDF().limit(0))


# Catch-all: no local behaviour, ``apply`` logs STUB (strict mode raises) ---


class FlatMap(GlueTransform):
    pass


class MapToCollection(GlueTransform):
    pass


class FillMissingValues(GlueTransform):
    pass


class FindMatches(GlueTransform):
    pass


class FindIncrementalMatches(GlueTransform):
    pass
