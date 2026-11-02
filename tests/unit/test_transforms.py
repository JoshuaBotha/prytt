"""awsglue.transforms: thin Glue-signature wrappers over DynamicFrame (spec §9).

One Spark session for the whole module: it is slow to start.
"""

import pytest

from prytt import config


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    return SparkSession.builder.getOrCreate()


@pytest.fixture(autouse=True)
def clean_env(monkeypatch):
    for name in ("CI", "PRYTT_STRICT", "PRYTT_LOG_LEVEL"):
        monkeypatch.delenv(name, raising=False)
    config.reset()
    yield
    config.reset()


@pytest.fixture
def people(spark):
    from awsglue.dynamicframe import DynamicFrame

    df = spark.createDataFrame(
        [(1, "ann", "31", ("cpt", "za")), (2, "bob", "42", ("ldn", "uk"))],
        "id int, name string, age string, address struct<city:string,country:string>",
    )
    return DynamicFrame.fromDF(df, None, "people")


def rows(frame):
    return sorted(tuple(r) for r in frame.toDF().collect())


def ddl(frame):
    return frame.toDF().schema.simpleString()


def test_star_import_exposes_glue_transforms():
    namespace = {}
    exec("from awsglue.transforms import *", namespace)

    for name in (
        "ApplyMapping", "SelectFields", "DropFields", "RenameField", "Join", "DropNullFields",
        "ResolveChoice", "Filter", "Map", "Relationalize", "Unbox", "UnnestFrame",
        "SplitFields", "SplitRows", "Spigot", "ErrorsAsDynamicFrame",
    ):
        assert name in namespace, name


def test_apply_mapping_with_glue_keywords(people):
    from awsglue.transforms import ApplyMapping

    out = ApplyMapping.apply(
        frame=people,
        mappings=[
            ("id", "int", "id", "long"),
            ("age", "string", "age", "int"),
            ("address.city", "string", "loc.city", "string"),
        ],
        transformation_ctx="am",
    )

    assert ddl(out) == "struct<id:bigint,age:int,loc:struct<city:string>>"
    assert rows(out) == [(1, 31, ("cpt",)), (2, 42, ("ldn",))]


def test_select_fields_keeps_listed_paths(people):
    from awsglue.transforms import SelectFields

    out = SelectFields.apply(frame=people, paths=["name", "address.country"], transformation_ctx="sf")

    assert ddl(out) == "struct<name:string,address:struct<country:string>>"
    assert rows(out) == [("ann", ("za",)), ("bob", ("uk",))]


def test_drop_fields_removes_top_level_and_nested(people):
    from awsglue.transforms import DropFields

    out = DropFields.apply(frame=people, paths=["age", "address.city"])

    assert ddl(out) == "struct<id:int,name:string,address:struct<country:string>>"
    assert rows(out) == [(1, "ann", ("za",)), (2, "bob", ("uk",))]


def test_rename_field_positional_and_keywords(people):
    from awsglue.transforms import RenameField

    by_position = RenameField.apply(people, "name", "full_name")
    by_keyword = RenameField.apply(frame=people, old_name="address.city", new_name="address.town")

    assert by_position.toDF().columns == ["id", "full_name", "age", "address"]
    assert rows(by_keyword.select_fields(["address.town"])) == [(("cpt",),), (("ldn",),)]
    address = by_keyword.toDF().schema["address"].dataType
    assert "city" not in address.fieldNames()
    assert sorted(address.fieldNames()) == ["country", "town"]


def test_join_positional_frames_then_keys(spark, people):
    from awsglue.dynamicframe import DynamicFrame
    from awsglue.transforms import Join

    orders = DynamicFrame.fromDF(
        spark.createDataFrame([(10, 1), (11, 1)], "order_id int, person_id int"), None, "orders"
    )

    out = Join.apply(people, orders, ["id"], ["person_id"], transformation_ctx="j")

    df = out.toDF()
    assert {"id", "person_id", "order_id", "name"} <= set(df.columns)
    assert sorted((r["id"], r["person_id"], r["order_id"], r["name"]) for r in df.collect()) == [
        (1, 1, 10, "ann"),
        (1, 1, 11, "ann"),
    ]


def test_join_on_same_key_name_keeps_one_key_column(spark, people):
    from awsglue.dynamicframe import DynamicFrame
    from awsglue.transforms import Join

    orders = DynamicFrame.fromDF(spark.createDataFrame([(10, 2)], "order_id int, id int"), None, "orders")

    df = Join.apply(frame1=people, frame2=orders, keys1=["id"], keys2=["id"]).toDF()

    assert df.columns.count("id") == 1
    assert [(r["id"], r["order_id"], r["name"]) for r in df.collect()] == [(2, 10, "bob")]


def test_drop_null_fields_drops_all_null_columns_only(spark):
    from awsglue.dynamicframe import DynamicFrame
    from awsglue.transforms import DropNullFields

    df = spark.createDataFrame([(1, None, None), (2, None, "x")], "id int, empty string, some string")
    out = DropNullFields.apply(frame=DynamicFrame.fromDF(df, None, "t"), transformation_ctx="dnf")

    assert out.toDF().columns == ["id", "some"]
    assert rows(out) == [(1, None), (2, "x")]


def test_resolve_choice_cast_specs(people):
    from awsglue.transforms import ResolveChoice

    out = ResolveChoice.apply(frame=people, specs=[("age", "cast:long")], transformation_ctx="rc")

    assert dict(out.toDF().dtypes)["age"] == "bigint"
    assert sorted((r["id"], r["age"]) for r in out.toDF().collect()) == [(1, 31), (2, 42)]


def test_transforms_return_dynamic_frames_with_context(people):
    from awsglue.dynamicframe import DynamicFrame
    from awsglue.transforms import SelectFields

    people.glue_ctx = ctx = object()
    out = SelectFields.apply(frame=people, paths=["id"])

    assert isinstance(out, DynamicFrame)
    assert out.glue_ctx is ctx


def test_instance_call_is_apply(people):
    from awsglue.transforms import DropFields

    assert DropFields()(frame=people, paths=["age"]).toDF().columns == ["id", "name", "address"]


def test_select_from_collection(people):
    from awsglue.dynamicframe import DynamicFrameCollection
    from awsglue.transforms import SelectFromCollection

    dfc = DynamicFrameCollection({"a": people}, None)

    assert SelectFromCollection.apply(dfc=dfc, key="a") is people


@pytest.mark.parametrize(
    "name, kwargs",
    [
        ("Filter", {"f": lambda r: False}),
        ("Map", {"f": lambda r: r}),
        ("Unbox", {"path": "name", "format": "json"}),
        ("UnnestFrame", {}),
        ("Spigot", {"path": "s3://b/spigot", "options": {"topk": 10}}),
    ],
)
def test_pass_through_transforms_warn_and_return_frame_even_in_strict_mode(
    people, capsys, monkeypatch, name, kwargs
):
    import awsglue.transforms as T

    monkeypatch.setenv("PRYTT_STRICT", "1")
    config.reset()

    out = getattr(T, name).apply(frame=people, transformation_ctx="x", **kwargs)

    assert out is people
    assert f"WARN {name}" in capsys.readouterr().err


def test_collection_returning_pass_throughs(people, capsys):
    from awsglue.transforms import Relationalize, SplitFields, SplitRows

    rel = Relationalize.apply(frame=people, staging_path="/tmp/x", name="root")
    split = SplitFields.apply(frame=people, paths=["id"], name1="left", name2="right")
    by_rows = SplitRows.apply(frame=people, comparison_dict={"id": {">": 1}})

    assert list(rel.keys()) == ["root"]
    assert rel.select("root") is people
    assert list(split.keys()) == ["left", "right"]
    assert list(by_rows.keys()) == ["frame1", "frame2"]
    err = capsys.readouterr().err
    assert "WARN Relationalize" in err
    assert "WARN SplitFields" in err
    assert "WARN SplitRows" in err


def test_errors_as_dynamic_frame_is_empty_with_same_schema(people):
    from awsglue.transforms import ErrorsAsDynamicFrame

    out = ErrorsAsDynamicFrame.apply(people)

    assert out.count() == 0
    assert out.schema() == people.schema()


def test_unimplemented_transform_logs_stub_and_returns_frame(people, capsys):
    from awsglue.transforms import FillMissingValues

    assert FillMissingValues.apply(frame=people, missing_values_column="age") is people
    assert "STUB FillMissingValues.apply" in capsys.readouterr().err


def test_unimplemented_transform_raises_in_strict_mode(people, monkeypatch):
    from awsglue.transforms import FlatMap
    from prytt.stub import GlueLocalUnsupported

    monkeypatch.setenv("PRYTT_STRICT", "1")
    config.reset()

    with pytest.raises(GlueLocalUnsupported):
        FlatMap.apply(frame=people, f=lambda f, ctx: f)


def test_importing_transforms_does_not_import_pyspark():
    import subprocess
    import sys

    code = "import sys; from awsglue.transforms import *; print('pyspark' in sys.modules)"
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True)

    assert out.stdout.strip() == "False"
