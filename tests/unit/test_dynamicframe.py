"""awsglue.dynamicframe: DynamicFrame and DynamicFrameCollection on Sail.

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


def columns(frame):
    return frame.toDF().columns


def ddl(frame):
    return frame.toDF().schema.simpleString()


def test_count_and_schema_delegate_to_the_dataframe(people):
    assert people.count() == 2
    assert people.schema() == people.toDF().schema


def test_show_prints_rows(people, capsys):
    people.show(1)

    assert "ann" in capsys.readouterr().out


def test_from_df_round_trips(spark):
    from awsglue.dynamicframe import DynamicFrame

    df = spark.range(2)
    assert DynamicFrame.fromDF(df, None, "r").toDF() is df


def test_select_fields_keeps_listed_fields_in_order(people):
    out = people.select_fields(["name", "id"])

    assert columns(out) == ["name", "id"]
    assert rows(out) == [("ann", 1), ("bob", 2)]


def test_select_fields_with_nested_path_keeps_structure(people):
    out = people.select_fields(["id", "address.city"])

    assert ddl(out) == "struct<id:int,address:struct<city:string>>"
    assert rows(out) == [(1, ("cpt",)), (2, ("ldn",))]


def test_drop_fields_top_level_and_nested(people):
    out = people.drop_fields(["age", "address.country"])

    assert ddl(out) == "struct<id:int,name:string,address:struct<city:string>>"


def test_rename_field_top_level_keeps_position(people):
    out = people.rename_field("name", "full_name")

    assert columns(out) == ["id", "full_name", "age", "address"]


def test_rename_field_nested(people):
    out = people.rename_field("address.city", "address.town")

    assert ddl(out) == "struct<id:int,name:string,age:string,address:struct<country:string,town:string>>"
    assert rows(out.select_fields(["address.town"])) == [(("cpt",),), (("ldn",),)]


def test_apply_mapping_casts_renames_and_drops_unmapped(people):
    out = people.apply_mapping(
        [
            ("id", "int", "person_id", "long"),
            ("age", "string", "age", "int"),
            ("address.city", "string", "city", "string"),
        ]
    )

    assert ddl(out) == "struct<person_id:bigint,age:int,city:string>"
    assert rows(out) == [(1, 31, "cpt"), (2, 42, "ldn")]


def test_apply_mapping_builds_nested_targets(people):
    out = people.apply_mapping(
        [
            ("id", "int", "id", "int"),
            ("name", "string", "who.name", "string"),
            ("age", "string", "who.age", "int"),
        ]
    )

    assert ddl(out) == "struct<id:int,who:struct<name:string,age:int>>"
    assert rows(out) == [(1, ("ann", 31)), (2, ("bob", 42))]


def test_apply_mapping_backticks_name_a_literal_dotted_column(spark):
    from awsglue.dynamicframe import DynamicFrame

    df = spark.createDataFrame([(1,)], "`a.b` int")
    out = DynamicFrame.fromDF(df, None, "d").apply_mapping([("`a.b`", "int", "ab", "string")])

    assert rows(out) == [("1",)]


def test_resolve_choice_cast_specs_are_real_casts(people):
    out = people.resolveChoice(specs=[("age", "cast:int"), ("address.city", "cast:string")])

    assert ddl(out) == "struct<id:int,name:string,age:int,address:struct<city:string,country:string>>"
    assert rows(out.select_fields(["age"])) == [(31,), (42,)]


def test_resolve_choice_other_actions_warn_and_pass_through(people, capsys):
    out = people.resolveChoice(specs=[("age", "make_cols")], choice="make_struct")

    assert ddl(out) == ddl(people)
    err = capsys.readouterr().err
    assert "WARN" in err and "make_cols" in err and "make_struct" in err


def test_filter_and_map_warn_and_pass_through(people, capsys):
    assert rows(people.filter(lambda r: r["id"] == 1)) == rows(people)
    assert rows(people.map(lambda r: r)) == rows(people)

    err = capsys.readouterr().err
    assert err.count("WARN") == 2


def test_repartition_and_coalesce_keep_rows(people):
    assert rows(people.repartition(3)) == rows(people)
    assert rows(people.coalesce(1)) == rows(people)


def test_error_counts_are_zero(people):
    assert people.errorsCount() == 0
    assert people.stageErrorsCount() == 0


def test_join_on_keys(spark, people):
    from awsglue.dynamicframe import DynamicFrame

    orders = DynamicFrame.fromDF(spark.createDataFrame([(10, 1), (11, 1)], "order_id int, pid int"), None, "o")

    out = people.select_fields(["id", "name"]).join(["id"], ["pid"], orders)

    assert rows(out) == [(1, "ann", 10, 1), (1, "ann", 11, 1)]


def test_unknown_methods_log_stub_and_return_self(people, capsys):
    assert people.relationalize("root", "s3://tmp/") is people
    assert "STUB DynamicFrame.relationalize" in capsys.readouterr().err


def test_methods_return_new_dynamic_frames_with_context(people):
    from awsglue.dynamicframe import DynamicFrame

    out = people.select_fields(["id"])
    assert isinstance(out, DynamicFrame) and out is not people


# --- DynamicFrameCollection ----------------------------------------------------


@pytest.fixture
def collection(people):
    from awsglue.dynamicframe import DynamicFrameCollection

    return DynamicFrameCollection({"a": people, "b": people.select_fields(["id"])}, None)


def test_collection_keys_values_select(collection, people):
    assert list(collection.keys()) == ["a", "b"]
    assert len(collection.values()) == 2
    assert collection.select("a") is people
    assert len(collection) == 2


def test_collection_map_applies_to_each_frame(collection):
    out = collection.map(lambda frame, ctx: frame.select_fields(["id"]))

    assert [columns(out.select(k)) for k in out.keys()] == [["id"], ["id"]]
