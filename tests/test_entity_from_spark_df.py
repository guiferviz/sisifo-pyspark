import pyspark.sql.types as T

import pytest

from sisifo_pyspark import EntityFromSparkDF
from sisifo_pyspark.entity_from_spark_df import dict_to_struct_type


def test_dict_to_struct_type(spark):  # spark is needed here!
    schema = dict_to_struct_type({
        "a": "integer",
        "b": "string",
        "c": "array<string>",
        "d": "map<array<boolean>, Decimal(16, 8)>",
    })

    expected_map_type = T.MapType(
        T.ArrayType(T.BooleanType(), True),
        T.DecimalType(16, 8),
        True,
    )
    expected = T.StructType([
        T.StructField("a", T.IntegerType(), True),
        T.StructField("b", T.StringType(), True),
        T.StructField("c", T.ArrayType(T.StringType(), True), True),
        T.StructField("d", expected_map_type, True),
    ])
    assert schema == expected


def test_dict_to_struct_type_recursive(spark):  # spark is needed here!
    schema = dict_to_struct_type({
        "a": {
            "b": {
                "c": "INTEGER",
            },
        },
    })

    expected = T.StructType([
        T.StructField("a", T.StructType([
            T.StructField("b", T.StructType([
                T.StructField("c", T.IntegerType(), True),
            ]), True),
        ]), True),
    ])
    assert schema == expected


def test_entity_from_spark_df_no_schema(spark, data_collection):
    data = [
        [1, "hi"],
        [2, "bye"],
    ]
    task = EntityFromSparkDF(entity_out="out", data=data)
    task.run(data_collection)
    df = data_collection["out"].toPandas()
    dtypes = data_collection["out"].dtypes

    assert len(df) == 2
    assert dtypes == [("_1", "bigint"), ("_2", "string")]
    assert list(df._1) == [1, 2]
    assert list(df._2) == ["hi", "bye"]


def test_entity_from_spark_df_with_column_names(spark, data_collection):
    data = [
        [1, "hi"],
        [2, "bye"],
    ]
    schema = ["a", "b"]
    task = EntityFromSparkDF(entity_out="out", data=data, schema=schema)
    task.run(data_collection)
    df = data_collection["out"].toPandas()
    dtypes = data_collection["out"].dtypes

    assert len(df) == 2
    assert dtypes == [("a", "bigint"), ("b", "string")]
    assert list(df.a) == [1, 2]
    assert list(df.b) == ["hi", "bye"]


def test_entity_from_spark_df_with_invalid_schema(spark, data_collection):
    with pytest.raises(ValueError) as e:
        EntityFromSparkDF(entity_out="out", data=[], schema="schema")
    assert "Schema must be a list or a dict" == str(e.value)


def test_entity_from_spark_df_with_no_data(spark, data_collection):
    task = EntityFromSparkDF(entity_out="out")
    with pytest.raises(ValueError) as e:
        task.run(data_collection)
    assert "can not infer schema from empty dataset" in str(e.value)


def test_entity_from_spark_df_with_schema(spark, data_collection):
    data = [
        [1, "hi"],
        [2, "bye"],
    ]
    schema = {
        "a": "int",
        "b": "string",
    }
    task = EntityFromSparkDF(entity_out="out", data=data, schema=schema)
    task.run(data_collection)
    df = data_collection["out"].toPandas()
    dtypes = data_collection["out"].dtypes

    assert len(df) == 2
    assert dtypes == [("a", "int"), ("b", "string")]
    assert list(df.a) == [1, 2]
    assert list(df.b) == ["hi", "bye"]
