import pyspark.sql.types as T

from sisifo_pyspark import ZipAndExplode

import pytest


def test_zip_and_explode(spark, data_collection):
    df = spark.createDataFrame([
        [1, ["A", "B"], ["a", "b"]],
        [2, ["C", "D"], ["c", "d"]],
    ], T.StructType([
        T.StructField("id", T.IntegerType()),
        T.StructField("upper", T.ArrayType(T.StringType())),
        T.StructField("lower", T.ArrayType(T.StringType())),
    ]))
    data_collection["abc"] = df

    task = ZipAndExplode(entity_in="abc", entity_out="abc_out",
                         columns=["upper", "lower"])
    task.run(data_collection)

    df = data_collection["abc_out"].toPandas()
    assert len(df) == 4
    assert list(df.columns) == ["id", "upper", "lower"]
    assert list(df.id) == [1, 1, 2, 2]
    assert list(df.upper) == ["A", "B", "C", "D"]
    assert list(df.lower) == ["a", "b", "c", "d"]


def test_zip_and_explode_no_columns():
    with pytest.raises(ValueError) as e:
        ZipAndExplode(entity="abc", columns=[])
    assert "Columns should be a non-empty list" == str(e.value)
