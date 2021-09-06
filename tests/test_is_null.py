import pytest

from sisifo_pyspark import IsNull


@pytest.fixture
def users(spark):
    df = spark.createDataFrame([
        [0, "Ignatius"],
        [1, None],
        [2, ""],
        [3, "Mancuso"],
        [4, None],
    ], [
        "id", "name",
    ])
    return df


def test_is_null(data_collection, users):
    data_collection["users"] = users

    task = IsNull(entity="users", column_in="name", column_out="is_name_null")
    task.run(data_collection)

    df_output = data_collection["users"].toPandas()
    df_output = df_output.sort_values("id")

    assert len(df_output.columns) == 3
    assert list(df_output.name) == ["Ignatius", None, "", "Mancuso", None]
    assert list(df_output.is_name_null) == [False, True, False, False, True]
