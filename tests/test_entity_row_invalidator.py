import pytest

from sisifo_pyspark import EntityRowInvalidator
from sisifo_pyspark import IsNull


@pytest.fixture
def users(spark):
    df = spark.createDataFrame([
        [0, "Ignatius", False, ["0"]],
        [1, None, True, ["1"]],
        [2, "", True, []],
        [3, "Mancuso", True, None],
        [4, None, True, None],
    ], [
        "id", "name", "valid", "reason",
    ])
    return df


def test_entity_row_invalidator_no_log(data_collection, users):
    data_collection["users"] = users.drop("reason")

    is_null_task = IsNull(entity="users", column="name")
    invalidator_task = EntityRowInvalidator(invalidate_if=is_null_task,
                                            valid_bool_column="valid")
    invalidator_task.run(data_collection)

    df_output = data_collection["users"].toPandas()
    df_output = df_output.sort_values("id")

    assert len(df_output.columns) == 3
    assert list(df_output.name) == ["Ignatius", None, "", "Mancuso", None]
    assert list(df_output.valid) == [False, False, True, True, False]


def test_entity_row_invalidator_log(data_collection, users):
    data_collection["users"] = users

    is_null_task = IsNull(entity="users", column="name")
    invalidator_task = EntityRowInvalidator(invalidate_if=is_null_task,
                                            valid_bool_column="valid",
                                            log_array_columns={
                                                "reason": "Null name",
                                            })
    invalidator_task.run(data_collection)

    df_output = data_collection["users"].toPandas()
    df_output = df_output.sort_values("id")

    assert len(df_output.columns) == 4
    assert list(df_output.name) == ["Ignatius", None, "", "Mancuso", None]
    assert list(df_output.valid) == [False, False, True, True, False]
    assert list(df_output.reason) == [["0"], ["1", "Null name"], [], None, ["Null name"]]
