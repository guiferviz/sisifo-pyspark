import pytest

from sisifo_pyspark import EntitySplitInvalidator
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


def test_entity_row_invalidator(data_collection, users):
    data_collection["users"] = users.drop("reason")

    is_null_task = IsNull(entity="users", column="name")
    invalidator_task = EntitySplitInvalidator(invalidate_if=is_null_task)
    invalidator_task.run(data_collection)

    df_input = data_collection["users"].toPandas()
    df_input = df_input.sort_values("id")
    df_valid = data_collection["users_valid"].toPandas()
    df_valid = df_valid.sort_values("id")
    df_invalid = data_collection["users_invalid"].toPandas()
    df_invalid = df_invalid.sort_values("id")

    assert len(df_input.columns) == 3
    assert len(df_valid.columns) == 3
    assert len(df_invalid.columns) == 3

    assert len(df_input) == 5
    assert len(df_invalid) == 2
    assert len(df_valid) == 3

    assert list(df_valid.id) == [0, 2, 3]
    assert list(df_valid.name) == ["Ignatius", "", "Mancuso"]
    assert list(df_invalid.id) == [1, 4]
    assert list(df_invalid.name) == [None, None]
    assert list(df_input.id) == [0, 1, 2, 3, 4]
    assert list(df_input.name) == ["Ignatius", None, "", "Mancuso", None]
