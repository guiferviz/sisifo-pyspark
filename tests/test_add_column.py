from sisifo_pyspark import AddColumn


def test_add_bool_with_type(spark, data_collection):
    df = spark.range(2)
    data_collection["range"] = df

    task = AddColumn(entity="range", column="new_col", default=124, dtype="float")
    task.run(data_collection)

    df_output = data_collection["range"].toPandas()

    assert len(df_output.columns) == 2
    assert str(df_output.new_col.dtype) == "float32"
    assert list(df_output.new_col) == [124, 124]


def test_add_bool_without_type(spark, data_collection):
    df = spark.range(3)
    data_collection["range"] = df

    task = AddColumn(entity="range", column="new_col", default=True)
    task.run(data_collection)

    df_output = data_collection["range"].toPandas()

    assert len(df_output.columns) == 2
    assert str(df_output.new_col.dtype) == "bool"
    assert list(df_output.new_col) == [True, True, True]
