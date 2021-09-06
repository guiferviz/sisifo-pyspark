from sisifo_pyspark import AddN


def test_add_bool_with_type(spark, data_collection):
    df = spark.range(2)
    data_collection["range"] = df

    task = AddN(entity="range", column_in="id", column_out="new_col", n=2)
    task.run(data_collection)

    df_output = data_collection["range"].toPandas()

    assert len(df_output.columns) == 2
    assert str(df_output.id.dtype) == "int64"
    assert str(df_output.new_col.dtype) == "int64"
    assert list(df_output.new_col) == [2, 3]
