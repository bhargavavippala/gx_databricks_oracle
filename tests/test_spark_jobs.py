def test_row_count(spark):
    data = [("Alice", 30), ("Bob", 25), ("Charlie", 40)]
    df = spark.createDataFrame(data, ["name", "age"])

    assert df.count() == 3