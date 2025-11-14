import great_expectations as gx

def test_no_null_names(spark):
    data = [("Alice", 30), ("Bob", 25), (None, 40)]
    df = spark.createDataFrame(data, ["name", "age"])

    # Create GX dataset
    gx_df = gx.dataset.SparkDFDataset(df)

    # Validate column "name" has no nulls
    result = gx_df.expect_column_values_to_not_be_null("name")

    assert result["success"] is False  # because we added a None