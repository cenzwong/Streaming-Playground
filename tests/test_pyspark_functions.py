import pytest
from pyspark.sql import functions as F

from pysparky import functions as F_


def test_when_mapping(spark):
    """
    Test Cases:
        1. Mapping using column name as a string.
        2. Mapping using Column object.
    """
    dict_ = {1: "a", 2: "b", 3: "c"}
    input_sdf = spark.createDataFrame(data=[(1,), (2,), (3,)], SCHEMA=["key"])
    expected_sdf = spark.createDataFrame(
        data=[(1, "a"), (2, "b"), (3, "c")], SCHEMA=["key", "value"]
    )
    actual_sdf = input_sdf.withColumn(
        "value", F_.when_mapping(column_or_name="key", dict_=dict_)
    )

    assert expected_sdf.collect() == actual_sdf.collect()

    actual2_sdf = input_sdf.withColumn(
        "value", F_.when_mapping(column_or_name=F.col("key"), dict_=dict_)
    )
    assert expected_sdf.collect() == actual2_sdf.collect()


if __name__ == "__main__":
    pytest.main()
