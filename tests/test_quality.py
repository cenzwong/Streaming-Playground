import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pysparky import quality

# Initialize Spark session
spark = SparkSession.builder.master("local").appName("pytest").getOrCreate()

# Sample DataFrame for testing
data = [("Alice", 1), ("Bob", 2), ("Charlie", 3), ("Alice", 4)]
columns = ["name", "id"]
df = spark.createDataFrame(data, columns)

# Test functions
@quality.expect_unique("id")
def get_df_unique():
    return df

@quality.expect_criteria(F.col("id") > 0)
def get_df_criteria():
    return df

# Pytest test cases
def test_expect_unique():
    get_df_unique()

def test_expect_criteria():
    get_df_criteria()  # Should pass without assertion error

if __name__ == "__main__":
    pytest.main()