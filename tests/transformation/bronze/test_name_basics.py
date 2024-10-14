import pytest

from pyspark.sql import types as T
# Import the transformation function
from transformation.bronze.name_basics import transformation

def test_transformation(spark):
    # Sample data as list of dictionaries
    data = [
        {"nconst": "nm0000001", "primaryName": "Fred Astaire", "birthYear": "1899", "deathYear": "1987", "primaryProfession": "actor soundtrack", "knownForTitles": "tt0072308 tt0053137"},
        {"nconst": "nm0000002", "primaryName": "Lauren Bacall", "birthYear": "1924", "deathYear": "2014", "primaryProfession": "actress soundtrack", "knownForTitles": "tt0037382 tt0071877"}
    ]

    schema = T.StructType([
        T.StructField("nconst", T.StringType(), True),
        T.StructField("primaryName", T.StringType(), True),
        T.StructField("birthYear", T.StringType(), True),
        T.StructField("deathYear", T.StringType(), True),
        T.StructField("primaryProfession", T.StringType(), True),
        T.StructField("knownForTitles", T.StringType(), True)
    ])

    sdf = spark.createDataFrame(data, schema)

    result_df = transformation(sdf)

    # Expected schema
    expected_schema = T.StructType([
        T.StructField("nconst", T.StringType(), True),
        T.StructField("primaryName", T.StringType(), True),
        T.StructField("birthYear", T.IntegerType(), True),
        T.StructField("deathYear", T.IntegerType(), True),
        T.StructField("primaryProfession", T.ArrayType(T.StringType(), containsNull=False), True),
        T.StructField("knownForTitles", T.ArrayType(T.StringType(), containsNull=False), True)
    ])

    expected_data = [
        {"nconst": "nm0000001", "primaryName": "Fred Astaire", "birthYear": 1899, "deathYear": 1987, "primaryProfession": ["actor", "soundtrack"], "knownForTitles": ["tt0072308", "tt0053137"]},
        {"nconst": "nm0000002", "primaryName": "Lauren Bacall", "birthYear": 1924, "deathYear": 2014, "primaryProfession": ["actress", "soundtrack"], "knownForTitles": ["tt0037382", "tt0071877"]}
    ]

    expected_df = spark.createDataFrame(expected_data, expected_schema)

    assert result_df.schema.simpleString() == expected_schema.simpleString()

    assert result_df.collect() == expected_df.collect()

if __name__ == "__main__":
    pytest.main()