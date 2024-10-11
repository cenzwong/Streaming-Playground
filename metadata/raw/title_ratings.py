from pyspark.sql import types as T

SCHEMA = T.StructType(
    [
        T.StructField("tconst", T.StringType(), True),
        T.StructField("averageRating", T.StringType(), True),
        T.StructField("numVotes", T.StringType(), True),
    ]
)
