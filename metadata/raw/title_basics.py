from pyspark.sql import types as T

SCHEMA = T.StructType(
    [
        T.StructField("tconst", T.StringType(), True),
        T.StructField("titleType", T.StringType(), True),
        T.StructField("primaryTitle", T.StringType(), True),
        T.StructField("originalTitle", T.StringType(), True),
        T.StructField("isAdult", T.StringType(), True),
        T.StructField("startYear", T.StringType(), True),
        T.StructField("endYear", T.StringType(), True),
        T.StructField("runtimeMinutes", T.StringType(), True),
        T.StructField("genres", T.StringType(), True),
    ]
)
