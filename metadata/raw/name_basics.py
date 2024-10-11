from pyspark.sql import types as T

SCHEMA = T.StructType(
    [
        T.StructField("nconst", T.StringType(), True),
        T.StructField("primaryName", T.StringType(), True),
        T.StructField("birthYear", T.StringType(), True),
        T.StructField("deathYear", T.StringType(), True),
        T.StructField("primaryProfession", T.StringType(), True),
        T.StructField("knownForTitles", T.StringType(), True),
    ]
)
