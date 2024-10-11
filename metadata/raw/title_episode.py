from pyspark.sql import types as T

SCHEMA = T.StructType(
    [
        T.StructField("tconst", T.StringType(), True),
        T.StructField("parentTconst", T.StringType(), True),
        T.StructField("seasonNumber", T.StringType(), True),
        T.StructField("episodeNumber", T.StringType(), True),
    ]
)
