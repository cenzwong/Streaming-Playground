from pyspark.sql import types as T

SCHEMA = T.StructType(
    [
        T.StructField("tconst", T.StringType(), True),
        T.StructField("ordering", T.StringType(), True),
        T.StructField("nconst", T.StringType(), True),
        T.StructField("category", T.StringType(), True),
        T.StructField("job", T.StringType(), True),
        T.StructField("characters", T.StringType(), True),
    ]
)
