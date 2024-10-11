from pyspark.sql import types as T

SCHEMA = T.StructType(
    [
        T.StructField("tconst", T.StringType(), True),
        T.StructField("directors", T.StringType(), True),
        T.StructField("writers", T.StringType(), True),
    ]
)
