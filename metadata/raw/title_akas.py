from pyspark.sql import types as T

SCHEMA = T.StructType(
    [
        T.StructField("titleId", T.StringType(), True),
        T.StructField("ordering", T.StringType(), True),
        T.StructField("title", T.StringType(), True),
        T.StructField("region", T.StringType(), True),
        T.StructField("language", T.StringType(), True),
        T.StructField("types", T.StringType(), True),
        T.StructField("attributes", T.StringType(), True),
        T.StructField("isOriginalTitle", T.StringType(), True),
    ]
)
