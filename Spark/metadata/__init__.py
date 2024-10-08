from pyspark.sql import types as T

schema = T.StructType([
    T.StructField("titleId", T.StringType(), True),
    T.StructField("ordering", T.IntegerType(), True),
    T.StructField("title", T.StringType(), True),
    T.StructField("region", T.StringType(), True),
    T.StructField("language", T.StringType(), True),
    T.StructField("types", T.ArrayType(T.StringType()), True),
    T.StructField("attributes", T.ArrayType(T.StringType()), True),
    T.StructField("isOriginalTitle", T.BooleanType(), True)
])