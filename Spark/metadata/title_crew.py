from pyspark.sql import types as T

# title.crew.tsv.gz

# tconst (string) - alphanumeric unique identifier of the title
# directors (array of nconsts) - director(s) of the given title
# writers (array of nconsts) - writer(s) of the given title
schema = T.StructType(
    [
        T.StructField("tconst", T.StringType(), True),
        T.StructField("directors", T.ArrayType(T.StringType()), True),
        T.StructField("writers", T.ArrayType(T.StringType()), True),
    ]
)
