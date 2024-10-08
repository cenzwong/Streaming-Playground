from pyspark.sql import types as T

# title.principals.tsv.gz

# tconst (string) - alphanumeric unique identifier of the title
# ordering (integer) - a number to uniquely identify rows for a given titleId
# nconst (string) - alphanumeric unique identifier of the name/person
# category (string) - the category of job that person was in
# job (string) - the specific job title if applicable, else '\N'
# characters (string) - the name of the character played if applicable, else '\N'
schema = T.StructType(
    [
        T.StructField("tconst", T.StringType(), True),
        T.StructField("ordering", T.IntegerType(), True),
        T.StructField("nconst", T.StringType(), True),
        T.StructField("category", T.StringType(), True),
        T.StructField("job", T.StringType(), True),
        T.StructField("characters", T.StringType(), True),
    ]
)
