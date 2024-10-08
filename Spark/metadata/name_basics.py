from pyspark.sql import types as T

# name.basics.tsv.gz

# nconst (string) - alphanumeric unique identifier of the name/person
# primaryName (string)- name by which the person is most often credited
# birthYear - in YYYY format
# deathYear - in YYYY format if applicable, else '\N'
# primaryProfession (array of strings)- the top-3 professions of the person
# knownForTitles (array of tconsts) - titles the person is known for
schema = T.StructType(
    [
        T.StructField("nconst", T.StringType(), True),
        T.StructField("primaryName", T.StringType(), True),
        T.StructField("birthYear", T.IntegerType(), True),
        T.StructField("deathYear", T.IntegerType(), True),
        T.StructField("primaryProfession", T.ArrayType(T.StringType()), True),
        T.StructField("knownForTitles", T.ArrayType(T.StringType()), True),
    ]
)
