from pyspark.sql import types as T

# title.akas.tsv.gz

# titleId (string) - a tconst, an alphanumeric unique identifier of the title
# ordering (integer) - a number to uniquely identify rows for a given titleId
# title (string) - the localized title
# region (string) - the region for this version of the title
# language (string) - the language of the title
# types (array) - Enumerated set of attributes for this alternative title. One or more of the following: "alternative", "dvd", "festival", "tv", "video", "working", "original", "imdbDisplay". New values may be added in the future without warning
# attributes (array) - Additional terms to describe this alternative title, not enumerated
# isOriginalTitle (boolean) - 0: not original title; 1: original title
schema = T.StructType(
    [
        T.StructField("titleId", T.StringType(), True),
        T.StructField("ordering", T.IntegerType(), True),
        T.StructField("title", T.StringType(), True),
        T.StructField("region", T.StringType(), True),
        T.StructField("language", T.StringType(), True),
        T.StructField("types", T.ArrayType(T.StringType()), True),
        T.StructField("attributes", T.ArrayType(T.StringType()), True),
        T.StructField("isOriginalTitle", T.BooleanType(), True),
    ]
)
