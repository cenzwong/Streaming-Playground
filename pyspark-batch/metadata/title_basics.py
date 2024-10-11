from pyspark.sql import types as T

# title.basics.tsv.gz

# tconst (string) - alphanumeric unique identifier of the title
# titleType (string) - the type/format of the title (e.g. movie, short, tvseries, tvepisode, video, etc)
# primaryTitle (string) - the more popular title / the title used by the filmmakers on promotional materials at the point of release
# originalTitle (string) - original title, in the original language
# isAdult (boolean) - 0: non-adult title; 1: adult title
# startYear (YYYY) - represents the release year of a title. In the case of TV Series, it is the series start year
# endYear (YYYY) - TV Series end year. '\N' for all other title types
# runtimeMinutes - primary runtime of the title, in minutes
# genres (string array) - includes up to three genres associated with the title
schema = T.StructType(
    [
        T.StructField("tconst", T.StringType(), True),
        T.StructField("titleType", T.StringType(), True),
        T.StructField("primaryTitle", T.StringType(), True),
        T.StructField("originalTitle", T.StringType(), True),
        T.StructField("isAdult", T.BooleanType(), True),
        T.StructField("startYear", T.IntegerType(), True),
        T.StructField("endYear", T.StringType(), True),
        T.StructField("runtimeMinutes", T.IntegerType(), True),
        T.StructField("genres", T.ArrayType(T.StringType()), True),
    ]
)
