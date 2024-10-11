from pyspark.sql import types as T

TABLE_COMMENT = (
    "This table contains basic information about titles, including unique identifiers, title types, "
    "primary and original titles, adult content indicators, release years, runtime, and genres."
)

COLUMN_COMMENTS = {
    "tconst": "Alphanumeric unique identifier of the title.",
    "titleType": "The type/format of the title (e.g., movie, short, tvseries, tvepisode, video, etc.).",
    "primaryTitle": "The more popular title / the title used by the filmmakers on promotional materials at the point of release.",
    "originalTitle": "Original title, in the original language.",
    "isAdult": "Boolean indicating if the title is for adults (0: non-adult title; 1: adult title).",
    "startYear": "Represents the release year of a title. In the case of TV Series, it is the series start year.",
    "endYear": "TV Series end year. '\\N' for all other title types.",
    "runtimeMinutes": "Primary runtime of the title, in minutes.",
    "genres": "Includes up to three genres associated with the title.",
}


SCHEMA = T.StructType(
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

for structfield in SCHEMA:
    structfield.metadata["comment"] = COLUMN_COMMENTS.get(structfield.name, "")
