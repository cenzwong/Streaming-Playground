from pyspark.sql import types as T

TABLE_COMMENT = (
    "This table contains information about TV series episodes, including unique identifiers for episodes, "
    "parent TV series identifiers, and season and episode numbers."
)

COLUMN_COMMENTS = {
    "tconst": "Alphanumeric identifier of the episode.",
    "parentTconst": "Alphanumeric identifier of the parent TV series.",
    "seasonNumber": "Season number the episode belongs to.",
    "episodeNumber": "Episode number of the tconst in the TV series.",
}

SCHEMA = T.StructType(
    [
        T.StructField("tconst", T.StringType(), True),
        T.StructField("parentTconst", T.StringType(), True),
        T.StructField("seasonNumber", T.IntegerType(), True),
        T.StructField("episodeNumber", T.IntegerType(), True),
    ]
)

for structfield in SCHEMA:
    structfield.metadata["comment"] = COLUMN_COMMENTS.get(structfield.name, "")
