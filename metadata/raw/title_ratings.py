from pyspark.sql import types as T

TABLE_COMMENT = (
    "This table contains information about the ratings of titles, including unique identifiers for titles, "
    "average ratings, and the number of votes received."
)

COLUMN_COMMENTS = {
    "tconst": "Alphanumeric unique identifier of the title.",
    "averageRating": "Weighted average of all the individual user ratings.",
    "numVotes": "Number of votes the title has received.",
}

SCHEMA = T.StructType(
    [
        T.StructField("tconst", T.StringType(), True),
        T.StructField("averageRating", T.FloatType(), True),
        T.StructField("numVotes", T.IntegerType(), True),
    ]
)

for structfield in SCHEMA:
    structfield.metadata["comment"] = COLUMN_COMMENTS.get(structfield.name, "")
