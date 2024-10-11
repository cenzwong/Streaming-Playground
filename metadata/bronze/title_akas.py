from pyspark.sql import types as T

TABLE_COMMENT = (
    "This table contains alternative titles for movies and TV shows, including localized titles, "
    "region and language information, and attributes describing the type and nature of the titles."
)

COLUMN_COMMENTS = {
    "tconst": "Alphanumeric unique identifier of the title.",
    "ordering": "A number to uniquely identify rows for a given titleId.",
    "title": "The localized title.",
    "region": "The region for this version of the title.",
    "language": "The language of the title.",
    "types": (
        "Enumerated set of attributes for this alternative title. One or more of the following: "
        '"alternative", "dvd", "festival", "tv", "video", "working", "original", "imdbDisplay". '
        "New values may be added in the future without warning."
    ),
    "attributes": "Additional terms to describe this alternative title, not enumerated.",
    "isOriginalTitle": "Boolean indicating if this is the original title (0: not original title; 1: original title).",
}


SCHEMA = T.StructType(
    [
        T.StructField("tconst", T.StringType(), True),
        T.StructField("ordering", T.IntegerType(), True),
        T.StructField("title", T.StringType(), True),
        T.StructField("region", T.StringType(), True),
        T.StructField("language", T.StringType(), True),
        T.StructField("types", T.ArrayType(T.StringType()), True),
        T.StructField("attributes", T.ArrayType(T.StringType()), True),
        T.StructField("isOriginalTitle", T.BooleanType(), True),
    ]
)

for structfield in SCHEMA:
    structfield.metadata["comment"] = COLUMN_COMMENTS.get(structfield.name, "")
