from pyspark.sql import types as T

TABLE_COMMENT = (
    "This table contains information about the principal cast and crew for titles, including unique identifiers for titles and names, "
    "job categories, specific job titles, and character names."
)

COLUMN_COMMENTS = {
    "tconst": "Alphanumeric unique identifier of the title.",
    "ordering": "A number to uniquely identify rows for a given titleId.",
    "nconst": "Alphanumeric unique identifier of the name/person.",
    "category": "The category of job that the person was in.",
    "job": "The specific job title if applicable, else '\\N'.",
    "characters": "The name of the character played if applicable, else '\\N'.",
}

SCHEMA = T.StructType(
    [
        T.StructField("tconst", T.StringType(), True),
        T.StructField("ordering", T.IntegerType(), True),
        T.StructField("nconst", T.StringType(), True),
        T.StructField("category", T.StringType(), True),
        T.StructField("job", T.StringType(), True),
        T.StructField("characters", T.StringType(), True),
    ]
)

for structfield in SCHEMA:
    structfield.metadata["comment"] = COLUMN_COMMENTS.get(structfield.name, "")
