from pyspark.sql import types as T

TABLE_COMMENT = (
    "This table contains information about the crew of titles, including unique identifiers for titles, "
    "and lists of directors and writers associated with each title."
)

COLUMN_COMMENTS = {
    "tconst": "Alphanumeric unique identifier of the title.",
    "directors": "Array of nconsts representing the director(s) of the given title.",
    "writers": "Array of nconsts representing the writer(s) of the given title.",
}

SCHEMA = T.StructType(
    [
        T.StructField("tconst", T.StringType(), True),
        T.StructField("directors", T.ArrayType(T.StringType()), True),
        T.StructField("writers", T.ArrayType(T.StringType()), True),
    ]
)

for structfield in SCHEMA:
    structfield.metadata["comment"] = COLUMN_COMMENTS.get(structfield.name, "")
