from pyspark.sql import types as T

TABLE_COMMENT = (
    "This table contains basic information about individuals, including their unique identifier, "
    "name, birth and death years, primary professions, and titles they are known for."
)

COLUMN_COMMENTS = {
    "nconst": "Alphanumeric unique identifier of the name/person.",
    "primaryName": "Name by which the person is most often credited.",
    "birthYear": "Birth year in YYYY format.",
    "deathYear": "Death year in YYYY format if applicable, else '\\N'.",
    "primaryProfession": "The top-3 professions of the person.",
    "knownForTitles": "Titles the person is known for, represented as an array of tconsts.",
}


SCHEMA = T.StructType(
    [
        T.StructField("nconst", T.StringType(), True),
        T.StructField("primaryName", T.StringType(), True),
        T.StructField("birthYear", T.IntegerType(), True),
        T.StructField("deathYear", T.IntegerType(), True),
        T.StructField("primaryProfession", T.ArrayType(T.StringType()), True),
        T.StructField("knownForTitles", T.ArrayType(T.StringType()), True),
    ]
)

for structfield in SCHEMA:
    structfield.metadata["comment"] = COLUMN_COMMENTS.get(structfield.name, "")
