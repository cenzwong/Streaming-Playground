import metadata
import pyspark_functions as F_
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def transformation(sdf) -> DataFrame:
    return sdf.select(
        F.col("titleId"),
        F.col("ordering").cast(T.IntegerType()),
        F.col("title"),
        F.col("region"),
        F.col("language"),
        F.split("types", pattern=" ").alias("types"),
        F.split("attributes", pattern=" ").alias("attributes"),
        F_.when_mapping(F.col("isOriginalTitle"), {1: True, 0: False}).alias(
            "isOriginalTitle"
        ),
    ).to(metadata.title_akas.schema)
