import metadata
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pysparky import functions as F_


def transformation(sdf) -> DataFrame:
    return sdf.select(
        F.col("tconst"),
        F.col("titleType"),
        F.col("primaryTitle"),
        F.col("originalTitle"),
        F_.when_mapping(F.col("isAdult"), {1: True, 0: False}).alias("isAdult"),
        F.col("startYear").cast(T.IntegerType()),
        F.col("endYear").cast(T.IntegerType()),
        F.col("runtimeMinutes").cast(T.IntegerType()),
        F.split("genres", pattern=",").alias("genres"),
    ).to(metadata.title_basics.schema)
