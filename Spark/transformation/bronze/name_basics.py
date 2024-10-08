import metadata
import pyspark_functions as F_
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def transformation(sdf) -> DataFrame:
    return sdf.select(
        F.col("nconst"),
        F.col("primaryName"),
        F.col("birthYear").cast(T.IntegerType()),
        F.col("deathYear").cast(T.IntegerType()),
        F.split("primaryProfession", pattern=" ").alias("primaryProfession"),
        F.split("knownForTitles", pattern=" ").alias("knownForTitles"),
    ).to(metadata.name_basics.schema)
