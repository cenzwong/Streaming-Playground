import metadata
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pysparky import functions as F_


def transformation(sdf) -> DataFrame:
    return sdf.select(
        F.col("nconst"),
        F.col("primaryName"),
        F.col("birthYear").cast(T.IntegerType()),
        F.col("deathYear").cast(T.IntegerType()),
        F.split("primaryProfession", pattern=" ").alias("primaryProfession"),
        F.split("knownForTitles", pattern=" ").alias("knownForTitles"),
    ).to(metadata.name_basics.schema)
