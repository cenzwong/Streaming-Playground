import metadata
import pyspark_functions as F_
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def transformation(sdf) -> DataFrame:
    return sdf.select(
        F.col("tconst"),
        F.col("parentTconst"),
        F.col("seasonNumber").cast(T.IntegerType()),
        F.col("episodeNumber").cast(T.IntegerType()),
    ).to(metadata.title_episode.schema)
