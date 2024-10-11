import metadata
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pysparky import functions as F_


def transformation(sdf) -> DataFrame:
    return sdf.select(
        F.col("tconst"),
        F.col("averageRating").cast(T.IntegerType()),
        F.col("numVotes").cast(T.IntegerType()),
    ).to(metadata.title_ratings.schema)
