import metadata
import pyspark_functions as F_
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def transformation(sdf) -> DataFrame:
    return sdf.select(
        F.col("tconst"),
        F.split("directors", pattern=",").alias("directors"),
        F.split("writers", pattern=",").alias("writers"),
    ).to(metadata.title_crew.schema)
