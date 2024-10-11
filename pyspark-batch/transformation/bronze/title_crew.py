import metadata
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pysparky import functions as F_


def transformation(sdf) -> DataFrame:
    return sdf.select(
        F.col("tconst"),
        F.split("directors", pattern=",").alias("directors"),
        F.split("writers", pattern=",").alias("writers"),
    ).to(metadata.title_crew.schema)
