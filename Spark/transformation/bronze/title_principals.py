import metadata
import pyspark_functions as F_
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T


def transformation(sdf) -> DataFrame:
    return sdf.select(
        F.col("tconst"),
        F.col("ordering").cast(T.IntegerType()),
        F.col("nconst"),
        F.col("category"),
        F.col("job"),
        F.regexp_extract("characters", pattern=r'\["([^"]+)"\]', idx=1).alias(
            "characters"
        ),
    ).to(metadata.title_principals.schema)
