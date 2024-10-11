"""
    Summary of transformation:
    - Casting `seasonNumber` and `episodeNumber` to IntegerType
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from metadata.bronze import title_episode


def transformation(sdf) -> DataFrame:
    """
    Transforms the input Spark DataFrame by selecting specific columns and casting data types.



    Args:
        sdf (DataFrame): Input Spark DataFrame containing the following columns:
            - tconst (str): Identifier for the title.
            - parentTconst (str): Identifier for the parent title.
            - seasonNumber (str): Season number, to be cast to IntegerType.
            - episodeNumber (str): Episode number, to be cast to IntegerType.

    Returns:
        DataFrame: Transformed Spark DataFrame with the following SCHEMA:
            - tconst (str): Identifier for the title.
            - parentTconst (str): Identifier for the parent title.
            - seasonNumber (int): Season number as IntegerType.
            - episodeNumber (int): Episode number as IntegerType.
    """
    return sdf.select(
        F.col("tconst"),
        F.col("parentTconst"),
        F.col("seasonNumber").cast(T.IntegerType()),
        F.col("episodeNumber").cast(T.IntegerType()),
    ).to(title_episode.SCHEMA)
