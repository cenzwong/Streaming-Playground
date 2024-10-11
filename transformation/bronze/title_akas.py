"""Summary of transformation
- Renaming `titleID` to `tconst`
- Spliting `types` and `attributes` to list
- Casting `isOriginalTitle` to bool
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

import metadata
from pysparky import functions as F_


def transformation(sdf) -> DataFrame:
    """
    Transforms the input Spark DataFrame by selecting and renaming columns, casting data types,
    splitting strings into arrays, and mapping integer values to boolean.

    Args:
        sdf (DataFrame): Input Spark DataFrame containing the following columns:
            - titleId (str): Identifier for the title.
            - ordering (str): Ordering information, to be cast to IntegerType.
            - title (str): Title of the entry.
            - region (str): Region code.
            - language (str): Language code.
            - types (str): Space-separated string of types.
            - attributes (str): Space-separated string of attributes.
            - isOriginalTitle (int): Indicator if the title is original (1 for True, 0 for False).

    Returns:
        DataFrame: Transformed Spark DataFrame with the following SCHEMA:
            - tconst (str): Identifier for the title.
            - ordering (int): Ordering information as IntegerType.
            - title (str): Title of the entry.
            - region (str): Region code.
            - language (str): Language code.
            - types (array): Array of types.
            - attributes (array): Array of attributes.
            - isOriginalTitle (bool): Boolean indicating if the title is original.
    """
    return sdf.select(
        F.col("titleId").alias("tconst"),
        F.col("ordering").cast(T.IntegerType()),
        F.col("title"),
        F.col("region"),
        F.col("language"),
        F.split("types", pattern=" ").alias("types"),
        F.split("attributes", pattern=" ").alias("attributes"),
        F_.when_mapping(F.col("isOriginalTitle"), {1: True, 0: False}).alias(
            "isOriginalTitle"
        ),
    ).to(metadata.bronze.title_akas.SCHEMA)
