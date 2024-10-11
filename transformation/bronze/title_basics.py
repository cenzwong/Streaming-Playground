"""Summary of transformation:
- Casting `isAdult` to bool
- Casting `startYear`, `endYear`, and `runtimeMinutes` to IntegerType
- Splitting `genres` into a list
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from metadata.bronze import title_basics
from pysparky import functions as F_


def transformation(sdf) -> DataFrame:
    """
    Transforms the input Spark DataFrame by selecting specific columns, casting data types,
    splitting strings into arrays, and mapping integer values to boolean.



    Args:
        sdf (DataFrame): Input Spark DataFrame containing the following columns:
            - tconst (str): Identifier for the title.
            - titleType (str): Type of the title.
            - primaryTitle (str): Primary title.
            - originalTitle (str): Original title.
            - isAdult (int): Indicator if the title is for adults (1 for True, 0 for False).
            - startYear (str): Start year, to be cast to IntegerType.
            - endYear (str): End year, to be cast to IntegerType.
            - runtimeMinutes (str): Runtime in minutes, to be cast to IntegerType.
            - genres (str): Comma-separated string of genres.

    Returns:
        DataFrame: Transformed Spark DataFrame with the following SCHEMA:
            - tconst (str): Identifier for the title.
            - titleType (str): Type of the title.
            - primaryTitle (str): Primary title.
            - originalTitle (str): Original title.
            - isAdult (bool): Boolean indicating if the title is for adults.
            - startYear (int): Start year as IntegerType.
            - endYear (int): End year as IntegerType.
            - runtimeMinutes (int): Runtime in minutes as IntegerType.
            - genres (array): Array of genres.
    """
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
    ).to(title_basics.SCHEMA)
