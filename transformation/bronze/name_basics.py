"""Summary of transformation:
- Casting `birthYear` and `deathYear` to IntegerType
- Splitting `primaryProfession` and `knownForTitles` into lists
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

import metadata


def transformation(sdf) -> DataFrame:
    """
    Transforms the input Spark DataFrame by selecting specific columns, casting data types,
    and splitting strings into arrays.



    Args:
        sdf (DataFrame): Input Spark DataFrame containing the following columns:
            - nconst (str): Identifier for the name.
            - primaryName (str): Primary name of the person.
            - birthYear (str): Birth year, to be cast to IntegerType.
            - deathYear (str): Death year, to be cast to IntegerType.
            - primaryProfession (str): Space-separated string of primary professions.
            - knownForTitles (str): Space-separated string of known titles.

    Returns:
        DataFrame: Transformed Spark DataFrame with the following SCHEMA:
            - nconst (str): Identifier for the name.
            - primaryName (str): Primary name of the person.
            - birthYear (int): Birth year as IntegerType.
            - deathYear (int): Death year as IntegerType.
            - primaryProfession (array): Array of primary professions.
            - knownForTitles (array): Array of known titles.
    """
    return sdf.select(
        F.col("nconst"),
        F.col("primaryName"),
        F.col("birthYear").cast(T.IntegerType()),
        F.col("deathYear").cast(T.IntegerType()),
        F.split("primaryProfession", pattern=" ").alias("primaryProfession"),
        F.split("knownForTitles", pattern=" ").alias("knownForTitles"),
    ).to(metadata.bronze.name_basics.SCHEMA)
