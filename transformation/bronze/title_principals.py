"""
    Summary of transformation:
    - Casting `ordering` to IntegerType
    - Extracting `characters` using a regular expression
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

import metadata


def transformation(sdf: DataFrame) -> DataFrame:
    """
    Transforms the input Spark DataFrame by selecting specific columns, casting data types,
    and extracting characters using a regular expression.



    Args:
        sdf (DataFrame): Input Spark DataFrame containing the following columns:
            - tconst (str): Identifier for the title.
            - ordering (str): Ordering information, to be cast to IntegerType.
            - nconst (str): Identifier for the name.
            - category (str): Category of the principal.
            - job (str): Job description.
            - characters (str): JSON-like string containing characters.

    Returns:
        DataFrame: Transformed Spark DataFrame with the following SCHEMA:
            - tconst (str): Identifier for the title.
            - ordering (int): Ordering information as IntegerType.
            - nconst (str): Identifier for the name.
            - category (str): Category of the principal.
            - job (str): Job description.
            - characters (str): Extracted character name.
    """
    return sdf.select(
        F.col("tconst"),
        F.col("ordering").cast(T.IntegerType()),
        F.col("nconst"),
        F.col("category"),
        F.col("job"),
        F.regexp_extract("characters", pattern=r'\["([^"]+)"\]', idx=1).alias(
            "characters"
        ),
    ).to(metadata.bronze.title_principals.SCHEMA)
