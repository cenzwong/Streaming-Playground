"""
Summary of transformation:
- Selecting `tconst`, `directors`, and `writers`
- Splitting `directors` and `writers` into lists
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

import metadata


def transformation(sdf) -> DataFrame:
    """
    Transforms the input Spark DataFrame by selecting specific columns and splitting strings into arrays.



    Args:
        sdf (DataFrame): Input Spark DataFrame containing the following columns:
            - tconst (str): Identifier for the title.
            - directors (str): Comma-separated string of directors.
            - writers (str): Comma-separated string of writers.

    Returns:
        DataFrame: Transformed Spark DataFrame with the following SCHEMA:
            - tconst (str): Identifier for the title.
            - directors (array): Array of directors.
            - writers (array): Array of writers.
    """
    return sdf.select(
        F.col("tconst"),
        F.split("directors", pattern=",").alias("directors"),
        F.split("writers", pattern=",").alias("writers"),
    ).to(metadata.bronze.title_crew.SCHEMA)
