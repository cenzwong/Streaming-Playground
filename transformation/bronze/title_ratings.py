"""Summary of transformation
- Casting `averageRating`, `numVotes` to integer
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import types as T

from metadata.bronze import title_ratings


def transformation(sdf: DataFrame) -> DataFrame:
    """
    Transforms the input Spark DataFrame by selecting specific columns and casting data types.

    Args:
        sdf (DataFrame): Input Spark DataFrame containing the following columns:
            - tconst (str): Identifier for the title.
            - averageRating (str): Average rating, to be cast to IntegerType.
            - numVotes (str): Number of votes, to be cast to IntegerType.

    Returns:
        DataFrame: Transformed Spark DataFrame with the following SCHEMA:
            - tconst (str): Identifier for the title.
            - averageRating (int): Average rating as IntegerType.
            - numVotes (int): Number of votes as IntegerType.
    """
    return sdf.select(
        F.col("tconst"),
        F.col("averageRating").cast(T.IntegerType()),
        F.col("numVotes").cast(T.IntegerType()),
    ).to(title_ratings.SCHEMA)
