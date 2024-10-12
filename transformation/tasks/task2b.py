from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def task2b(
    top_10_movies_with_min_500_votes_sdf: DataFrame, title_akas_sdf: DataFrame
) -> DataFrame:
    """
    Retrieves alternative titles for the top 10 movies with over 500 votes.

    This function takes two DataFrames:
    - A DataFrame containing the top 10 ranked movies with at least 500 votes
      (`top_10_movies_with_min_500_votes_sdf`).
    - A DataFrame with alternative titles (AKAs) and regions for movies (`title_akas_sdf`).

    It joins these DataFrames to find the alternative titles for each of the top 10 movies
    and returns a DataFrame where each movie is associated with a set of its alternative titles.

    Args:
        top_10_movies_with_min_500_votes_sdf (DataFrame): A Spark DataFrame containing the top 10
            ranked movies with at least 500 votes. It includes columns such as `tconst` for movie IDs
            and `primaryTitle` for the main title of the movie.
        title_akas_sdf (DataFrame): A Spark DataFrame with alternative titles (`title`) for movies,
            regions (`region`), and flags indicating if it's the original title (`isOriginalTitle`).

    Returns:
        DataFrame: A Spark DataFrame containing the top 10 movies along with a collection of their
        alternative titles. The resulting DataFrame includes:
        - `tconst`: The unique identifier for the movie.
        - `primaryTitle`: The main title of the movie.
        - `different_title`: A set of alternative titles for the movie.
    """
    top_10_movies_with_aka_sdf = top_10_movies_with_min_500_votes_sdf.select(
        "tconst", "primaryTitle"
    ).join(
        title_akas_sdf.select("tconst", "title", "region", "isOriginalTitle"),
        on="tconst",
        how="left",
    )

    return top_10_movies_with_aka_sdf.groupBy("tconst", "primaryTitle").agg(
        F.collect_set("title").alias("different_title")
    )
