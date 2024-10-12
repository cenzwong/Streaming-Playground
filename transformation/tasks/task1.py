from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def task1(
    title_basics_sdf: DataFrame, title_ratings_sdf: DataFrame, ranking_logics: Column
) -> DataFrame:
    """
    Filters and ranks movies based on a given ranking logic and a minimum number of votes.

    This function takes in two Spark DataFrames (`title_basics_sdf` and `title_ratings_sdf`)
    along with a ranking logic (`ranking_logics`) and returns the top 10 movies that meet the
    following criteria:
    - The movie type is "movie".
    - The movie has received at least 500 votes.

    The movies are ranked according to the logic provided by the `ranking_logics` column,
    which is applied after filtering for votes and movie type. The result is a DataFrame
    containing the top 10 movies with the highest ranking values.

    Args:
        title_basics_sdf (DataFrame): A Spark DataFrame containing basic information about titles
            (such as `tconst`, `primaryTitle`, and `titleType`).
        title_ratings_sdf (DataFrame): A Spark DataFrame containing ratings information for titles
            (including `tconst`, `numVotes`, and average ratings).
        ranking_logics (Column): A Spark column that defines the logic for ranking the movies
            (e.g., based on rating, number of votes, or a custom combination of these).

    Returns:
        DataFrame: A Spark DataFrame with the top 10 ranked movies that have a minimum of 500 votes.
        The DataFrame includes the following columns:
        - `tconst`: The unique identifier for the movie.
        - `primaryTitle`: The movie's title.
        - `numVotes`: The number of votes the movie received.
        - `ranking`: The ranking value based on the provided ranking logic.
    """
    # Getting a list of movies
    title_movie_sdf = title_basics_sdf.filter(F.col("titleType") == "movie").select(
        "tconst", "primaryTitle"
    )

    top_10_movies_with_min_500_votes_sdf = (
        title_ratings_sdf.filter(F.col("numVotes") >= 500)
        .join(
            # Filtering the ratings table to movie only
            title_movie_sdf,
            on="tconst",
            how="inner",
        )
        .select(
            "tconst",
            "primaryTitle",
            F.col("numVotes"),
            ranking_logics.alias("ranking"),
        )
        .orderBy(F.col("ranking").desc())
        .limit(10)
    )

    # top_10_movies_with_min_500_votes_sdf.show(truncate=False)
    return top_10_movies_with_min_500_votes_sdf
