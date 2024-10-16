from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def task1(
    title_basics_sdf: DataFrame, title_ratings_ssdf: DataFrame, ranking_logics: Column
) -> DataFrame:
    """
    Filters and ranks movies based on a given ranking logic and a minimum number of votes, using a window function.

    This function processes two Spark DataFrames:
    - A DataFrame containing basic information about titles (`title_basics_sdf`).
    - A Streaming DataFrame containing ratings information for titles (`title_ratings_ssdf`), where it adds a
      timestamp for each record and groups data into windows of 5 seconds.

    The function filters for movies, selects those with at least 500 votes, and applies the provided
    ranking logic to determine the top 10 movies. The result is a DataFrame containing the movies
    with their number of votes and rankings, grouped within 5-second windows.

    Args:
        title_basics_sdf (DataFrame): A Spark DataFrame with basic title information, including
            `tconst` (movie IDs) and `primaryTitle` (movie titles).
        title_ratings_ssdf (DataFrame): A Spark Streaming DataFrame containing movie ratings, including `tconst`
            (movie IDs), `numVotes` (number of votes), and other rating metrics.
        ranking_logics (Column): A Spark column defining the ranking logic based on which movies
            are ordered (e.g., based on rating, number of votes, or a combination of metrics).

    Returns:
        DataFrame: A Spark DataFrame containing the top 10 ranked movies with the following columns:
        - `tconst`: The unique identifier for the movie.
        - `primaryTitle`: The title of the movie.
        - `numVotes`: The number of votes the movie received.
        - `ranking`: The ranking value based on the provided logic.
    """
    # Getting a list of movies
    title_movie_sdf = title_basics_sdf.filter(F.col("titleType") == "movie").select(
        "tconst", "primaryTitle"
    )

    if title_ratings_ssdf.isStreaming:

        movies_with_ranking_ssdf = (
            title_ratings_ssdf.filter(F.col("numVotes") >= 500)
            .join(
                # Filtering the ratings table to movie only
                title_movie_sdf,
                on="tconst",
                how="inner",
            )
            .select(
                F.current_timestamp().alias("timestamp"),
                "tconst",
                "primaryTitle",
                F.col("numVotes"),
                ranking_logics.alias("ranking"),
            )
            .groupBy(
                F.window(F.col("timestamp"), "5 seconds"),
                F.col("tconst"),
                F.col("primaryTitle"),
            )
            .agg(
                F.max("numVotes").alias("numVotes"),
                F.max("ranking").alias("ranking"),
            )
            .orderBy(F.col("ranking").desc())
            .limit(10)
        )

    else:
        raise Exception("title_ratings_ssdf should be a streaming table.")

    return movies_with_ranking_ssdf
