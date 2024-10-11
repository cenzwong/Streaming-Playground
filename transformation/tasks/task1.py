from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def task1(
    title_basics_sdf: DataFrame, title_ratings_sdf: DataFrame, ranking_logics: Column
) -> DataFrame:
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
