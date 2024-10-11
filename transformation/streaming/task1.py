from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def task1(
    title_basics_sdf: DataFrame, title_ratings_sdf: DataFrame, ranking_logics: Column
) -> DataFrame:

    # Getting a list of movies
    title_movie_sdf = title_basics_sdf.filter(F.col("titleType") == "movie").select(
        "tconst", "primaryTitle"
    )

    title_ratings_sdf = title_ratings_sdf.withColumn("timestamp", F.current_timestamp())

    title_ratings_windowed_sdf = (
        title_ratings_sdf.filter(F.col("numVotes") >= 500)
        .withColumn("ranking", ranking_logics)
        .groupBy(F.window(F.col("timestamp"), "5 seconds"), F.col("tconst"))
        .agg(
            F.max("numVotes").alias("numVotes"),
            F.max("ranking").alias("ranking"),
        )
        .orderBy(F.col("ranking").desc())
        .limit(10)
    )

    movies_with_ranking_sdf = title_ratings_windowed_sdf.join(
        # Filtering the ratings table to movie only
        title_movie_sdf,
        on="tconst",
        how="inner",
    ).select(
        "tconst",
        "primaryTitle",
        F.col("numVotes"),
        F.col("ranking"),
    )

    return movies_with_ranking_sdf
