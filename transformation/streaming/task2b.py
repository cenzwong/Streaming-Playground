from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def task2b(
    top_10_movies_with_min_500_votes_sdf: DataFrame, title_akas_sdf: DataFrame
) -> DataFrame:
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
