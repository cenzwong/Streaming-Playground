from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def task2a(
    top_10_movies_with_min_500_votes_sdf: DataFrame,
    title_principals_sdf: DataFrame,
    name_basics_sdf: DataFrame,
) -> DataFrame:
    top10_movies_with_credited_person_sdf = top_10_movies_with_min_500_votes_sdf.select(
        "tconst"
    ).join(
        title_principals_sdf.select("tconst", "nconst"),
        on="tconst",
        how="inner",  # Some data is missing in title_principals
    )

    most_credited_person_for_top10_movies_sdf = (
        top10_movies_with_credited_person_sdf.groupBy("nconst")
        .count()
        .groupBy()
        .agg(F.max_by("nconst", "count").alias("nconst"))
    )

    most_credited_person_for_top10_movies_with_names_sdf = (
        most_credited_person_for_top10_movies_sdf.join(
            name_basics_sdf.select(
                "nconst",
                "primaryName",
            ),
            on="nconst",
            how="left",
        )
    )

    return most_credited_person_for_top10_movies_with_names_sdf
