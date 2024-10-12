from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def task2a(
    top_10_movies_with_min_500_votes_sdf: DataFrame,
    title_principals_sdf: DataFrame,
    name_basics_sdf: DataFrame,
) -> DataFrame:
    """
    Identifies the most credited person for the top 10 movies with over 500 votes.

    This function takes three DataFrames:
    - The top 10 movies that have received more than 500 votes (`top_10_movies_with_min_500_votes_sdf`),
    - A DataFrame containing information about the people credited in the movies (`title_principals_sdf`),
    - A DataFrame containing the names of the people (`name_basics_sdf`).

    The function joins these DataFrames to find the person who has been credited the most across
    the top 10 movies. It returns a DataFrame with the name of the most credited person.

    Args:
        top_10_movies_with_min_500_votes_sdf (DataFrame): A Spark DataFrame of the top 10 ranked
            movies with at least 500 votes. Contains movie IDs (`tconst`).
        title_principals_sdf (DataFrame): A Spark DataFrame containing the principal people
            credited in movies (with columns such as `tconst` for movie IDs and `nconst` for person IDs).
        name_basics_sdf (DataFrame): A Spark DataFrame with basic information about people,
            including their names (`primaryName`) and unique IDs (`nconst`).

    Returns:
        DataFrame: A Spark DataFrame with the name of the most credited person across the
        top 10 movies. The resulting DataFrame contains:
        - `nconst`: The unique identifier for the person.
        - `primaryName`: The name of the most credited person.
    """
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
