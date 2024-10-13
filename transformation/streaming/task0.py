from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def task0(title_basics_ssdf: DataFrame) -> DataFrame:
    """
    Filters the input DataFrame to return only movies.

    This function takes a DataFrame (`title_basics_sdf`) containing basic information about titles
    and filters it to return only rows where the `titleType` is "movie". It returns a DataFrame
    with the movie's unique identifier (`tconst`), primary title (`primaryTitle`), and title type (`titleType`).

    Args:
        title_basics_ssdf (DataFrame): A Spark Streaming DataFrame containing information about various titles,
            including movies, TV shows, etc. The DataFrame includes columns like `tconst`,
            `primaryTitle`, and `titleType`.

    Returns:
        DataFrame: A Spark Streaming DataFrame containing:
        - `tconst`: The unique identifier for the movie.
        - `primaryTitle`: The primary title of the movie.
        - `titleType`: The type of the title, which is "movie" for all rows in the result.
    """
    # Getting a list of movies
    title_movie_sdf = title_basics_ssdf.filter(F.col("titleType") == "movie").select(
        "tconst", "primaryTitle", "titleType"
    )

    return title_movie_sdf
