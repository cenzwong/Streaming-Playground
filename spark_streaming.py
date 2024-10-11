from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pathlib import Path

# Initialize Spark session
spark = SparkSession.builder.appName("imdb_query_streaming").getOrCreate()

# Define the streaming source
data_folder = "./data/input"
imdb_tables = {}



# Read streaming data
for file in ["name.basics.tsv.gz", "title.akas.tsv.gz", "title.basics.tsv.gz", "title.crew.tsv.gz", "title.episode.tsv.gz", "title.principals.tsv.gz", "title.ratings.tsv.gz"]:
    table_name = "_".join(file.split(".")[:2])
    print(table_name)
    imdb_tables[table_name] = (
        spark.readStream.options(
            sep="\t",
            header=True,
            compression="gzip",
            nullValue=r"\N"
        ).csv(f"{data_folder}/{file}")
    )

# Apply transformations
from transformation import bronze as xform_bronze

bronze_transformation = {
    "name_basics": xform_bronze.name_basics.transformation,
    "title_akas": xform_bronze.title_akas.transformation,
    "title_basics": xform_bronze.title_basics.transformation,
    "title_crew": xform_bronze.title_crew.transformation,
    "title_episode": xform_bronze.title_episode.transformation,
    "title_principals": xform_bronze.title_principals.transformation,
    "title_ratings": xform_bronze.title_ratings.transformation,
}

cleansed_imdb_tables = {
    _table_name: _sdf.transform(bronze_transformation[_table_name])
    for _table_name, _sdf in imdb_tables.items()
}

# Define tasks
from transformation.tasks import task1, task2a, task2b

# Task 1: Retrieve the top 10 movies with a minimum of 500 votes
top_10_movies_with_min_500_votes_sdf = task1(
    title_basics_sdf=cleansed_imdb_tables["title_basics"],
    title_ratings_sdf=cleansed_imdb_tables["title_ratings"],
)

# Write the output to the console
query1 = top_10_movies_with_min_500_votes_sdf.writeStream.outputMode("complete").format("console").start()

# Task 2a: List the persons who are most often credited
query2 = task2a(
    top_10_movies_with_min_500_votes_sdf,
    title_principals_sdf=cleansed_imdb_tables["title_principals"],
    name_basics_sdf=cleansed_imdb_tables["name_basics"],
).writeStream.outputMode("complete").format("console").start()

# Task 2b: List the different titles of the 10 movies
query3 = task2b(
    top_10_movies_with_min_500_votes_sdf,
    title_akas_sdf=cleansed_imdb_tables["title_akas"],
).writeStream.outputMode("complete").format("console").start()

# Await termination
query1.awaitTermination()
query2.awaitTermination()
query3.awaitTermination()
