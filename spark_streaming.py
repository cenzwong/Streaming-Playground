# %%
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# %%
spark = SparkSession.builder.appName("imdb_query_streaming").getOrCreate()
spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", True)
spark.conf.set("spark.sql.shuffle.partitions", 4)

# %%
from pathlib import Path

# Define the streaming source
data_folder = Path("./data/input")
imdb_tables = {}

from metadata import raw

raw_schema = {
    # "name_basics": raw.name_basics.SCHEMA,
    # "title_akas": raw.title_akas.SCHEMA,
    "title_basics": raw.title_basics.SCHEMA,
    # # "title_crew": raw.title_crew.SCHEMA,
    # # "title_episode": raw.title_episode.SCHEMA,
    # "title_principals": raw.title_principals.SCHEMA,
    "title_ratings": raw.title_ratings.SCHEMA,
}

reader_options = {
    "sep": "\t",
    "header": True,
    "nullValue": r"\N",
    # "compression": "gzip",
    "cleanSource": "archive",
    "sourceArchiveDir": "./data/archive/title_ratings",
    "maxFilesPerTrigger": 1,
}

imdb_tables["title_ratings"] = (
    spark.readStream.options(**reader_options)
    .schema(raw_schema["title_ratings"])
    .csv(f"{data_folder}/{"title_ratings"}")
)


batch_reader_options = {
    "sep": "\t",
    "header": True,
    "nullValue": r"\N",
    "compression": "gzip",
}

imdb_tables["title_basics"] = (
    spark.read.options(**batch_reader_options)
    .schema(raw_schema["title_basics"])
    .csv(f"{data_folder}/{"title_basics"}")
)

# Read streaming data
# for file in data_folder.glob("**/*.tsv.gz"):
#     table_name = "_".join(file.name.split(".")[:2])
#     print(table_name)
#     imdb_tables[table_name] = (
#         spark.readStream.options(**reader_options)
#         .schema(raw_schema[table_name])
#         .csv(f"{data_folder}/{table_name}")
#     )

# %%
from transformation import bronze as xform_bronze

bronze_transformation = {
    # "name_basics": xform_bronze.name_basics.transformation,
    # "title_akas": xform_bronze.title_akas.transformation,
    "title_basics": xform_bronze.title_basics.transformation,
    # # # "title_crew": xform_bronze.title_crew.transformation,
    # # # "title_episode": xform_bronze.title_episode.transformation,
    # "title_principals": xform_bronze.title_principals.transformation,
    "title_ratings": xform_bronze.title_ratings.transformation,
}

cleansed_imdb_tables = {
    _table_name: _sdf.transform(bronze_transformation[_table_name])
    for _table_name, _sdf in imdb_tables.items()
}

# %%
from transformation.streaming import task1

# %%
# Ranking Logics is given from the PDF
ranking_logics = (
    F.col("numVotes")
    * F.col("averageRating")
    # / F.lit(average_number_of_voters)
)
# top_10_movies_with_min_500_votes_sdf
top_10_movies_with_min_500_votes_sdf = task1(
    title_basics_sdf=cleansed_imdb_tables["title_basics"],
    title_ratings_sdf=cleansed_imdb_tables["title_ratings"],
    ranking_logics=ranking_logics,
)

# %%
# Write the output to the console
writer_options = {"path": "data/output/query1", "checkpointLocation": "data/.ckpt/q1"}
query1 = (
    top_10_movies_with_min_500_votes_sdf.writeStream.outputMode("complete")
    .options(**writer_options)
    .format("console")
    .start()
)

# %%
# Task 2a: List the persons who are most often credited
# query2a = (
#     task2a(
#         top_10_movies_with_min_500_votes_sdf,
#         title_principals_sdf=cleansed_imdb_tables["title_principals"],
#         name_basics_sdf=cleansed_imdb_tables["name_basics"],
#     )
#     .writeStream.outputMode("complete")
#     .options(
#         path="data/output/query2a",
#         checkpointLocation = "data/.ckpt/q2a"
#     )
#     .format("csv")
#     .start()
# )

# %%
# Task 2b: List the different titles of the 10 movies
# query2b = (
#     task2b(
#         top_10_movies_with_min_500_votes_sdf,
#         title_akas_sdf=cleansed_imdb_tables["title_akas"],
#     )
#     .writeStream.outputMode("complete")
#     .options(
#         path="data/output/query2b",
#         checkpointLocation = "data/.ckpt/q2b"
#     )
#     .format("csv")
#     .start()
# )

# %%
# Await termination
query1.awaitTermination()
# query2a.awaitTermination()
# query2b.awaitTermination()

# %%
# from metadata import raw

# spark.conf.set("spark.sql.streaming.schemaInference", True)
# spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", True)

# title_akas_sdf = (
#     spark.readStream.options(
#         **{
#             "sep": "\t",
#             "header": True,
#             "nullValue": r"\N",
#             "compression": "gzip",
#             "cleanSource": "archive",
#             "sourceArchiveDir": "./data/archive",
#             "maxFilesPerTrigger": 1,
#         }
#     )
#     # .schema(raw.title_akas.SCHEMA)
#     .format("csv").load(f"data/input")
# )

# (
#     title_akas_sdf.limit(1)
#     .writeStream.format("console")
#     .outputMode("complete")
#     .options(
#         **{
#             # "path": "data/output",
#             "checkpointLocation": "data/.checkpoint_dir"
#         }
#     )
#     .start()
#     .awaitTermination()
# )

# %%
# from pathlib import Path
# from metadata import raw

# imdb_tables = {}
# data_folder = Path("./data")

# for file in list(data_folder.glob("*.tsv.gz")):
#     table_name = "_".join(file.name.split(".")[:2])
#     print(table_name)
#     imdb_tables[table_name] = (
#         spark.readStream.options(
#             **{
#                 "sep": "\t",
#                 "header": True,
#                 "compression": "gzip",
#                 "nullValue": r"\N",
#             }
#         )
#         .schema(raw.title_akas.SCHEMA)
#         .csv(f"./data/{file.name}")
#         # .limit(100)
#     )
