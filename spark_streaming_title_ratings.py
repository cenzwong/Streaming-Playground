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
batch_data_folder = Path("./data/raw")
imdb_tables = {}

from metadata import raw

raw_schema = {
    "title_basics": raw.title_basics.SCHEMA,
    "title_ratings": raw.title_ratings.SCHEMA,
}

reader_options = {
    "sep": "\t",
    # "header": True,
    "nullValue": r"\N",
    "cleanSource": "archive",
    "sourceArchiveDir": "./data/archive/title_ratings",
    "maxFilesPerTrigger": 1,
}

imdb_tables["title_ratings"] = (
    spark.readStream.options(**reader_options)
    .schema(raw_schema["title_ratings"])
    .csv(f"{data_folder}/title_ratings")
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
    .csv(f"{batch_data_folder}/title.basics.tsv.gz")
)

# %%
from transformation import bronze as xform_bronze

bronze_transformation = {
    "title_basics": xform_bronze.title_basics.transformation,
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
top_10_movies_with_min_500_votes_ssdf = task1(
    title_basics_sdf=cleansed_imdb_tables["title_basics"],
    title_ratings_ssdf=cleansed_imdb_tables["title_ratings"],
    ranking_logics=ranking_logics,
)

# %%
# Write the output to the console
writer_options = {
    "path": "data/output/query1",
    "checkpointLocation": "data/.ckpt/q1",
    "truncate": False,
}

query1 = (
    top_10_movies_with_min_500_votes_ssdf.writeStream.outputMode("complete")
    .options(**writer_options)
    .format("console")
    .start()
)

# %%
# Await termination
query1.awaitTermination()
