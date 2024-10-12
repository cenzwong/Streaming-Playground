# %%
from pyspark.sql import SparkSession

# %%
spark = SparkSession.builder.appName("imdb_query_streaming").getOrCreate()
spark.conf.set("spark.sql.streaming.forceDeleteTempCheckpointLocation", True)
spark.conf.set("spark.sql.shuffle.partitions", 4)
spark.conf.set("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", False)

# %%
from pathlib import Path

# Define the streaming source
data_folder = Path("./data/input")
imdb_tables = {}

from metadata import raw

raw_schema = {
    "title_basics": raw.title_basics.SCHEMA,
}

reader_options = {
    "sep": "\t",
    # "header": True,
    "nullValue": r"\N",
    # "compression": "gzip",
    "cleanSource": "archive",
    "sourceArchiveDir": "./data/archive/title_basics",
    "maxFilesPerTrigger": 1,
}

imdb_tables["title_basics"] = (
    spark.readStream.options(**reader_options)
    .schema(raw_schema["title_basics"])
    .csv(f"{data_folder}/{"title_basics"}")
)

# %%
from transformation import bronze as xform_bronze

bronze_transformation = {
    "title_basics": xform_bronze.title_basics.transformation,
}

cleansed_imdb_tables = {
    _table_name: _sdf.transform(bronze_transformation[_table_name])
    for _table_name, _sdf in imdb_tables.items()
}

# %%
from transformation.streaming import task0

# %%
title_movies_sdf = task0(
    title_basics_sdf=cleansed_imdb_tables["title_basics"],
)

# %%
# Write the output to the console
writer_options = {"path": "data/output/query0", "checkpointLocation": "data/.ckpt/q0"}
query0 = (
    title_movies_sdf.writeStream.outputMode("append")
    .options(**writer_options)
    .format("json")
    .start()
)

# %%
# Await termination
query0.awaitTermination()
