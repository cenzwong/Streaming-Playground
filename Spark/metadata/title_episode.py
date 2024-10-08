from pyspark.sql import types as T

# title.episode.tsv.gz

# tconst (string) - alphanumeric identifier of episode
# parentTconst (string) - alphanumeric identifier of the parent TV Series
# seasonNumber (integer) - season number the episode belongs to
# episodeNumber (integer) - episode number of the tconst in the TV series
schema = T.StructType(
    [
        T.StructField("tconst", T.StringType(), True),
        T.StructField("parentTconst", T.StringType(), True),
        T.StructField("seasonNumber", T.IntegerType(), True),
        T.StructField("episodeNumber", T.IntegerType(), True),
    ]
)
