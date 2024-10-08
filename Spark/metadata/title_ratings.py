from pyspark.sql import types as T

# title.ratings.tsv.gz

# tconst (string) - alphanumeric unique identifier of the title
# averageRating - weighted average of all the individual user ratings
# numVotes - number of votes the title has received
schema = T.StructType(
    [
        T.StructField("tconst", T.StringType(), True),
        T.StructField("averageRating", T.FloatType(), True),
        T.StructField("numVotes", T.IntegerType(), True),
    ]
)
