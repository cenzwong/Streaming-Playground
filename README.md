# Streaming-Playground
This repository serve a place for playing around with Streaming

```sh
python -m venv .venv
source download_data.sh 

/home/codespace/.python/current/lib/python3.12/site-packages/pyspark/bin/spark-submit spark_streaming.py
```


Spark Streaming have three output: 
- complete, 
- update, 
- append

Data Processing Architectures
- Lambda Architecture
- Kappa

# Streaming

```
zcat data/input/raw/title.ratings.tsv.gz | split -l 100000 --additional-suffix=.tsv - "data/input/title_ratings/title.ratings-"
```

## Preparing environemnet
```
zcat data/input/raw/title.ratings.tsv.gz | split -l 100000 --additional-suffix=.tsv - "data/input/title_ratings/title.ratings-"

mv data/archive/title_ratings/workspaces/Streaming-Playground/data/input/title_ratings/*.tsv data/input/title_ratings
rm -rf data/.ckpt
```