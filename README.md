# IMDb Spark Processing

This repository is a demo on running batch and streaming processing using IMDb data and PySpark. It showcases how to handle large datasets efficiently and provides step-by-step instructions stored in the `docs/` directory.

## Introduction

This project demonstrates the use of Apache Spark and PySpark to process IMDb datasets. It includes examples of batch and streaming processing, showcasing the power of Spark in handling large-scale data.

## Data Understanding

The dataset can be downloaded from IMDB: https://datasets.imdbws.com/

A simple ER diagram is drawn using D2 lang: https://github.com/cenzwong/imdbws-erdigram

[![hello](https://github.com/cenzwong/imdbws-erdigram/blob/main/output/imdb.png?raw=true)](https://github.com/cenzwong/imdbws-erdigram/blob/main/output/imdb.png)

## Setup

### Prerequisites

Ensure you have the following software installed on your machine:

- Python
- Apache Spark
- Java Development Kit (JDK)

#### Installing Apache Spark and PySpark

To get started with Apache Spark and PySpark, follow these steps to install the necessary software:

```sh
brew install python
brew install openjdk@17
pip install pyspark
```

### Installation

Clone this repository and navigate to the project directory:

```sh
git clone https://github.com/cenzwong/imdb-spark-processing.git
cd imdb-spark-processing
```

Install the required Python packages:
```sh
pip install -r requirements.txt
```

### Download IMDB Data
Ensure you have the required IMDB data ready for analysis. I've prepared a script that will download the data into a `data/` folder, which is listed in your `.gitignore` file to avoid committing it to version control.
Run the script by executing:

```sh
source download_data.sh
```

You are now set for running spark_batch.ipynb

## Folder Structure

- **pysparky/**: Contains the main package and its functions.
    - check my other project: [PySparky-pyspark-helper](https://pysparky.github.io/pysparky-pyspark-helper/)
- **transformation/**: Holds scripts for data transformations.
    - **bronze/**: Scripts for initial data processing.
    - **streaming/**: Scripts for streaming tasks.
    - **tasks/**: Additional task scripts.
- **metadata/**: Metadata-related scripts, including table comment, column comments and schema.
    - **raw/**: Metadata for raw data.
    - **bronze/**: Metadata for initial processing.
- **data/**: Data storage.
    - **raw/**: Raw, compressed data files.
    - **input/**: Input data files for streaming, both compressed and split.
    - **archive/**: Archived data for streaming storing processed data.


> - A *functions* should be column-in-column-out
> - A *transformation* should be dataframe-in-dataframe-out

## Batch Processing

Run the `spark_batch.ipynb`

![](https://github.com/cenzwong/imdbws-erdigram/blob/main/output/pipeline_split.png?raw=true)

## Preparing Stream Data

To illustrate different files landing in the folder, we will `split` the files into smaller ones and pass those files into the streaming engine. `zcat`/`gzcat` is used for reading `gzip` files, and the output files are not zipped.

> 
> macOS doesn't support --additional-suffix, so we’ll use a workaround. Create the directories and split the files, then rename them to add the .tsv suffix:

```sh
mkdir data/input/title_ratings

# linux
zcat data/raw/title.ratings.tsv.gz | split -l 100000 --additional-suffix=.tsv - "data/input/title_ratings/title.ratings-"
zcat data/raw/title.basics.tsv.gz | split -l 1000000 --additional-suffix=.tsv - "data/input/title_basics/title.basics-"

# Mac
# Mac doesn't support additional-suffix
mkdir data/input
mkdir data/input/title_ratings
gzcat data/raw/title.ratings.tsv.gz | split -l 100000 -a 1 - "data/input/title_ratings/title.ratings-"
for f in data/input/title_ratings/title.ratings-*; do mv "$f" "$f.tsv"; done
```

## Streaming Processing
Run the streaming scripts to process the data:

```bash
python spark_streaming_title_basics.py
python spark_streaming_title_ratings.py
```

- `spark_streaming_title_basics.py` demonstrates files arriving and processing by filtering for movies only. The output is `append` on `JSON` files.
- `spark_streaming_title_ratings.py` filters the title_basics dataset to include only movies, and it uses title_ratings to calculate rankings based on a custom logic. The output shows the top 10 movies with at least 500 votes in a 5-second window, sorted by the custom ranking. The output is `complete` on `console`.

## Clean up
To clean up the data and reset the setup, run:

```bash
./clean_streaming_setup.sh
```