# Databricks notebook source
# MAGIC %run ./_lib_dq_helpers

# COMMAND ----------

import requests
import json
from datetime import datetime, timezone
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from pyspark.sql.functions import lit, col

# COMMAND ----------

def ingest_and_write_to_datalake(api_key, country, layer):
    # Config
    endpoint = f"https://newsapi.org/v2/top-headlines?country={country}&apiKey={api_key}"

    # 1. GET DATA FROM API
    response = requests.get(endpoint)
    data = response.json()

    # 2. EXTRACT ARTICLES
    articles = data.get("articles", [])

    # 3. CREATE DATAFRAME
    schema = StructType([
        StructField("source", StructType([StructField("name", StringType())])),
        StructField("author", StringType()),
        StructField("title", StringType()),
        StructField("description", StringType()),
        StructField("url", StringType()),
        StructField("urlToImage", StringType()),
        StructField("publishedAt", StringType()),
        StructField("content", StringType())
    ])

    df = spark.createDataFrame(articles, schema=schema)

    # Add metadata columns
    df = (
        df.withColumn("ingestion_time", lit(datetime.now(timezone.utc).isoformat()))
          .withColumn("country", lit(country))
          .withColumn("publishedAt", col("publishedAt").cast(TimestampType()))
    )

    # Write to Delta Lake
    write_to_datalake(df, layer)

# COMMAND ----------

# Ingest Data
api_key = "your_api_key"  # Replace with your key
country = "us"  # Specify the country
layer = "bronze"  # Specify the layer: bronze, silver, or gold

ingest_and_write_to_datalake(api_key, country, layer)

# COMMAND ----------

