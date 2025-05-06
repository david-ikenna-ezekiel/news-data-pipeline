# Databricks notebook source

from pyspark.sql.functions import col, md5, lit, to_date, upper, row_number
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ./_lib_dq_helpers

# COMMAND ----------

from pyspark.sql.functions import to_date, col, row_number
from pyspark.sql.window import Window
from pyspark.sql.types import StringType

def process_and_write_to_gold(silver_path, gold_layer):
    """
    Processes the silver data to create dimension and fact tables, and writes them to both Delta Lake and Hive.
    
    Parameters:
    silver_path (str): The path to the silver data.
    gold_layer (str): The layer to write to ('gold').
    database_name (str): The name of the Hive database.
    """
    # --------------------------------------
    # Load Silver Delta
    # --------------------------------------
    silver_df = spark.read.format("delta").load(silver_path)

    # --------------------------------------
    # Dimension Tables
    # --------------------------------------

    # DIM SOURCE
    source_df = silver_df.select("SOURCE").distinct().fillna("UNKNOWN")
    source_window = Window.orderBy("SOURCE")
    dim_source = source_df.withColumn("SOURCE_ID", row_number().over(source_window).cast(StringType()).substr(1, 5))

    # DIM AUTHOR
    author_df = silver_df.select("AUTHOR").distinct().fillna("UNKNOWN")
    author_window = Window.orderBy("AUTHOR")
    dim_author = author_df.withColumn("AUTHOR_ID", row_number().over(author_window).cast(StringType()).substr(1, 5))

    # --------------------------------------
    # Fact Table
    # --------------------------------------

    # Join dimensions
    fact_df = silver_df.fillna("UNKNOWN") \
        .withColumn("PUBLISHED_DATE", to_date(col("PUBLISHED_DATE"))) \
        .withColumn("INGESTION_TIME", to_date(col("INGESTION_TIME"))) \
        .join(dim_source, "SOURCE") \
        .join(dim_author, "AUTHOR")

    # Add ARTICLE_ID using row_number
    fact_window = Window.orderBy("URL")
    fact_df = fact_df.withColumn("ARTICLE_ID", row_number().over(fact_window).cast(StringType()).substr(1, 5))

    # Reorder and select fact columns
    fact_news_articles = fact_df.select(
        "ARTICLE_ID",
        "SOURCE_ID",
        "AUTHOR_ID",
        "DOMAIN",
        "COUNTRY",
        "PUBLISHED_DATE",
        "INGESTION_TIME",
        "SENTIMENT_SCORE",
        "SENTIMENT_LABEL",
        "CONTENT_WORD_COUNT",
        "TITLE",
        "DESCRIPTION",
        "CONTENT",
        "URL"
    )

    # --------------------------------------
    # Write to Gold
    # --------------------------------------

    # Write the DataFrames to both Delta Lake and Hive
    write_to_datalake_and_hive(
        df=dim_source,
        layer=f"{gold_layer}/dim_source",
        mode="overwrite",
        hive_table="dim_source"
    )

    write_to_datalake_and_hive(
        df=dim_author,
        layer=f"{gold_layer}/dim_author",
        mode="overwrite",
        hive_table="dim_author"
    )

    write_to_datalake_and_hive(
        df=fact_news_articles,
        layer=f"{gold_layer}/fact_news_articles",
        mode="overwrite",
        hive_table="fact_news_articles"
    )

    print("🎯 Gold Modeling Complete: All tables saved successfully.")

# COMMAND ----------

# Example usage
silver_path = "abfss://newsdata@yourstorageaaccount.dfs.core.windows.net/silver/"
gold_layer = "gold"

process_and_write_to_gold(silver_path, gold_layer)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# --------------------------------------
# Gold Modeling
# --------------------------------------

# Model 1: Top Publishers
gold_top_publishers = (
    silver_df
    .groupBy("SOURCE")
    .count()
    .orderBy(col("count").desc())
)

# Model 2: Sentiment Trends
gold_sentiment_trends = (
    silver_df
    .groupBy("PUBLISHED_DATE", "SENTIMENT_LABEL")
    .count()
    .orderBy("PUBLISHED_DATE")
)

# Model 3: Country Distribution
gold_country_distribution = (
    silver_df
    .groupBy("COUNTRY")
    .count()
    .orderBy(col("count").desc())
)

# --------------------------------------
# Display all Gold DataFrames (optional check)
# --------------------------------------
print("✅ Preview: Top Publishers")
display(gold_top_publishers)

print("✅ Preview: Sentiment Trends")
display(gold_sentiment_trends)

print("✅ Preview: Country Distribution")
display(gold_country_distribution)

# --------------------------------------
# Save all Gold DataFrames
# --------------------------------------
print("💾 Saving Gold tables...")
