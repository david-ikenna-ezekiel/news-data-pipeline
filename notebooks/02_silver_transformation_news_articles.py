# Databricks notebook source
from pyspark.sql.functions import col, to_date, lower, upper, regexp_extract, length, to_json, struct, lit
from datetime import datetime

# COMMAND ----------

# MAGIC %run ./_lib_dq_helpers

# COMMAND ----------


def process_bronze_to_silver(bronze_path, quarantine_path, dq_config):
    # --------------------------------------
    # Register UDFs
    # --------------------------------------
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType, StructType, StructField, FloatType

    remove_html_tags_udf = udf(remove_html_tags, StringType())

    analyze_sentiment_udf = udf(analyze_sentiment, StructType([
        StructField("polarity", FloatType(), True),
        StructField("label", StringType(), True)
    ]))

    # --------------------------------------
    # Load Bronze Delta
    # --------------------------------------
    bronze_df = spark.read.format("delta").load(bronze_path)

    # Flatten STRUCTs if needed
    if 'source' in bronze_df.columns:
        bronze_df = bronze_df.withColumn("source_name", col("source.name")).drop("source")

    # --------------------------------------
    # Run dynamic DQ from config
    # --------------------------------------
    valid_df, failed_df = run_dq_from_config(bronze_df, "news_articles", dq_config)

    # --------------------------------------
    # Handle quarantine
    # --------------------------------------
    if failed_df is not None and failed_df.count() > 0:
        quarantined_df = failed_df.withColumn("payload", to_json(struct([col(c) for c in failed_df.columns]))) \
                                  .withColumn("reason", lit("Failed DQ checks")) \
                                  .withColumn("source_table", lit("bronze.news_articles")) \
                                  .withColumn("ingestion_time", lit(datetime.now()))
        quarantine_df = quarantined_df.select("payload", "reason", "source_table", "ingestion_time")
        quarantine_df.write.format("delta").mode("append").save(quarantine_path)
        print(f"⚠️ Quarantined {failed_df.count()} bad records.")
    else:
        print("✅ No records quarantined.")

    # --------------------------------------
    # Debug counts
    # --------------------------------------
    print(f"Bronze: {bronze_df.count()}, Failed: {failed_df.count() if failed_df else 0}, Valid: {valid_df.count() if valid_df else 0}")

    # --------------------------------------
    # Silver Transformations
    # --------------------------------------
    if valid_df is not None and valid_df.count() > 0:
        df_silver = (
            valid_df
            .dropna(subset=["title", "publishedAt"])  # Remove rows with missing key fields
            .dropDuplicates(["title", "publishedAt", "url", "author"])  # Remove duplicates
            .withColumn("published_date", to_date(col("publishedAt")))  # Parse published date
            .withColumn("source", col("source_name"))  # Flattened source
            .withColumn("title_lower", lower(col("title")))  # Lowercase title
            .withColumn("domain", regexp_extract(col("url"), r"https?://(?:www\.)?([^/]+)", 1))  # Extract domain
            .withColumn("content_word_count", length(col("content")))  # Word count
            .withColumn("description", remove_html_tags_udf(col("description")))  # Clean HTML
            .withColumn("content", remove_html_tags_udf(col("content")))  # Clean HTML
            .withColumn("sentiment", analyze_sentiment_udf(col("title")))  # Sentiment
            .withColumn("sentiment_score", col("sentiment.polarity"))
            .withColumn("sentiment_label", col("sentiment.label"))
            .withColumn("country", upper(col("country")))  # Uppercase country
            .select(
                "source",
                "author",
                "title",
                "description",
                "domain",
                "published_date",
                "content",
                "content_word_count",
                "sentiment_score",
                "sentiment_label",
                "ingestion_time",
                "country",
                "url",
            )
        )

        # Format column names to uppercase with spaces
        df_silver = format_column_names_upper_spaces(df_silver)

        # Show result
        # display(df_silver)

        # (Optional) Save to Silver Layer
        write_to_datalake(df=df_silver, layer="silver", partition_by="COUNTRY", mode="overwrite")
        print("✅ Transformed clean data ready for Silver.")
    else:
        print("❌ No valid records available for Silver transformation.")


# COMMAND ----------

# Example usage
process_bronze_to_silver(
    bronze_path="abfss://newsdata@yourstorageaccount.dfs.core.windows.net/bronze/",
    quarantine_path="abfss://quarantine@yourstorageaccount.dfs.core.windows.net/news_articles/",
    dq_config=dq_config
)

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

from pyspark.sql.functions import (
    col, to_date, lower, upper, regexp_extract, length, udf, to_json, struct, lit
)
from pyspark.sql.types import StringType, FloatType, StructType, StructField
from pyspark.sql.utils import AnalysisException
from datetime import datetime
import re
from textblob import TextBlob

# --------------------------------------
# UDFs
# --------------------------------------
def remove_html_tags(text):
    if text is None:
        return None
    clean = re.compile('<.*?>')
    return re.sub(clean, '', text)

def analyze_sentiment(text):
    if text is None:
        return None, None
    blob = TextBlob(text)
    polarity = blob.sentiment.polarity
    if polarity > 0:
        label = "positive"
    elif polarity < 0:
        label = "negative"
    else:
        label = "neutral"
    return polarity, label

remove_html_tags_udf = udf(remove_html_tags, StringType())

analyze_sentiment_udf = udf(analyze_sentiment, StructType([
    StructField("polarity", FloatType(), True),
    StructField("label", StringType(), True)
]))

def format_column_names(df):
    for col_name in df.columns:
        new_col_name = col_name.upper().replace("_", " ")
        df = df.withColumnRenamed(col_name, new_col_name)
    return df

# --------------------------------------
# Data Quality Checker Class (Fixed)
# --------------------------------------
class DataQualityChecker:
    def __init__(self, df, table_name=""):
        self.df = df
        self.table_name = table_name
        self.issues = []
        self.bad_keys = None  # Only store keys (e.g., duplicated URL values)

    def check_not_null(self, columns):
        for c in columns:
            bad = self.df.filter((col(c).isNull()) | (col(c) == ""))
            if bad.count() > 0:
                self.issues.append(f"❌ Null or empty values in column '{c}'")
                if self.bad_keys is None:
                    self.bad_keys = bad.select(c).dropDuplicates()
                else:
                    self.bad_keys = self.bad_keys.union(bad.select(c)).dropDuplicates()

    def check_column_exists(self, columns):
        missing = [c for c in columns if c not in self.df.columns]
        if missing:
            self.issues.append(f"❌ Missing required columns: {missing}")

    def check_duplicates(self, subset_columns):
        dup_keys = self.df.groupBy(subset_columns).count().filter("count > 1").drop("count")
        if dup_keys.count() > 0:
            if self.bad_keys is None:
                self.bad_keys = dup_keys
            else:
                self.bad_keys = self.bad_keys.union(dup_keys).dropDuplicates()
            self.issues.append(f"❌ Duplicate keys detected on: {subset_columns}")

    def run(self):
        if not self.issues:
            print(f"✅ [{self.table_name}] Data quality passed.")
            return True
        else:
            print(f"🛑 [{self.table_name}] Data quality failed:")
            for issue in self.issues:
                print("-", issue)
            return False

    def get_valid_invalid(self):
        if self.bad_keys is not None:
            # Get all bad rows
            bad_rows = self.df.join(self.bad_keys, on=self.bad_keys.columns, how="inner")
            valid_rows = self.df.join(self.bad_keys, on=self.bad_keys.columns, how="left_anti")
            return valid_rows, bad_rows
        else:
            return self.df, None

# --------------------------------------
# Paths
# --------------------------------------
bronze_path = "abfss://newsdata@datalakedavidezekiel.dfs.core.windows.net/bronze/"
silver_path = "abfss://newsdata@datalakedavidezekiel.dfs.core.windows.net/silver/"
quarantine_path = "abfss://quarantine@datalakedavidezekiel.dfs.core.windows.net/news_articles/"

# --------------------------------------
# Load Bronze Delta
# --------------------------------------
try:
    bronze_df = spark.read.format("delta").load(bronze_path)
except AnalysisException as e:
    raise Exception(f"❌ Could not load Bronze table: {e}")

# --------------------------------------
# Flatten STRUCT columns if necessary
# --------------------------------------
if 'source' in bronze_df.columns:
    bronze_df = bronze_df.withColumn("source_name", col("source.name")).drop("source")

# --------------------------------------
# Run Data Quality
# --------------------------------------
dq = DataQualityChecker(bronze_df, table_name="news_articles")

required_columns = ["title", "publishedAt", "url"]

dq.check_column_exists(required_columns)
dq.check_not_null(required_columns)
dq.check_duplicates(["url"])

dq.run()

valid_df, failed_df = dq.get_valid_invalid()

# --------------------------------------
# Quarantine Failed Records
# --------------------------------------
if failed_df is not None and failed_df.count() > 0:
    quarantined_df = failed_df.withColumn("payload", to_json(struct([col(c) for c in failed_df.columns]))) \
                              .withColumn("reason", lit("; ".join(dq.issues))) \
                              .withColumn("source_table", lit("bronze.news_articles")) \
                              .withColumn("ingestion_time", lit(datetime.now()))
    quarantine_df = quarantined_df.select("payload", "reason", "source_table", "ingestion_time")
    quarantine_df.write.format("delta").mode("append").save(quarantine_path)
    print(f"⚠️ Quarantined {failed_df.count()} bad records.")
else:
    print("✅ No records quarantined.")

# --------------------------------------
# Debugging Counts
# --------------------------------------
print(f"Total rows in bronze_df: {bronze_df.count()}")
print(f"Total rows in failed_df: {failed_df.count() if failed_df else 0}")
print(f"Total rows in valid_df: {valid_df.count() if valid_df else 0}")

# --------------------------------------
# Silver Transformation
# --------------------------------------
if valid_df is not None and valid_df.count() > 0:
    df_silver = (
        valid_df
        .dropna(subset=["title", "publishedAt"])
        .dropDuplicates(["title", "publishedAt", "url", "author"])
        .withColumn("published_date", to_date(col("publishedAt")))
        .withColumn("source", col("source_name"))
        .withColumn("title_lower", lower(col("title")))
        .withColumn("domain", regexp_extract(col("url"), r"https?://(?:www\.)?([^/]+)", 1))
        .withColumn("content_word_count", length(col("content")))
        .withColumn("description", remove_html_tags_udf(col("description")))
        .withColumn("content", remove_html_tags_udf(col("content")))
        .withColumn("sentiment", analyze_sentiment_udf(col("title")))
        .withColumn("sentiment_score", col("sentiment.polarity"))
        .withColumn("sentiment_label", col("sentiment.label"))
        .withColumn("country", upper(col("country")))
        .select(
            "source",
            "author",
            "title",
            "description",
            "domain",
            "published_date",
            "content",
            "content_word_count",
            "sentiment_score",
            "sentiment_label",
            "ingestion_time",
            "country",
            "url",
        )
    )

    df_silver = format_column_names(df_silver)
    display(df_silver)
    # Uncomment to save to Silver
    # df_silver.write.format("delta").mode("append").save(silver_path)
    print("✅ Transformed clean data written to Silver.")
else:
    print("❌ No valid records available for Silver transformation.")