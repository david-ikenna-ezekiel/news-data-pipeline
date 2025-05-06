# Databricks notebook source
from pyspark.sql.functions import (
    col, to_date, lower, upper, regexp_extract, length, udf, to_json, struct, lit
)
from pyspark.sql.types import TimestampType
from datetime import datetime

from pyspark.sql.functions import col, to_json, struct, lit
from pyspark.sql.types import StringType, FloatType, StructType, StructField
from datetime import datetime
import re
from textblob import TextBlob
from pyspark.sql.types import StringType, FloatType, StructType, StructField
from pyspark.sql.utils import AnalysisException
from datetime import datetime
import re
from textblob import TextBlob

# COMMAND ----------

def write_to_datalake(df, layer, partition_by=None, mode="append"):
    """
    Writes the given DataFrame to the specified layer in the data lake with optional partitioning.
    
    Parameters:
    df (DataFrame): The DataFrame to write.
    layer (str): The layer to write to ('bronze', 'silver', 'gold').
    partition_by (str or list, optional): The column(s) to partition by.
    mode (str): Write mode ('append' or 'overwrite'). Defaults to 'append'.
    
    Returns:
    str: The path where the data was saved.
    """
    try:
        # Define the base path
        base_path = f"abfss://newsdata@yourstorage.dfs.core.windows.net/{layer}/"
        
        # Setup writer
        writer = df.write.format("delta").mode(mode)
        
        if partition_by:
            if isinstance(partition_by, str):
                partition_by = [partition_by]
            writer = writer.partitionBy(*partition_by)
        
        # Save
        writer.save(base_path)
        
        print(f"✅ Data successfully written to {base_path} with mode='{mode}'")
        return base_path
    
    except Exception as e:
        print(f"❌ Error writing to datalake: {str(e)}")
        raise


# COMMAND ----------



# COMMAND ----------

# _lib_dq_helpers

# --------------------------------------
# Format all column names to UPPERCASE
# --------------------------------------
def format_column_names_upper_spaces(df):
    for col_name in df.columns:
        new_col_name = col_name.upper().replace(" ", "_")
        df = df.withColumnRenamed(col_name, new_col_name)
    return df


# ------------------------------
# UDFs
# ------------------------------
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




# COMMAND ----------

# ------------------------------
# DQ Config
# ------------------------------
dq_config = {
    "news_articles": {
        "required_columns": ["title", "publishedAt", "url"],
        "not_null": ["title", "publishedAt", "url"],
        "unique_keys": ["url"]
    }
}

# ------------------------------
# DQ Class
# ------------------------------
class DataQualityChecker:
    def __init__(self, df, table_name=""):
        self.df = df
        self.table_name = table_name
        self.issues = []
        self.bad_keys = None

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
            bad_rows = self.df.join(self.bad_keys, on=self.bad_keys.columns, how="inner")
            valid_rows = self.df.join(self.bad_keys, on=self.bad_keys.columns, how="left_anti")
            return valid_rows, bad_rows
        else:
            return self.df, None


# COMMAND ----------

# ------------------------------
# DQ Runner
# ------------------------------
def run_dq_from_config(df, table_name, config):
    dq = DataQualityChecker(df, table_name=table_name)

    rules = config.get(table_name, {})

    required_columns = rules.get("required_columns", [])
    not_null_columns = rules.get("not_null", [])
    unique_keys = rules.get("unique_keys", [])

    if required_columns:
        dq.check_column_exists(required_columns)

    if not_null_columns:
        dq.check_not_null(not_null_columns)

    if unique_keys:
        dq.check_duplicates(unique_keys)

    dq.run()
    return dq.get_valid_invalid()

# COMMAND ----------



# COMMAND ----------

def write_to_datalake_and_hive(df, layer, partition_by=None, mode="overwrite", hive_table=None):
    """
    Writes the given DataFrame to the specified layer in the data lake with optional partitioning,
    and also writes to a Hive table if specified.
    """
    try:
        # Define the base path
        base_path = f"abfss://newsdata@yourstorage.dfs.core.windows.net/{layer}/"

        # Setup writer for Delta Lake
        writer = df.write.format("delta").mode(mode)

        if partition_by:
            if isinstance(partition_by, str):
                partition_by = [partition_by]
            writer = writer.partitionBy(*partition_by)

        # Save to Delta Lake
        writer.save(base_path)
        print(f"✅ Data successfully written to {base_path} with mode='{mode}'")


        # Save to Hive table if specified
        database_name = "news_articles"
        if database_name and hive_table:
            # Create database if not exists
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {database_name}")
            full_hive_table_name = f"{database_name}.{hive_table}"
            df.write.format("hive").mode(mode).saveAsTable(full_hive_table_name)
            print(f"✅ Data successfully written to Hive table '{full_hive_table_name}' with mode='{mode}'")

        return base_path

    except Exception as e:
        print(f"❌ Error writing to datalake or Hive table: {str(e)}")
        raise


# COMMAND ----------

