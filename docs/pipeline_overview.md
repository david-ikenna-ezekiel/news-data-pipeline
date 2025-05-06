# Building a News Data Pipeline with Delta Lake, PySpark, and Hive: A Step-by-Step Look

We live in an age where news spreads fast and changes even faster. But with so much content floating around, how do you make sense of it all—especially if you want to analyze trends, track sentiment, or build dashboards from it?

That’s where this project comes in.

It’s a full pipeline built to collect, clean, and make sense of news articles from the web, all using modern data tools: **Apache Spark (PySpark), Delta Lake, and Hive**, running on **Databricks**. The setup follows something called the **medallion architecture**—a layered approach to data engineering that keeps things organized and scalable.

This article walks through how the whole system works, from pulling raw news stories to serving clean, query-ready data for analysis.

---

## 🗞 What This Pipeline Does (In Plain English)

Think of the pipeline like a coffee-making process:

- **Bronze layer** is like collecting the coffee beans—raw and unfiltered.
- **Silver layer** is where the beans get roasted and ground—cleaned and prepared.
- **Gold layer** is the final brew—ready to drink (or in our case, ready to analyze).

Now, let’s break this down layer by layer.

---

## 1️⃣ Collecting Raw News Data (Bronze Layer)

The first step is pulling in fresh news articles from an external source—[NewsAPI.org](https://newsapi.org), a service that lets you query real-time news from across the web.

**How it works:**
- A Databricks notebook uses Python’s `requests` library to call the API.
- The data is saved in **raw JSON format** into **Azure Data Lake Gen2**, using the `abfss` protocol.
- Stored in **Delta format** for better performance and transaction support.

**Tools Used:**
- Python `requests`
- Databricks Utilities (`dbutils.fs.put`)
- Delta Lake

---

## 2️⃣ Validating Data Quality

Before we move on to processing, we stop and ask: **Is this data any good?**

A custom `DataQualityChecker` class enforces rules like:

- Required fields: `title`, `publishedAt`, `url`
- Duplicates removed based on `url`
- Rows with missing or empty values are filtered
- Minimum row count check

❌ Invalid rows are quarantined in a separate Delta table with metadata explaining why they failed.

**Tools Used:**
- PySpark DataFrames
- Delta Lake (quarantine table)

---

## 3️⃣ Cleaning and Enriching the Data (Silver Layer)

With valid data in hand, we clean and enrich it:

- Flatten nested fields like `source.name`
- Remove HTML tags using regex-based UDFs
- Run sentiment analysis using `TextBlob`
  - Outputs a **polarity score** and a **sentiment label**
- Extract metadata: domain names, content length
- Deduplicate and normalize column names

**Key Functions:**
```python
remove_html_tags(text)
analyze_sentiment(text)  # returns polarity, sentiment label
format_column_names_upper_spaces(df)
```

---
## 4️⃣ Modeling the Data (Gold Layer)


Now we structure the data for easy analysis.

**Fact Table:**

- `fact_news_articles:`  Enriched news records with foreign keys


**Dimension Tables:**

- `dim_source:`  Unique list of sources
- `dim_author:`  Unique list of authors

---
## 🔢 IDs are generated using row_number() and capped at 5 characters for storage efficiency.

**Write Strategy:**

- Save to both Delta and Hive Metastore

- Auto-create Hive DB if not present

**Function:**

```python
write_to_gold_and_hive(df, name, partition_by=None, mode='overwrite')
```
---
## 5️⃣ Making the Data Usable for BI Tools
Once modeled, the data is ready for analysis.

- Hive Metastore tables can be queried by:

    - Power BI

    - Tableau

    - Databricks SQL

    - Even Excel

🔍 Use cases:

- Sentiment trends over time

- Source reliability analysis

- Publication volume by date

---
## 🔁 Architecture Summary
Here’s the full data flow:


```scss
API Source 
   ↓
Bronze (Raw Delta) 
   ↓
Data Quality Checks 
   ↓
Silver (Clean/Enriched Delta) 
   ↓
Gold (Fact/Dim Delta + Hive) 
   ↓
BI & Dashboards
```

Each layer has a clear role:

- Bronze = Traceability

- Silver = Clean, useful data

- Gold = Analytics-ready structure

---
## 🔮 What’s Next?
This pipeline is solid—but here are a few ways to level it up:

- Auto Schema Evolution for flexible ingestion

- Streaming Ingestion for real-time use cases

- Databricks Workflows for job scheduling

- Unity Catalog for role-based access control


---
##  ✅ Final Thoughts
This project shows how to take raw news articles and turn them into structured, useful data that analysts and stakeholders can trust.

It combines:

- Reliable ingestion (Bronze)

- Quality enforcement and enrichment (Silver)

- Business-ready modeling (Gold)

If you're looking to build something similar—whether for news, social media, or any fast-moving data source—this pattern can get you there.