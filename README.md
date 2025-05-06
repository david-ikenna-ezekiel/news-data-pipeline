# 📰 News Data Pipeline Project

An end-to-end data engineering pipeline built on Databricks using PySpark, Delta Lake, and Hive Metastore. This project ingests news articles from an API, performs data quality checks, applies sentiment analysis, models the data into fact/dim structures, and exposes clean gold-layer datasets for analytics.

---

## 📽️ Watch the Demo
[![YouTube Video](https://img.youtube.com/vi/R-oXsJLPYcQ/0.jpg)](https://www.youtube.com/watch?v=R-oXsJLPYcQ)

---

## 🔧 Architecture Overview

```bash
    API → Bronze (Raw Delta) → DQ Checks & Quarantine → Silver (Clean, Enriched) → Gold (Fact/Dim Model) → Hive Metastore → BI Tools
```

----

## 🧱 Tech Stack

- **Databricks** (Notebooks, Delta Lake, Spark, Hive)  
- **Azure Data Lake Gen2** (`abfss`)  
- **PySpark** (UDFs, DataFrame ops)  
- **TextBlob** (Sentiment analysis)  
- **Hive Metastore / Unity Catalog**  

---



---

## ⚙️ Features

- ✅ Dynamic DQ Rules: Configurable per table  
- ✅ Sentiment Analysis: TextBlob-based polarity & label  
- ✅ Data Modeling: Fact and dimension tables with surrogate keys  
- ✅ Hive Integration: Auto-creates schemas, saves tables  
- ✅ Quarantine Zone: Bad records stored with reason metadata  

---

## 🚀 How to Run

1. Clone repo & open in Databricks  
2. Run `01_ingest_bronze_news_api` to pull from API  
3. Run `02_bronze_to_silver_with_dq` for cleaning + DQ  
4. Run `03_silver_to_gold_modeling` to build gold model  
5. Use `04_visualize_gold_tables` to explore results  

---

## 📊 Gold Layer Tables

- `fact_news_articles`  
- `dim_author`  
- `dim_source`  
- `dim_date`  

---

## 📜 License
MIT License

---

## 🙌 Credits
Built by **[Your Name]** with ❤️ on Databricks

---

> 📝 Replace `YOUR_VIDEO_ID` with your actual YouTube video ID to embed the preview.
