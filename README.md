
Databricks-based ETL pipeline for FMCG data consolidation and analytics.

# FMCG Data Consolidation Pipeline (Databricks)

## 📌 Overview
This project demonstrates a real-world data engineering use case of consolidating data from two different sources into a unified data platform. The goal is to build a scalable ETL pipeline to ingest, clean, transform, and integrate data into a single lakehouse architecture for analytics.

The pipeline is implemented using Databricks and follows the Medallion Architecture (Bronze, Silver, Gold) to ensure reliable and efficient data processing.

---

## 🚀 Tech Stack
- Python  
- PySpark  
- SQL  
- Databricks (Community Edition)  
- Amazon S3  
- Spark  

---

## 🏗️ Architecture
The pipeline is built using a layered Medallion Architecture:

### 🔹 Bronze Layer
- Ingests raw CSV data from Amazon S3  
- Captures metadata such as file name, file size, and ingestion timestamp  

### 🔹 Silver Layer
- Performs data cleaning and transformation  
- Removes duplicates and handles null values  
- Standardizes formats (trimming, casing, correcting data inconsistencies)  
- Ensures schema consistency across both data sources  

### 🔹 Gold Layer
- Creates analytics-ready datasets  
- Builds dimension and fact tables for reporting  

---

## 📊 Data Model

### Dimension Tables
- Customers  
- Products  
- Gross Price  

### Fact Table
- Orders (sales transactions)

---

## ⚙️ Pipeline Workflow
1. Load raw data from S3 into Bronze layer  
2. Clean and transform data in Silver layer  
3. Build curated tables in Gold layer  
4. Merge data from both sources into unified tables  
5. Aggregate data for reporting and analytics  

---

## 🔄 Key Features
- End-to-end ETL pipeline using Databricks  
- Consolidation of data from two different sources  
- Incremental data processing using merge (upsert) logic  
- Data quality checks and standardization  
- Schema alignment across datasets  
- Aggregation for business reporting  
- BI dashboard  
---

## 📈 Use Case
- Integrating data from two sources into a unified platform  
- Enabling consistent reporting and analytics  
- Ensuring data quality and reliability  

---

## 🧠 Learnings
- Building scalable ETL pipelines using PySpark  
- Implementing layered data architecture  
- Handling real-world data quality issues  
- Working with incremental data loads  
- Creating analytics-ready datasets  

---
## 📁 Repository Structure

## 📌 Notes

- `1_setup/` contains initial setup scripts for catalogs, schemas, and reusable utilities.
- `2_dim_data_processing/` handles data cleaning, transformation, and loading for dimension tables.
- `3_fact_data_processing/` includes both full load and incremental load logic for fact table processing.
- Incremental load script updates only new or changed data using merge (upsert) logic.


## 👩‍💻 Author
**Shejal Aute**  


---
