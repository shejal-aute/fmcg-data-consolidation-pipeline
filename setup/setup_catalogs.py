# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS fmcg;
# MAGIC use catalog fmcg;

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.gold;
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS fmcg.bronze;
# MAGIC

# COMMAND ----------

