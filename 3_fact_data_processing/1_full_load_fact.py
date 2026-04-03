# Databricks notebook source
from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/shejal.aute101@gmail.com/consolidated_pipeline/setup/utilities

# COMMAND ----------

print(bronze_schema, silver_schema, gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog", "fmcg", "Catalog")
dbutils.widgets.text("data_source", "orders", "Data Source")

catalog = dbutils.widgets.get("catalog")
data_source = dbutils.widgets.get("data_source")


base_path = f's3://sportsbar-dbproject/{data_source}'
landing_path=f"{base_path}/landing/"
processed_path=f"{base_path}/processed/"

print("Base Path " ,base_path)
print("Landing Path ", landing_path)
print("Processed Path ", processed_path)

bronze_table= f"{catalog}.{bronze_schema}.{data_source}"
silver_table=f"{catalog}.{silver_schema}.{data_source}"
gold_table=f"{catalog}.{gold_schema}.sb_fact_{data_source}"

# COMMAND ----------

df =spark.read.option("header",True)\
    .option("inferSchema",True)\
    .csv(f"{landing_path}/*.csv")\
    .withColumn("read_timestamp",F.current_timestamp())\
    .select("*","_metadata.file_name","_metadata.file_size")


total=df.count()
print(total)
df.limit(10).display()

# COMMAND ----------

df.write.format("delta")\
    .option("delta.enableChangeDataFeed",True)\
    .mode("append")\
    .saveAsTable(bronze_table)


# COMMAND ----------

#We need to move all the files from landing folder to processed folder
files=dbutils.fs.ls(landing_path)
files

# COMMAND ----------

for file_info in files:
    dbutils.fs.mv(
        file_info.path,
        f"{processed_path}/{file_info.name}",
        True
    )


# COMMAND ----------

df_orders = spark.sql(f"""SELECT * from {bronze_table}""")

df_orders.show(2)

# COMMAND ----------

#1. keep only those records whose order_qty is present

df_orders=df_orders.filter(F.col("order_qty").isNotNull())

# 2.clean the customer_id keep only numeric values otherwise set it to "999999"

df_orders= df_orders\
    .withColumn("customer_id",
    F.when(F.col("customer_id").rlike("^[0-9]+$"),F.col("customer_id"))
    .otherwise("999999").cast("string")
    )

# 3. Remove weekday name from the date text
#    "Tuesday, July 01, 2025" → "July 01, 2025"
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.regexp_replace(F.col("order_placement_date"), r"^[A-Za-z]+,\s*", "")
)

# 4. Parse order_placement_date using multiple possible formats
df_orders = df_orders.withColumn(
    "order_placement_date",
    F.coalesce(
        F.try_to_date("order_placement_date", "yyyy/MM/dd"),
        F.try_to_date("order_placement_date", "dd-MM-yyyy"),
        F.try_to_date("order_placement_date", "dd/MM/yyyy"),
        F.try_to_date("order_placement_date", "MMMM dd, yyyy"),
    )
)

# 5. Drop duplicates
df_orders = df_orders.dropDuplicates(["order_id", "order_placement_date", "customer_id", "product_id", "order_qty"])

# 5. convert product id to string
df_orders = df_orders.withColumn('product_id', F.col('product_id').cast('string'))





# COMMAND ----------

df_orders.agg(
    F.min("order_placement_date").alias("min_date"),
    F.max("order_placement_date").alias("max_date")
).display()

# COMMAND ----------

df_orders.limit(20).display()

# COMMAND ----------

df_products = spark.table("fmcg.silver.products")

df_products.show(2)


# COMMAND ----------

df_joined= df_orders.join(df_products,"product_id","inner").select(df_orders["*"],df_products["product_code"])


df_joined.limit(10).display()

# COMMAND ----------

# checking if table already exists if it is then upsert if not then update

if not (spark.catalog.tableExists(silver_table)):
    df_joined.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(silver_table)
else:
    silver_delta = DeltaTable.forName(spark, silver_table)
    silver_delta.alias("silver").merge(df_joined.alias("bronze"), "silver.order_placement_date = bronze.order_placement_date AND silver.order_id = bronze.order_id AND silver.product_code = bronze.product_code AND silver.customer_id = bronze.customer_id").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Layer Processing

# COMMAND ----------

df_gold = spark.sql(f"SELECT order_id, order_placement_date as date, customer_id as customer_code, product_code, product_id, order_qty as sold_quantity FROM {silver_table};")

df_gold.show(2)

# COMMAND ----------

#if tables exist then perform upsert otherwise create new table and insert all rows

if not (spark.catalog.tableExists(gold_table)):
    print("creating New Table")
    df_gold.write.format("delta").option(
        "delta.enableChangeDataFeed", "true"
    ).option("mergeSchema", "true").mode("overwrite").saveAsTable(gold_table)
else:
    gold_delta = DeltaTable.forName(spark, gold_table)
    gold_delta.alias("source").merge(df_gold.alias("gold"), "source.date = gold.date AND source.order_id = gold.order_id AND source.product_code = gold.product_code AND source.customer_code = gold.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merge with Parent Company

# COMMAND ----------

df_child=spark.sql(f"select * from {gold_table}")

df_child.limit(10).display()

# COMMAND ----------

#Change the date to first day of the month 2025-07-10  --> 2025-07-01


df_monthly=(
    df_child\
    .withColumn("month_start",F.trunc("date","MM"))\
    .groupBy("month_start","product_code","customer_code")\
    .agg(F.sum("sold_quantity").alias("sold_quantity"))\
    .withColumnRenamed("month_start","date")    
)


display(df_monthly.limit(10))

# COMMAND ----------

df_monthly.count()

# COMMAND ----------

#merging with main parent table

gold_parent_delta = DeltaTable.forName(spark, f"{catalog}.{gold_schema}.fact_orders")
gold_parent_delta.alias("parent_gold").merge(df_monthly.alias("child_gold"), "parent_gold.date = child_gold.date AND parent_gold.product_code = child_gold.product_code AND parent_gold.customer_code = child_gold.customer_code").whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

