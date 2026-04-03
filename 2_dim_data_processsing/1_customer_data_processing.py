# Databricks notebook source
#importing essential libraries

from pyspark.sql import functions as F
from delta.tables import DeltaTable

# COMMAND ----------

# MAGIC %run /Workspace/Users/shejal.aute101@gmail.com/consolidated_pipeline/setup/utilities

# COMMAND ----------

print(bronze_schema,silver_schema,gold_schema)

# COMMAND ----------

dbutils.widgets.text("catalog","fmcg","Catalog")
dbutils.widgets.text("data_source","customers","Data Source")

catalog = dbutils.widgets.get("catalog")
data_source=dbutils.widgets.get("data_source")


print(catalog,data_source)

# COMMAND ----------

base_path=f's3://sportsbar-dbproject/{data_source}/*.csv'
print(base_path)

# COMMAND ----------

df=(
    spark.read.format("csv")\
    .option("header",True)\
    .option("inferSchema",True)\
    .load(base_path)\
    .withColumn("read_timestamp",F.current_timestamp())\
    .select("*","_metadata.file_name","_metadata.file_size")
)


display(df.limit(10))

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed",True)\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{bronze_schema}.{data_source}")
    

# COMMAND ----------

# MAGIC %md
# MAGIC ### Silver Layer Processing

# COMMAND ----------

df_bronze=spark.sql(f"SELECT * FROM {catalog}.{bronze_schema}.{data_source};")

df_bronze.show(5)


# COMMAND ----------

df_duplicates = df_bronze.groupBy("customer_id").count().filter(F.col("count")>1)

print(df_duplicates)

# COMMAND ----------

print("Count before dropping duplicates",df_bronze.count())
df_silver=df_bronze.dropDuplicates(["customer_id"])
print("Count After dropping duplicates",df_silver.count())

# COMMAND ----------

# to find column in which data has leading spaces

df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name"))).display()

# COMMAND ----------

#use trim function to remove the leading spaces

df_silver=df_silver.withColumn("customer_name",F.trim(F.col("customer_name")))

# COMMAND ----------

#Cross checking for leading spaces

df_silver.filter(F.col("customer_name") != F.trim(F.col("customer_name"))).display()

# COMMAND ----------

df_silver.select(F.col("city")).distinct().display()

# COMMAND ----------

city_mapping={
    "Bengaluruu":"Bengaluru",
    "Bengalore":"Bengaluru",

    "Hyderabadd":"Hyderabad",
    "Hyderbad":"Hyderabad",


    "NewDelhee":"New Delhi",
    "NewDelhi":"New Delhi",
    "NewDheli":"New Delhi"
}

allowed =[ "Bengaluru","Hyderabad","New Delhi"]

df_silver=(df_silver.replace(city_mapping,subset=["city"])\
    .withColumn("city",
                F.when(F.col("city").isin(allowed),F.col("city"))\
                    .when(F.col("city").isNull(),None)\
                       .otherwise(None)
                )
)

# COMMAND ----------

#Cross checking city names
df_silver.select(F.col("city")).distinct().display()

# COMMAND ----------

df_silver.select("customer_name").distinct().show()

# COMMAND ----------

# fixing the case problems in customer_name
df_silver=df_silver.withColumn("customer_name",
    F.when(F.col("customer_name").isNull(),None).otherwise(F.initcap(F.col("customer_name")))
                               )


# COMMAND ----------

df_silver.select("customer_name").distinct().display()

# COMMAND ----------

df_silver.filter(F.col("city").isNull()).display()

# COMMAND ----------

null_customer_city=["Sprintx Nutrition",
"Zenathlete Foods",
"Primefuel Nutrition",
"Recovery Lane"]

df_silver.filter(F.col("customer_name").isin(null_customer_city)).display()

# COMMAND ----------

# Business Confirmation Note: City corrections confirmed by business team
customer_city_fix = {
    # Sprintx Nutrition
    789403: "New Delhi",

    # Zenathlete Foods
    789420: "Bengaluru",

    # Primefuel Nutrition
    789521: "Hyderabad",

    # Recovery Lane
    789603: "Hyderabad"
}

df_fix = spark.createDataFrame(
    [(k,v) for k ,v in customer_city_fix.items()],
    ["customer_id","fixed_city"]
)

df_fix.display()

# COMMAND ----------

df_silver = (
    df_silver
    .join(df_fix, "customer_id", "left")
    .withColumn(
        "city",
        F.coalesce("city", "fixed_city")   # Replace null with fixed city
    )
    .drop("fixed_city")
)

# COMMAND ----------

df_silver.display()



# COMMAND ----------

#checks for customer name

null_customer_names = ['Sprintx Nutrition', 'Zenathlete Foods', 'Primefuel Nutrition', 'Recovery Lane']
df_silver.filter(F.col("customer_name").isin(null_customer_names)).show(truncate=False)

# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

df_silver=df_silver.withColumn("customer_id",F.col("customer_id").cast("string"))


# COMMAND ----------

df_silver.printSchema()

# COMMAND ----------

#customizing columns based on gold table columns of parent company

df_silver=(
    df_silver.withColumn("customer",F.concat_ws("-","customer_name",F.coalesce(F.col("city"),F.lit("Unknown"))))
    .withColumn("market",F.lit("India"))
    .withColumn("platform",F.lit("Sports Bar"))
    .withColumn("channel",F.lit("Acquisition"))
)

df_silver.limit(10).display()

# COMMAND ----------

df_silver.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed",True)\
    .option("mergeSchema",True)\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{silver_schema}.{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Gold Processing

# COMMAND ----------

df_silver= spark.sql(f"""SELECT * from {catalog}.{silver_schema}.{data_source}""")

df_gold= df_silver.select("customer_id", "customer_name", "city", "customer", "market", "platform", "channel")

# COMMAND ----------

df_gold.display()

# COMMAND ----------

df_gold.write\
    .format("delta")\
    .option("delta.enableChangeDataFeed",True)\
    .mode("overwrite")\
    .saveAsTable(f"{catalog}.{gold_schema}.sb_dim_{data_source}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Merging Parent and Child company's Gold tables

# COMMAND ----------

delta_table=DeltaTable.forName(spark,"fmcg.gold.dim_customers")

df_child_customers=spark.table("fmcg.gold.sb_dim_customers").select(
    F.col("customer_id").alias("customer_code"),
    "customer",
    "market",
    "platform",
    "channel"
)

# COMMAND ----------

delta_table.alias("target").merge(
    source=df_child_customers.alias("source"),
    condition="target.customer_code=source.customer_code"
).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()