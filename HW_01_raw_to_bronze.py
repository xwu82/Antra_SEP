# Databricks notebook source
import pandas as pd
import json
from pyspark.sql.functions import explode
from pyspark.sql.functions import current_timestamp, lit


# COMMAND ----------

root = "dbfs:/FileStore/tables/"
rawdata = root + "raw/"
bronzePath = root + "bronze/"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the Files in the Raw Path

# COMMAND ----------

display(dbutils.fs.ls(rawdata))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make Notebook Idempotent

# COMMAND ----------

dbutils.fs.rm(bronzePath, recurse=True)  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingest raw data

# COMMAND ----------

raw_movie_data_df = spark.read.option("multiline","true").json(rawdata)
raw_movie_data_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the Raw Data

# COMMAND ----------

raw_movie_df = raw_movie_data_df.select(explode(raw_movie_data_df.movie).alias("value"))
display(raw_movie_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestion Metadata

# COMMAND ----------

raw_movie_df = (
  raw_movie_df.select(
   "value",
lit("files.training.databricks.com").alias("datasource"),
    current_timestamp().alias ("ingesttime"),
    lit("new").alias("status"),
    current_timestamp().cast("date").alias("ingestdate")
  )
)

# COMMAND ----------

display(raw_movie_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Widget

# COMMAND ----------

dropdown_ingestdate = [row[0] for row in raw_movie_df.select("ingestdate").distinct().collect()]
dropdown_ingestdate_str = [date_obj.strftime('%Y-%m-%d') for date_obj in dropdown_ingestdate]

dbutils.widgets.text("Bronze database path", bronzePath)
dbutils.widgets.dropdown("ingestdate", dropdown_ingestdate_str[0], dropdown_ingestdate_str)

# COMMAND ----------

user_select = dbutils.widgets.get("ingestdate")
user_select_bronzePath = bronzePath + "p_ingestdate=" + user_select + "/"
print(user_select_bronzePath)

# COMMAND ----------

from pyspark.sql import functions
from pyspark.sql.functions import row_number
from pyspark.sql.window import Window

# COMMAND ----------

w = Window().orderBy(lit('A'))
# df = df.withColumn("row_num", row_number().over(w))

raw_movie_df = raw_movie_df.withColumn("row_number", row_number().over(w))
display(raw_movie_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## WRITE Batch to a Bronze Table

# COMMAND ----------

 from pyspark.sql.functions import col

(
  raw_movie_df.select(
  "row_number",
  "datasource",
  "ingesttime",
  "value",
  "value.*", 
  "status",
  col("ingestdate").alias("p_ingestdate"),
  )
  .write.format("delta")
  .mode("append")
  .partitionBy("p_ingestdate")
  .save(bronzePath)
)

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Register the Bronze Table in the Metastore

# COMMAND ----------

spark.sql("""
DROP TABLE IF EXISTS movie_bronze;
""")

spark.sql(f"""
CREATE TABLE movie_bronze
USING DELTA
LOCATION "{bronzePath}"
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display Classic Bronze Table

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movie_bronze

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------


