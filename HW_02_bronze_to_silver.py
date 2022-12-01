# Databricks notebook source
from delta.tables import DeltaTable
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col,
    current_timestamp,
    from_json,
    from_unixtime,
    lag,
    lead,
    lit,
    mean,
    stddev,
    max,
)
from typing import List
from pyspark.sql.session import SparkSession
from pyspark.sql.window import Window

# COMMAND ----------

# MAGIC %run ./HW_01_raw_to_bronze

# COMMAND ----------

silverPath = root + "silver/"

# COMMAND ----------

# MAGIC %md
# MAGIC ###Display the Files in the Bronze Paths

# COMMAND ----------

display(dbutils.fs.ls(bronzePath))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Current Delta Architecture

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Create the `rawDF` DataFrame

# COMMAND ----------

def read_batch_raw(rawPath: str) -> DataFrame:
    raw_movie_data_df = spark.read.option("multiline","true").json(rawdata)
    raw_movie_df = raw_movie_data_df.select(explode(raw_movie_data_df.movie).alias("value"))
    return raw_movie_df

    
    

# COMMAND ----------

rawDF = read_batch_raw(rawdata)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Transform the Raw Data

# COMMAND ----------

def transform_raw(raw: DataFrame) -> DataFrame:
    return raw.select(
        lit("files.training.databricks.com").alias("datasource"),
        current_timestamp().alias("ingesttime"),
        lit("new").alias("status"),
        "value",
        current_timestamp().cast("date").alias("p_ingestdate"),
    )

# COMMAND ----------

transformedRawDF = transform_raw(rawDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify the Schema with an Assertion

# COMMAND ----------

display(transformedRawDF)
display(transformedRawDF.schema)


# COMMAND ----------

from pyspark.sql.types import *

assert transformedRawDF.schema == StructType([StructField('datasource', StringType(), False), StructField('ingesttime', TimestampType(), False), StructField('status', StringType(), False), StructField('value', StructType([StructField('BackdropUrl', StringType(), True), StructField('Budget', DoubleType(), True), StructField('CreatedBy', StringType(), True), StructField('CreatedDate', StringType(), True), StructField('Id', LongType(), True), StructField('ImdbUrl', StringType(), True), StructField('OriginalLanguage', StringType(), True), StructField('Overview', StringType(), True), StructField('PosterUrl', StringType(), True), StructField('Price', DoubleType(), True), StructField('ReleaseDate', StringType(), True), StructField('Revenue', DoubleType(), True), StructField('RunTime', LongType(), True), StructField('Tagline', StringType(), True), StructField('Title', StringType(), True), StructField('TmdbUrl', StringType(), True), StructField('UpdatedBy', StringType(), True), StructField('UpdatedDate', StringType(), True), StructField('genres', ArrayType(StructType([StructField('id', LongType(), True), StructField('name', StringType(), True)]), True), True)]), True), StructField('p_ingestdate', DateType(), False)])

print("Assertion passed.")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3: Write Batch to a Bronze Table

# COMMAND ----------

def batch_writer(
    dataframe: DataFrame,
    partition_column: str,
    exclude_columns: List = [],
    mode: str = "overwrite",
) -> DataFrame:
    return (
        dataframe.drop(
            *exclude_columns
        )  # This uses Python argument unpacking (https://docs.python.org/3/tutorial/controlflow.html#unpacking-argument-lists)
        .write.format("delta")
        .mode(mode)
        .partitionBy(partition_column)
    )


# COMMAND ----------

bronzeDF = spark.read.table("movie_bronze")

# COMMAND ----------

rawToBronzeWriter = batch_writer(
    dataframe=bronzeDF, partition_column="p_ingestdate"
)

rawToBronzeWriter.save(bronzePath)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Purge Raw File Path

# COMMAND ----------

#dbutils.fs.rm(bronzePath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Display the Bronze Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_bronze

# COMMAND ----------

# MAGIC %md
# MAGIC ## Bronze to Silver Step

# COMMAND ----------

# MAGIC %md
# MAGIC ## Make Notebook Idempotent

# COMMAND ----------

# dbutils.fs.rm(bronzePath, recurse=True)
# dbutils.fs.rm(silverPath, recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Load New Records from the Bronze Records

# COMMAND ----------

bronzeDF = spark.read.table("movie_bronze").filter("status = 'new'")
display(bronzeDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Extract the Nested JSON from the Bronze Records

# COMMAND ----------

# MAGIC %md
# MAGIC ### Create the Silver DataFrame by Unpacking the `nested_json` Column

# COMMAND ----------

movie_silver = bronzeDF.select("row_number", "value", "value.*", "status", "p_ingestdate")

# COMMAND ----------

display(movie_silver)

# COMMAND ----------

movie_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transform the Data

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import *

# COMMAND ----------

movie_silver = movie_silver.select(
  "row_number",
"BackdropUrl", "Budget", "CreatedBy", "CreatedDate", "Id", "ImdbUrl", "OriginalLanguage", "Overview","PosterUrl","Price","ReleaseDate", "Revenue","RunTime","Tagline","Title","TmdbUrl", "UpdatedBy", "UpdatedDate","genres","status", "p_ingestdate"
)

# COMMAND ----------

movie_silver.printSchema()

# COMMAND ----------

display(movie_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Quarantine the Bad Data

# COMMAND ----------

import pyspark.sql.functions as func


# COMMAND ----------

movie_silver.count()

# COMMAND ----------

movie_silver.filter(func.col("Runtime") > 0).count()

# COMMAND ----------

movie_silver.filter(func.col("Runtime") <= 0).count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Split the Silver DataFrame

# COMMAND ----------

movie_silver_clean = movie_silver.filter(func.col("Runtime") > 0)
movie_silver_quarantine = movie_silver.filter(func.col("Runtime") <= 0)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Display the Quarantined Records

# COMMAND ----------

display(movie_silver_clean.schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## WRITE Clean Batch to a Silver Table

# COMMAND ----------

(
    movie_silver_clean.select("*"
    )
    .write.format("delta")
    .mode("overwrite")
    .save(silverPath)
)

# COMMAND ----------

spark.sql(
    """
DROP TABLE IF EXISTS movie_silver
"""
)

spark.sql(
    f"""
CREATE TABLE movie_silver
USING DELTA
LOCATION "{silverPath}"
"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT * FROM movie_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Update Bronze table to Reflect the Loads

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Update Clean records

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

bronzeTable = DeltaTable.forPath(spark, bronzePath)
silverAugmented = movie_silver_clean.withColumn("status", lit("loaded"))

update_match = "bronze.row_number = clean.row_number"
update = {"status": "clean.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("clean"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Update Quarantined records

# COMMAND ----------

silverAugmented = movie_silver_quarantine.withColumn(
    "status", lit("quarantined")
)

display(silverAugmented)

update_match = "bronze.row_number = quarantine.row_number"
update = {"status": "quarantine.status"}

(
    bronzeTable.alias("bronze")
    .merge(silverAugmented.alias("quarantine"), update_match)
    .whenMatchedUpdate(set=update)
    .execute()
)


