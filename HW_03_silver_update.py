# Databricks notebook source
# MAGIC %run ./HW_01_raw_to_bronze

# COMMAND ----------

# MAGIC %run ./HW_02_bronze_to_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ### Perform a Visual Verification of the Silver Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM movie_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ## Handle Quarantined Records

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Load Quarantined Records from the Bronze Table

# COMMAND ----------

bronzeDF_quarantined = spark.read.table("movie_bronze").filter("status='quarantined'")
display(bronzeDF_quarantined)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2: Transform the Quarantined Records

# COMMAND ----------

from pyspark.sql.functions import abs

# COMMAND ----------

bronzeDF_cleaned = bronzeDF_quarantined.withColumn('RunTime', abs(bronzeDF_quarantined.RunTime))
display(bronzeDF_cleaned)

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from movie_silver

# COMMAND ----------

(
  bronzeDF_cleaned.select("*").drop(*["value", "datasource", "ingesttime"])
  .write.format("delta")
  .mode("append")
  .save(silverPath)
)

# COMMAND ----------

# bronzeDF_cleaned = bronzeDF_cleaned.withColumn("RunTime", col("RunTime").cast("Long"))
bronzeDF_cleaned = bronzeDF_cleaned.withColumn("status", lit("loaded"))

(
  bronzeDF_cleaned.select("*")
  .write.format("delta")
  .mode("overwrite")
  .save(bronzePath)
)

# COMMAND ----------

SilverDF = spark.read.table("movie_silver")
display(SilverDF)

# COMMAND ----------

from pyspark.sql.functions import when

# COMMAND ----------

silverDF_cleaned = SilverDF.withColumn("Budget", when(SilverDF.Budget < 1000000, 1000000)
                           .otherwise(SilverDF.Budget))
display(silverDF_cleaned)

# COMMAND ----------

# MAGIC %md
# MAGIC Language

# COMMAND ----------

language = SilverDF.select("OriginalLanguage").distinct().collect()
print(language )

# COMMAND ----------

# deptColumns = ["dept_name","dept_id"]
# deptDF = spark.createDataFrame(data=dept, schema = deptColumns)
# deptDF.printSchema()
# deptDF.show(truncate=False)

OLanguage = ["OriginalLanguage"]
LanguageDF = spark.createDataFrame(data=language, schema = OLanguage)

w = Window().orderBy(lit('A'))
LanguageDF = LanguageDF.withColumn("LanguageId", row_number().over(w))
display(LanguageDF)

# COMMAND ----------

LangSilverPath = root + "LangSilver/"

# COMMAND ----------

#dbutils.fs.rm(LangSilverPath, recurse=True)

# COMMAND ----------

 from pyspark.sql.functions import col

(
  LanguageDF.select(
  "*",
  )
  .write.format("delta")
  .mode("OVERWRITE")
  .save(LangSilverPath)
)

# COMMAND ----------

spark.sql("""
DROP TABLE IF EXISTS Language_silver;
""")

spark.sql(f"""
CREATE TABLE Language_silver
USING DELTA
LOCATION "{LangSilverPath}"
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC select *
# MAGIC from Language_silver

# COMMAND ----------

# MAGIC %md
# MAGIC ##Create a Genres Lookup Table

# COMMAND ----------

SilverDF.schema

# COMMAND ----------

# raw_movie_df = raw_movie_data_df.select(explode(raw_movie_data_df.movie).alias("value"))
# display(raw_movie_df)

GenreDF = SilverDF.select(explode(SilverDF.genres).alias("genres"))
display(GenreDF)

# COMMAND ----------


from pyspark.sql.functions import collect_set

# COMMAND ----------

GenreDF = GenreDF.agg(collect_set("genres"))
                      


# COMMAND ----------

display(GenreDF)

# COMMAND ----------


