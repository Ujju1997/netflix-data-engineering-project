# Databricks notebook source
checkpoint_path = "abfss://netflix@netflixprojectstorage.dfs.core.windows.net/silver/checkpoint"

# COMMAND ----------



df = spark.readStream\
  .format("cloudFiles")\
  .option("cloudFiles.format", "csv")\
  .option("cloudFiles.schemaLocation", checkpoint_path)\
  .load("abfss://raw@netflixprojectstorage.dfs.core.windows.net/")
 

# COMMAND ----------

display(df)

# COMMAND ----------

 
 df.writeStream\
  .option("checkpointLocation", checkpoint_path)\
  .trigger(availableNow = True)\
  .start("abfss://netflix@netflixprojectstorage.dfs.core.windows.net/bronze/netflix_titles/", mode="overwrite")

# COMMAND ----------

