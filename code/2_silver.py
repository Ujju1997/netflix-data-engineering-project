# Databricks notebook source
# MAGIC %md
# MAGIC ####Parameters

# COMMAND ----------

dbutils.widgets.text("source_folder", "")
dbutils.widgets.text("destination_folder", "")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Variables

# COMMAND ----------

var_src_folder = dbutils.widgets.get("source_folder")
var_tgt_folder = dbutils.widgets.get("destination_folder")

# COMMAND ----------

df = spark.read.format("csv").option("header", "True")\
    .option("inferSchema", "True")\
    .load(f"abfss://netflix@netflixprojectstorage.dfs.core.windows.net/bronze/{var_src_folder}")

# COMMAND ----------

df.display()

# COMMAND ----------

df.write.format("delta")\
.mode("append")\
.save(f"abfss://netflix@netflixprojectstorage.dfs.core.windows.net/silver/{var_tgt_folder}")