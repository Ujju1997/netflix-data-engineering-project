# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read.format("delta")\
    .option("header", "True")\
    .option("inferSchema", "True")\
    .load("abfss://netflix@netflixprojectstorage.dfs.core.windows.net/bronze/netflix_titles")
     

# COMMAND ----------

display(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------


# try_cast returns null instead of throwing error
df_cleaned = df.withColumns({
    "duration_minutes": coalesce(expr("try_cast(duration_minutes as int)"), lit(0)),
    "duration_seasons": coalesce(expr("try_cast(duration_seasons as int)"), lit(0))
})

df_cleaned.display()

# COMMAND ----------

df_cleaned.printSchema()

# COMMAND ----------

df_cleaned = df_cleaned.withColumn("short_title", split(col("title"), ":").getItem(0))


# COMMAND ----------

from pyspark.sql.window import Window

df_cleaned = df_cleaned.withColumn("rank_by_duration", dense_rank().over(Window.orderBy(col("duration_minutes").desc())))

# COMMAND ----------

df_cleaned.groupby('type').agg(count('*')).display()

# COMMAND ----------

display(df_cleaned)

# COMMAND ----------

df_cleaned.write.format("delta")\
    .mode('overwrite')\
    .option("path", "abfss://netflix@netflixprojectstorage.dfs.core.windows.net/silver/netflix_titles")\
    .save()

# COMMAND ----------

