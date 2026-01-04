# Databricks notebook source
# MAGIC %sql
# MAGIC use catalog netflix_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists gold;
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create table netflix_catalog.gold.netflix_titles
# MAGIC USING Delta
# MAGIC location 'abfss://netflix@netflixprojectstorage.dfs.core.windows.net/silver/netflix_titles'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC describe extended netflix_catalog.gold.netflix_titles

# COMMAND ----------

# MAGIC %sql
# MAGIC create table netflix_catalog.gold.netflix_cast
# MAGIC USING Delta
# MAGIC location 'abfss://netflix@netflixprojectstorage.dfs.core.windows.net/silver/netflix_cast'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create table netflix_catalog.gold.netflix_category
# MAGIC USING Delta
# MAGIC location 'abfss://netflix@netflixprojectstorage.dfs.core.windows.net/silver/netflix_category'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create table netflix_catalog.gold.netflix_countries
# MAGIC USING Delta
# MAGIC location 'abfss://netflix@netflixprojectstorage.dfs.core.windows.net/silver/netflix_countries'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC create table netflix_catalog.gold.netflix_directors
# MAGIC USING Delta
# MAGIC location 'abfss://netflix@netflixprojectstorage.dfs.core.windows.net/silver/netflix_directors'
# MAGIC