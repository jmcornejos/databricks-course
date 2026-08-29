# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC #drop schema demo;

# COMMAND ----------

# MAGIC %sql
# MAGIC show databases

# COMMAND ----------

# MAGIC %sql
# MAGIC describe schema extended demo

# COMMAND ----------

df = spark.read.parquet(f"{gold_folder_path}/results_movie_genre_language")

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("results")


# COMMAND ----------

# MAGIC %sql 
# MAGIC describe extended results

# COMMAND ----------

df.write.mode("overwrite").format("delta").saveAsTable("databricks_course_ws.demo.results_movie_genre_language")

# COMMAND ----------

# MAGIC %sql
# MAGIC describe formatted results_movie_genre_language

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from databricks_course_ws.demo.results_movie_genre_language

# COMMAND ----------

