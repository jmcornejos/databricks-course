# Databricks notebook source
# MAGIC %md
# MAGIC ### Spark aggregate functions

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

movies_df = spark.read.parquet(f"{silver_folder_path}/movies")

display(movies_df)

# COMMAND ----------

from pyspark.sql.functions import count,countDistinct,sum,max,min,avg

movies_df.select(count('*')).show()




# COMMAND ----------

movies_df.filter("year_release_date = 2016") \
    .select(sum("budget").alias("total_budget"),
            count("movie_id").alias("total_movies")).display()

# COMMAND ----------

movies_df.groupBy("year_release_date") \
    .agg(
    sum("budget").alias("total_budget"),
    count("movie_id").alias("total_movies"),
    max("budget").alias("max_budget"),
    avg("budget").alias("avg_budget"),
    min("budget").alias("min_budget")
    ).display()

# COMMAND ----------

from pyspark.sql.functions import rank,desc,dense_rank
from pyspark.sql.window import Window


# COMMAND ----------

movies_df.select("title","budget","year_release_date") \
    .filter("year_release_date is not null") \
    .withColumn("rank",dense_rank().over(Window.partitionBy("year_release_date").orderBy(desc("budget")))) \
    .display()

# COMMAND ----------

movie_dense_rank = Window.partitionBy("year_release_date").orderBy(desc("budget"))
movie_rank = Window.partitionBy("year_release_date").orderBy(desc("budget"))

movies_df.select("title","budget","year_release_date") \
    .filter("year_release_date is not null") \
    .withColumn("rank",rank().over(movie_rank)) \
    .withColumn("dense_rank",dense_rank().over(movie_dense_rank)) \
    .display()