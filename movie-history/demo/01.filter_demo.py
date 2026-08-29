# Databricks notebook source
# MAGIC %md
# MAGIC #### Spark Filter transformtion

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

movies_df = spark.read.parquet(f"{silver_folder_path}/movies")

display(movies_df.limit(5))

# COMMAND ----------

movies_filtered_df = movies_df.filter("year_release_date = 2007")

display(movies_filtered_df.limit(5))


# COMMAND ----------

movies_filtered_df = movies_df.filter(movies_df.year_release_date == 2007)

display(movies_filtered_df.limit(5))

# COMMAND ----------

movies_filtered_df = movies_df.filter(movies_df['year_release_date'] == 2007)

display(movies_filtered_df.limit(5))

# COMMAND ----------

from pyspark.sql.functions import col

movies_filtered_df = movies_df.filter(col('year_release_date') == 2007)

display(movies_filtered_df.limit(5))

# COMMAND ----------

from pyspark.sql.functions import col

movies_filtered_df = movies_df.filter(col("year_release_date") == 2007)

display(movies_filtered_df.limit(5))

# COMMAND ----------

from pyspark.sql.functions import col

movies_filtered_df = movies_df.where(col("year_release_date") == 2007)

display(movies_filtered_df.limit(5))

# COMMAND ----------

from pyspark.sql.functions import col

movies_filtered_df = movies_df.where((col("year_release_date") == 2007) & (col("revenue") > 1000000
))
display(movies_filtered_df.limit(5))

# COMMAND ----------

