# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesta del archivo "movie_genre.json"

# COMMAND ----------

# DBTITLE 1,Widget p_environment
dbutils.widgets.text("p_environment","production")


# COMMAND ----------

# DBTITLE 1,Get p_environment
v_environment = dbutils.widgets.get("p_environment")

# COMMAND ----------

# MAGIC %md
# MAGIC #### paso 1- Leer archivo JSON usando "DataFrameReader" de spark   

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType



# COMMAND ----------

# DBTITLE 1,schma de name
# alternativa para el schema
# movie_genre_schema = StructType(fields=[
#     StructField("movieId", IntegerType(), True),
#     StructField("genreId", IntegerType(), True)
# ])

movie_genre_schema = "movieId INT, genreId STRING"



# COMMAND ----------

movie_genre_df = spark.read \
    .schema(movie_genre_schema) \
    .json("abfss://bronze@moviehistory2.dfs.core.windows.net/movie_genre.json")

#display(movie_genre_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar columnas y agregar nuevas
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, col


# COMMAND ----------

movie_genre_final_df = movie_genre_df \
    .withColumnRenamed("movieId", "movie_id") \
    .withColumnRenamed("genreId", "genre_id") \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("environment", lit(v_environment)) 
   


# COMMAND ----------


#display(movie_genre_final_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en un archivo parquet

# COMMAND ----------

# movie_genre_final_df \
#     .write.mode("overwrite") \
#     .partitionBy("movie_id") \
#     .parquet("abfss://silver@moviehistory2.dfs.core.windows.net/movies_genres")




# COMMAND ----------

movie_genre_final_df \
    .write.mode("overwrite") \
    .parquet("abfss://silver@moviehistory2.dfs.core.windows.net/movies_genres")

# COMMAND ----------


display(spark.read.parquet("abfss://silver@moviehistory2.dfs.core.windows.net/movies_genres"))

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit("success")