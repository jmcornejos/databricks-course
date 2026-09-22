# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ### Ingesta del archivo "movie_genre.json"

# COMMAND ----------

# DBTITLE 1,Parametros
dbutils.widgets.text("p_environment","production")
v_environment = dbutils.widgets.get("p_environment")

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

# DBTITLE 1,Configuraciones
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# DBTITLE 1,Funciones comunes
# MAGIC %run "../includes/common_functions"

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
    .json(f"{bronze_folder_path}/{v_file_date}/movie_genre.json")

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
    .withColumn("environment", lit(v_environment)) \
    .withColumn("file_date", lit(v_file_date)) 
   


# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en un archivo parquet y tabla delta

# COMMAND ----------

# DBTITLE 1,parquet
# movie_genre_final_df \
#     .write.mode("overwrite") \
#     .partitionBy("movie_id") \
#     .parquet("abfss://silver@moviehistory2.dfs.core.windows.net/movies_genres")




# COMMAND ----------

# DBTITLE 1,parquet
# Falta el borrado de la particion, no lo puse ya que no vamos a escribir directo en parquet
# movie_genre_final_df \
#     .write.mode("append") \
#     .partitionBy("file_date") \
#     .parquet("abfss://silver@moviehistory2.dfs.core.windows.net/movies_genres")

# COMMAND ----------

# DBTITLE 1,Borra datos de la particion
delete_partition(f"{catalogo}.{schema_silver}.movies_genres","file_date", v_file_date)

# COMMAND ----------

# DBTITLE 1,Escribe particion
movie_genre_final_df.write \
    .mode("append") \
    .format("delta") \
    .partitionBy("file_date") \
    .saveAsTable(f"{catalogo}.{schema_silver}.movies_genres")

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select file_date,count(1) 
# MAGIC from moviehistory.movie_silver.movies_genres
# MAGIC group by file_date

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit("success")