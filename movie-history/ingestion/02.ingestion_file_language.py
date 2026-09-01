# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "2"
# ///
# MAGIC %md
# MAGIC ##Ingesta del archivo language.csv

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# DBTITLE 1,Widget p_environment
dbutils.widgets.text("p_environment","production")


# COMMAND ----------

# DBTITLE 1,Get p_environment
v_environment = dbutils.widgets.get("p_environment")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Paso 1 - Leer el archivo CSV usando "DataFrameReader" de Spark
# MAGIC

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType 
from pyspark.sql.functions import *


# COMMAND ----------

# DBTITLE 1,Creo el schema para movie
languages_schema = StructType([
    StructField("languageId", IntegerType(), False),
    StructField("languageCode", StringType(), True),
    StructField("languageName", StringType(), True)
    
])

# COMMAND ----------

languages_df = spark.read \
    .option("header", True) \
    .schema(languages_schema) \
    .csv(f"{bronze_folder_path}/language.csv", nullValue="Hyukjin Kwon")

display(languages_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Paso 2 - Seleccionar solo las columnas requeridas

# COMMAND ----------

# DBTITLE 1,Opcion 1 select (aca no se puede aplicar funciones a campos)
#movies_selected_df = movie_df.select("movieId", "title", "budget", "popularity","yearReleaseDate","releaseDate", "revenue", "durationTime", "voteAverage","voteCount")        

# COMMAND ----------

# DBTITLE 1,Opcion 2 select
#movies_selected_df = movie_df.select(movie_df.movieId, movie_df.title, movie_df.budget, movie_df.popularity,movie_df.yearReleaseDate,movie_df.releaseDate, movie_df.revenue, movie_df.durationTime, movie_df.voteAverage, movie_df.voteCount)

# COMMAND ----------

# DBTITLE 1,Opcion 3 select
#movies_selected_df = movie_df.select(movie_df["movieId"], movie_df["title"], movie_df["budget"], movie_df["popularity"],movie_df["yearReleaseDate"],movie_df["releaseDate"], movie_df["revenue"], movie_df["durationTime"], movie_df["voteAverage"], movie_df["voteCount"])


# COMMAND ----------

# DBTITLE 1,Opcion 4 select incorpora el rename (alias)
# from pyspark.sql.functions import col, lit

# language_selected_df = language_df.select(col("languageId").alias("language_id"), col("languageCode").alias("language_code"), col("languageName").alias("language_name"))   

# display(language_selected_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Opcion 5 Select sin alias
from pyspark.sql.functions import col, lit

languages_selected_df = languages_df.select(col("languageId"), col("languageName"))   

display(languages_selected_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Paso 3 - Cambiar el nombre de las columnas según lo requerido

# COMMAND ----------

# DBTITLE 1,Rename forma 1
#movies_renamed_df = movies_selected_df \
#    .withColumnsRenamed({"movieId": "movie_id", "yearReleaseDate": "year_release_date", "releaseDate": "release_date", "durationTime": "duration_time", "voteAverage": "vote_average", "voteCount": "vote_count"})

#display(movies_renamed_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Renamed forma 2
languages_renamed_df = languages_selected_df \
    .withColumnRenamed("languageId", "language_id") \
    .withColumnRenamed("languageName", "language_name") \
  

display(languages_renamed_df.limit(5))


# COMMAND ----------

# MAGIC %md
# MAGIC ####Paso 4 - Agregar columnas al dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# DBTITLE 1,Agrega Columnas forma 1
languages_final_df = languages_renamed_df \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("environment", lit(v_environment))


#display(languages_final_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Agrega Columnas forma 2
#languages_final_df = languages_renamed_df.withColumns({"ingestion_date": current_timestamp(), "environment": lit("produccion")})

#display(languages_final_df.limit(5))
        

# COMMAND ----------

# MAGIC %md
# MAGIC ####Paso 5 - Escribir datos en el datalake en formato parquet

# COMMAND ----------

languages_final_df.write \
    .mode("overwrite") \
    .parquet(f"{silver_folder_path}/languages")

# COMMAND ----------

languages_final_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(f"{catalogo}.{schema_silver}.languages")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from moviehistory.movie_silver.languages limit 10

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit("success")