# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ##Ingesta del archivo movie.csv

# COMMAND ----------

# DBTITLE 1,Widget p_environment
dbutils.widgets.text("p_environment","")

# COMMAND ----------

v_environment = dbutils.widgets.get("p_environment")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

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
movie_schema = StructType([
    StructField("movieId", IntegerType(), False),
    StructField("title", StringType(), True),
    StructField("budget", DoubleType(), True),
    StructField("homePage", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("yearReleaseDate", IntegerType(), True),
    StructField("releaseDate", DateType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("durationTime", IntegerType(), True),
    StructField("movieStatus", StringType(), True),
    StructField("tagline", StringType(), True),
    StructField("voteAverage", DoubleType(), True),
    StructField("voteCount", IntegerType(), True)     
])

# COMMAND ----------

movie_df = spark.read \
    .option("header", True) \
    .schema(movie_schema) \
    .csv(f"{bronze_folder_path}/movie.csv", nullValue="Hyukjin Kwon")

#display(movie_df.limit(5))

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

# DBTITLE 1,Opcion 4 select
from pyspark.sql.functions import col, lit

movies_selected_df = movie_df.select(col("movieId"), col("title"), col("budget"), col("popularity"),col("yearReleaseDate"),col("releaseDate"), col("revenue"), col("durationTime"), col("voteAverage"), col("voteCount"))


#display(movies_selected_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Paso 3 - Cambiar el nombre de las columnas según lo requerido

# COMMAND ----------

# DBTITLE 1,Renamed forma 1
movies_renamed_df = movies_selected_df \
    .withColumnRenamed("movieId", "movie_id") \
    .withColumnRenamed("yearReleaseDate", "year_release_date") \
    .withColumnRenamed("releaseDate", "release_date") \
    .withColumnRenamed("durationTime", "duration_time") \
    .withColumnRenamed("voteAverage", "vote_average") \
    .withColumnRenamed("voteCount", "vote_count")

#display(movies_renamed_df.limit(5))


# COMMAND ----------

# DBTITLE 1,Renamed forma 2
#movies_renamed_df = movies_selected_df \
#    .withColumnsRenamed({"movieId": "movie_id", "yearReleaseDate": "year_release_date", "releaseDate": "release_date", "durationTime": "duration_time", "voteAverage": "vote_average", "voteCount": "vote_count"})

#display(movies_renamed_df.limit(5))


# COMMAND ----------

# MAGIC %md
# MAGIC ####Paso 4 - Agregar columnas al dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit



# COMMAND ----------

# DBTITLE 1,Agrega Columnas forma 1
movies_final_df = add_ingestion_date(movies_renamed_df) \
    .withColumn("environment", lit(v_environment))

#display(movies_final_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Agrega Columnas forma 2
#movies_final_df = movies_renamed_df.withColumns({"ingestion_date": current_timestamp(), "environment": lit("produccion")})

#display(movies_final_df.limit(5))
        

# COMMAND ----------

# MAGIC %md
# MAGIC ####Paso 5 - Escribir datos en el datalake en formato paruet

# COMMAND ----------

movies_final_df.write \
    .mode("overwrite") \
    .parquet(f"{silver_folder_path}/movies")



# COMMAND ----------

df = spark.read.parquet(f"{silver_folder_path}/movies")
display(df.limit(5))

# COMMAND ----------

movies_final_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(f"{catalogo}.{schema_silver}.movies")


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from moviehistory.movie_silver.movies

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit("success")