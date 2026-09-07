# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Título
# MAGIC %md
# MAGIC ##Ingesta del archivo genre.csv

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# DBTITLE 1,Widget p_environment
dbutils.widgets.text("p_environment","production")
v_environment = dbutils.widgets.get("p_environment")

# COMMAND ----------

# DBTITLE 1,Get p_environment
dbutils.widgets.text("p_file_date","2024-12-16")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# DBTITLE 1,Paso 1 header
# MAGIC %md
# MAGIC ####Paso 1 - Leer el archivo CSV usando "DataFrameReader" de Spark

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType 
from pyspark.sql.functions import *

# COMMAND ----------

# DBTITLE 1,Creo el schema para genre
genres_schema = StructType([
    StructField("genreId", IntegerType(), False),
    StructField("genreName", StringType(), True)
])

# COMMAND ----------

# DBTITLE 1,Lectura del CSV genre
genres_df = spark.read \
    .option("header", True) \
    .schema(genres_schema) \
    .csv(f"{bronze_folder_path}/{v_file_date}/genre.csv")

#display(genres_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Paso 2 header
# MAGIC %md
# MAGIC ####Paso 2 - Seleccionar solo las columnas requeridas

# COMMAND ----------

# DBTITLE 1,Opcion 1 select (aca no se puede aplicar funciones a campos)
#genres_selected_df = genres_df.select("genreId", "genreName")

# COMMAND ----------

# DBTITLE 1,Opcion 2 select
#genres_selected_df = genres_df.select(genres_df.genreId, genres_df.genreName)

# COMMAND ----------

# DBTITLE 1,Opcion 3 select
#genres_selected_df = genres_df.select(genres_df["genreId"], genres_df["genreName"])

# COMMAND ----------

# DBTITLE 1,Opcion 4 select incorpora el rename (alias)
# from pyspark.sql.functions import col, lit

# genres_selected_df = genres_df.select(col("genreId").alias("genre_id"), col("genreName").alias("genre_name"))

# display(genres_selected_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Opcion 5 Select sin alias
from pyspark.sql.functions import col, lit

genres_selected_df = genres_df.select(col("genreId"), col("genreName"))

#display(genres_selected_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Paso 3 header
# MAGIC %md
# MAGIC ####Paso 3 - Cambiar el nombre de las columnas según lo requerido

# COMMAND ----------

# DBTITLE 1,Rename forma 1
#genres_renamed_df = genres_selected_df \
#    .withColumnsRenamed({"genreId": "genre_id", "genreName": "genre_name"})

#display(genres_renamed_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Renamed forma 2
genres_renamed_df = genres_selected_df \
    .withColumnRenamed("genreId", "genre_id") \
    .withColumnRenamed("genreName", "genre_name")

#display(genres_renamed_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Paso 4 header
# MAGIC %md
# MAGIC ####Paso 4 - Agregar columnas al dataframe

# COMMAND ----------

# DBTITLE 1,Imports timestamp y lit
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

# DBTITLE 1,Agrega Columnas forma 1
genres_final_df = genres_renamed_df \
    .withColumn("environment", lit(v_environment)) \
    .withColumn("file_date", lit(v_file_date)) 

#display(genres_final_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Agrega Columnas forma 2
#genres_final_df = genres_renamed_df.withColumns({"ingestion_date": current_timestamp(), "environment": lit("production")})

#display(genres_final_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Paso 5 header
# MAGIC %md
# MAGIC ####Paso 5 - Escribir datos en el datalake en formato parquet y tabla delta

# COMMAND ----------

# DBTITLE 1,Escritura en Silver
#genres_final_df.write.mode("overwrite").parquet("abfss://silver@moviehistory2.dfs.core.windows.net/genres")

# COMMAND ----------

genres_final_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(f"{catalogo}.{schema_silver}.genres")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM moviehistory.movie_silver.genres

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit("success")