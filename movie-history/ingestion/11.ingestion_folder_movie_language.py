# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ### Ingesta del archivo "movie_language.json" (archivo multilinea)

# COMMAND ----------

# DBTITLE 1,Parametros
dbutils.widgets.text("p_environment","production")
v_environment = dbutils.widgets.get("p_environment")

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

# DBTITLE 1,configuraciones
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# DBTITLE 1,funciones comunes
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### paso 1- Leer archivo JSON usando "DataFrameReader" de spark   

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType



# COMMAND ----------

# DBTITLE 1,schma de name
movie_language_schema = StructType(fields=[
    StructField("movieId", IntegerType(), True),
    StructField("languageId", IntegerType(), True),
    StructField("languageRoleId", IntegerType(), True)
])


# COMMAND ----------

movies_languages_df = spark.read \
    .schema(movie_language_schema) \
    .option("multiline", "true") \
    .json(f"{bronze_folder_path}/{v_file_date}/movie_language/")

#display(movies_languages_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar columnas y agregar nuevas
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, concat, col


# COMMAND ----------

# DBTITLE 1,Actualizar environment
movies_languages_renamed_df = movies_languages_df \
    .withColumnRenamed("movieId", "movie_id") \
    .withColumnRenamed("languageId", "language_id") \
    .withColumn("environment", lit(v_environment)) \
    .withColumn("file_date", lit(v_file_date)) 

#display(movies_languages_renamed_df.limit(10))



# COMMAND ----------

# MAGIC %md
# MAGIC #### paso 3 - Eliminar columnas no utilizadas

# COMMAND ----------

# DBTITLE 1,drop para eliminar o select de lo que me interesa dejar
#movies_languages_final_df = movies_languages_renamed_df \
#    .drop("languageRoleId") 

#movies_languages_final_df = movies_languages_renamed_df \
#    .drop(col("languageRoleId")) \

movies_languages_final_df = movies_languages_renamed_df \
    .select("movie_id", "language_id", "file_date", "environment" )

#display(movies_languages_final_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en un archivo parquet

# COMMAND ----------

#movies_languages_final_df.write.mode("overwrite").parquet("abfss://silver@moviehistory2.dfs.core.windows.net/movies_languages")

# COMMAND ----------

# DBTITLE 1,Borra particion
delete_partition(f"{catalogo}.{schema_silver}.movies_languages","file_date", v_file_date)

# COMMAND ----------

# DBTITLE 1,Escribe en tabla delta
movies_languages_final_df.write \
    .mode("append") \
    .format("delta") \
    .partitionBy("file_date") \
    .saveAsTable(f"{catalogo}.{schema_silver}.movies_languages")

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select file_date,count(1) 
# MAGIC from moviehistory.movie_silver.movies_languages
# MAGIC group by file_date

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit("success")