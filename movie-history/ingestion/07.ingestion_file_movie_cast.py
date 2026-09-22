# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ### Ingesta del archivo "movie_cast.json" (archivo multilinea)

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
movie_cast_schema = StructType(fields=[
    StructField("movieId", IntegerType(), True),
    StructField("personId", IntegerType(), True),
    StructField("characterName", StringType(), True),
    StructField("genderId", IntegerType(), True),
    StructField("castOrder", IntegerType(), True)
])


# COMMAND ----------

movies_casts_df = spark.read \
    .schema(movie_cast_schema) \
    .option("multiline", "true") \
    .json(f"{bronze_folder_path}/{v_file_date}/movie_cast.json")

#display(movies_casts_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar columnas y agregar nuevas
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, concat, col


# COMMAND ----------

movies_casts_new_df = movies_casts_df \
    .withColumnRenamed("movieId", "movie_id") \
    .withColumnRenamed("personId", "person_id") \
    .withColumnRenamed("characterName", "character_name") \
    .withColumn("environment", lit(v_environment)) \
    .withColumn("file_date", lit(v_file_date)) 



# COMMAND ----------

# MAGIC %md
# MAGIC #### paso 3 - Eliminar columnas no utilizadas

# COMMAND ----------

movies_casts_final_df = movies_casts_new_df \
    .drop(col("genderId"),col("castOrder"))


#display(movies_casts_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en un archivo parquet

# COMMAND ----------

# DBTITLE 1,Escribe particion en archivo parquet ADLS
#movies_casts_final_df.write.mode("overwrite").parquet("abfss://silver@moviehistory2.dfs.core.windows.net/movies_casts")

# COMMAND ----------

# DBTITLE 1,Borra datos de la particion
delete_partition(f"{catalogo}.{schema_silver}.movies_casts","file_date", v_file_date)

# COMMAND ----------

# DBTITLE 1,Escribe particion en tabla delta
movies_casts_final_df.write \
    .mode("append") \
    .format("delta") \
    .partitionBy("file_date") \
    .saveAsTable(f"{catalogo}.{schema_silver}.movies_casts")

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select file_date,count(1) 
# MAGIC from moviehistory.movie_silver.movies_casts
# MAGIC group by file_date

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit("success")