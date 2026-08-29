# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesta del archivo "movie_cast.json" (archivo multilinea)

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
    .json("abfss://bronze@moviehistory2.dfs.core.windows.net/movie_cast.json")

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
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("environment", lit(v_environment)) 



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

movies_casts_final_df.write.mode("overwrite").parquet("abfss://silver@moviehistory2.dfs.core.windows.net/movies_casts")

# COMMAND ----------


#display(spark.read.parquet("abfss://silver@moviehistory2.dfs.core.windows.net/movies_casts"))

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit("success")