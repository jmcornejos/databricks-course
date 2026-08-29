# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesta del archivo "movie_language.json" (archivo multilinea)

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
movie_language_schema = StructType(fields=[
    StructField("movieId", IntegerType(), True),
    StructField("languageId", IntegerType(), True),
    StructField("languageRoleId", IntegerType(), True)
])


# COMMAND ----------

movies_languages_df = spark.read \
    .schema(movie_language_schema) \
    .option("multiline", "true") \
    .json("abfss://bronze@moviehistory2.dfs.core.windows.net/movie_language/")

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
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("environment", lit(v_environment)) 

display(movies_languages_renamed_df.limit(10))



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
    .select("movie_id", "language_id", "ingestion_date", "environment" )

#display(movies_languages_final_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en un archivo parquet

# COMMAND ----------

movies_languages_final_df.write.mode("overwrite").parquet("abfss://silver@moviehistory2.dfs.core.windows.net/movies_languages")

# COMMAND ----------


display(spark.read.parquet("abfss://silver@moviehistory2.dfs.core.windows.net/movies_languages"))

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit("success")