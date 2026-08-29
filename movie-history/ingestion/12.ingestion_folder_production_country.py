# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesta del archivo "production_country.json" (archivo multilinea)

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
production_country_schema = StructType(fields=[
    StructField("movieId", IntegerType(), True),
    StructField("countryId", IntegerType(), True)
])

# COMMAND ----------

productions_countries_df = spark.read \
    .schema(production_country_schema) \
    .option("multiline", "true") \
    .json("abfss://bronze@moviehistory2.dfs.core.windows.net/production_country/")

#display(productions_countries_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar columnas y agregar nuevas
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, concat, col


# COMMAND ----------

# DBTITLE 1,Actualizar environment
productions_countries_final_df = productions_countries_df \
    .withColumnRenamed("movieId", "movie_id") \
    .withColumnRenamed("countryId","country_id") \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("environment", lit(v_environment)) 

#display(productions_countries_final_df)



# COMMAND ----------

# MAGIC %md
# MAGIC #### paso 3 - Eliminar columnas no utilizadas

# COMMAND ----------

#NO aplica




# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en un archivo parquet

# COMMAND ----------

productions_countries_final_df.write.mode("overwrite").parquet("abfss://silver@moviehistory2.dfs.core.windows.net/productions_countries")

# COMMAND ----------


#display(spark.read.parquet("abfss://silver@moviehistory2.dfs.core.windows.net/productions_countries"))

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit("success")