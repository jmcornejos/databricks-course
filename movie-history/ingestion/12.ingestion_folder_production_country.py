# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ### Ingesta del archivo "production_country.json" (archivo multilinea)

# COMMAND ----------

# DBTITLE 1,Widget p_environment
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
production_country_schema = StructType(fields=[
    StructField("movieId", IntegerType(), True),
    StructField("countryId", IntegerType(), True)
])

# COMMAND ----------

productions_countries_df = spark.read \
    .schema(production_country_schema) \
    .option("multiline", "true") \
    .json(f"{bronze_folder_path}/{v_file_date}/production_country/")

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
    .withColumn("environment", lit(v_environment)) \
    .withColumn("file_date", lit(v_file_date))  

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

# DBTITLE 1,Escribe archivo parquet en ADLS
#productions_countries_final_df.write.mode("overwrite").parquet("abfss://silver@moviehistory2.dfs.core.windows.net/productions_countries")

# COMMAND ----------

# DBTITLE 1,Borra particion
delete_partition(f"{catalogo}.{schema_silver}.productions_countries","file_date", v_file_date)

# COMMAND ----------

# DBTITLE 1,Escribe tabla delta

productions_countries_final_df.write \
    .mode("append") \
    .format("delta") \
    .partitionBy("file_date") \
    .saveAsTable(f"{catalogo}.{schema_silver}.productions_countries")

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date,count(1) 
# MAGIC from moviehistory.movie_silver.productions_countries
# MAGIC group by file_date

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit("success")