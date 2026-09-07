# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ### Ingesta del archivo "country.json"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# DBTITLE 1,Widget p_environment
dbutils.widgets.text("p_environment","production")
v_environment = dbutils.widgets.get("p_environment")

# COMMAND ----------

# DBTITLE 1,Widget fecha

dbutils.widgets.text("p_file_date","2024-12-16")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC #### paso 1- Leer archivo JSON usando "DataFrameReader" de spark   

# COMMAND ----------

countries_schema = "countryId INT, countryIsoCode STRING, countryName STRING"

# COMMAND ----------

countries_df = spark.read \
    .schema(countries_schema) \
    .json(f"{bronze_folder_path}/{v_file_date}/country.json")

#display(countries_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Eliminar columnas no deseadas

# COMMAND ----------

# DBTITLE 1,forma 1
countries_dropped_df = countries_df.drop("countryIsoCode")

#display(countries_dropped_df)

# COMMAND ----------

# DBTITLE 1,Forma 2
from pyspark.sql.functions import col

countries_dropped_df = countries_df.drop(col("countryIsoCode"))

#display(countries_dropped_df)

# COMMAND ----------

# DBTITLE 1,forma 3
countries_dropped_df = countries_df.drop(countries_df["countryIsoCode"])

display(countries_dropped_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### paso 3 - Cambiar el nombre de las columnas y agregar "Ingestion:date" y "Environment"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit


# COMMAND ----------


countries_final_df = countries_dropped_df \
    .withColumnsRenamed({"countryId": "country_id", "countryName": "country_name"}) \
     .withColumn("environment", lit(v_environment)) \
     .withColumn("file_date", lit(v_file_date)) 
    
#display(countries_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en un archivo parquet y tabla delta

# COMMAND ----------

#countries_final_df.write.mode("overwrite").parquet("abfss://silver@moviehistory2.dfs.core.windows.net/countries")

# COMMAND ----------

countries_final_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(f"{catalogo}.{schema_silver}.countries")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from moviehistory.movie_silver.countries limit 10

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit("success")