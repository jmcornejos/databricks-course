# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesta del archivo "country.json"

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

countries_schema = "countryId INT, countryIsoCode STRING, countryName STRING"

# COMMAND ----------

countries_df = spark.read \
    .schema(countries_schema) \
    .json("abfss://bronze@moviehistory2.dfs.core.windows.net/country.json")

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
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("environment", lit(v_environment)) 
    
#display(countries_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en un archivo parquet

# COMMAND ----------

countries_final_df.write.mode("overwrite").parquet("abfss://silver@moviehistory2.dfs.core.windows.net/countries")

# COMMAND ----------

display(countries_final_df)

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit("success")