# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ### Ingesta del archivo "language_role.json" (archivo multilinea)

# COMMAND ----------

# DBTITLE 1,Widget p_environment
dbutils.widgets.text("p_environment","production")
v_environment = dbutils.widgets.get("p_environment")

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### paso 1- Leer archivo JSON usando "DataFrameReader" de spark   

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType



# COMMAND ----------

# DBTITLE 1,schma de name
language_role_schema = StructType(fields=[
    StructField("roleId", IntegerType(), True),
    StructField("languageRole", StringType(), True)
])


# COMMAND ----------

language_role_df = spark.read \
    .schema(language_role_schema) \
    .option("multiline", "true") \
    .json(f"{bronze_folder_path}/{v_file_date}/language_role.json")

#display(language_role_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar columnas y agregar nuevas
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, concat, col


# COMMAND ----------

language_role_final_df = language_role_df \
    .withColumnRenamed("roleId", "role_id") \
    .withColumnRenamed("languageRole", "language_role") \
    .withColumn("environment", lit(v_environment)) \
    .withColumn("file_date", lit(v_file_date)) 



# COMMAND ----------

# MAGIC %md
# MAGIC #### paso 3 - Eliminar columnas no utilizadas

# COMMAND ----------

#NO aplica




# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en un archivo parquet y tabla delta

# COMMAND ----------

#language_role_final_df.write.mode("overwrite").parquet("abfss://silver@moviehistory2.dfs.core.windows.net/languages_roles")

# COMMAND ----------

language_role_final_df.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(f"{catalogo}.{schema_silver}.languages_roles")

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SELECT * FROM moviehistory.movie_silver.languages_roles

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit("success")