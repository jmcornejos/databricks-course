# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ### Ingesta de la caperta "production_company" (archivo multilinea)

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
# MAGIC #### paso 1- Leer los archivo CSV usando "DataFrameReader" de spark   

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType



# COMMAND ----------

# DBTITLE 1,schma de name
production_company_schema = StructType(fields=[
    StructField("companyId", IntegerType(), True),
    StructField("companyName", StringType(), True)
])


# COMMAND ----------

production_company_df = spark.read \
    .schema(production_company_schema) \
    .csv(f"{bronze_folder_path}/{v_file_date}/production_company/")

#display(production_company_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar columnas y agregar nuevas
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, concat, col


# COMMAND ----------

# DBTITLE 1,Actualizar environment
productions_companies_final_df = production_company_df \
    .withColumnRenamed("companyId", "company_id") \
    .withColumnRenamed("companyName", "company_name") \
    .withColumn("environment", lit(v_environment)) \
    .withColumn("file_date", lit(v_file_date)) 
    
    #display(productions_companies_final_df)



# COMMAND ----------

# MAGIC %md
# MAGIC #### paso 3 - Eliminar columnas no utilizadas

# COMMAND ----------

#NO aplica




# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en un archivo parquet

# COMMAND ----------

# DBTITLE 1,Escribir parquet en ADLS
#productions_companies_final_df.write.mode("overwrite").parquet("abfss://silver@moviehistory2.dfs.core.windows.net/productions_companies")

# COMMAND ----------

# DBTITLE 1,Borra particion
delete_partition(f"{catalogo}.{schema_silver}.productions_companies","file_date", v_file_date)

# COMMAND ----------

# DBTITLE 1,Escribir particion en tabla delta
productions_companies_final_df.write \
    .mode("append") \
    .format("delta") \
    .partitionBy("file_date") \
    .saveAsTable(f"{catalogo}.{schema_silver}.productions_companies")

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select file_date,count(1) 
# MAGIC from moviehistory.movie_silver.productions_companies
# MAGIC group by file_date

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit("success")