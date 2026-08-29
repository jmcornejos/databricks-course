# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesta de la caperta "production_company" (archivo multilinea)

# COMMAND ----------

# DBTITLE 1,Widget p_environment
dbutils.widgets.text("p_environment","production")


# COMMAND ----------

# DBTITLE 1,Get p_environment
v_environment = dbutils.widgets.get("p_environment")

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
    .csv("abfss://bronze@moviehistory2.dfs.core.windows.net/production_company/")

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
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("environment", lit(v_environment)) 

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

productions_companies_final_df.write.mode("overwrite").parquet("abfss://silver@moviehistory2.dfs.core.windows.net/productions_companies")

# COMMAND ----------


#display(spark.read.parquet("abfss://silver@moviehistory2.dfs.core.windows.net/productions_companies"))

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit("success")