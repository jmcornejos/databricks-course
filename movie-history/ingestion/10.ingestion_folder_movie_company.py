# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesta de la caperta "movie_company" (archivo multilinea)

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
movie_company_schema = StructType(fields=[
    StructField("movieId", IntegerType(), True),
    StructField("companyId", IntegerType(), True)
])


# COMMAND ----------

movie_company_df = spark.read \
    .schema(movie_company_schema) \
    .csv("abfss://bronze@moviehistory2.dfs.core.windows.net/movie_company/")

display(movie_company_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar columnas y agregar nuevas
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, concat, col


# COMMAND ----------

# DBTITLE 1,Actualizar environment
movies_companies_final_df = movie_company_df \
    .withColumnRenamed("movieId", "movie_id") \
    .withColumnRenamed("companyId", "company_id") \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("environment", lit(v_environment)) 

#display(movies_companies_final_df)



# COMMAND ----------

# MAGIC %md
# MAGIC #### paso 3 - Eliminar columnas no utilizadas

# COMMAND ----------

#NO aplica




# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en un archivo parquet

# COMMAND ----------

movies_companies_final_df.write.mode("overwrite").parquet("abfss://silver@moviehistory2.dfs.core.windows.net/movies_companies")

# COMMAND ----------


display(spark.read.parquet("abfss://silver@moviehistory2.dfs.core.windows.net/movies_companies"))

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit("success")