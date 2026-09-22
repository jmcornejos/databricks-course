# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ### Ingesta del archivo "country.json"

# COMMAND ----------

# DBTITLE 1,Prametros
dbutils.widgets.text("p_environment","production")
v_environment = dbutils.widgets.get("p_environment")

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

# DBTITLE 1,configuraciones
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# DBTITLE 1,funciones comunes
# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC #### paso 1- Leer archivo JSON usando "DataFrameReader" de spark   

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType



# COMMAND ----------

# DBTITLE 1,schma de name
name_schema = StructType([
    StructField("forename", StringType(), True),
    StructField("surname", StringType(), True)
])



# COMMAND ----------


person_schema = StructType([
    StructField("personId", IntegerType(), False),
    StructField("personName", name_schema)
])

# COMMAND ----------

person_df = spark.read \
    .schema(person_schema) \
    .json(f"{bronze_folder_path}/{v_file_date}/person.json")

#display(person_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar columnas y agregar nuevas
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, concat, col


# COMMAND ----------

# DBTITLE 1,Agrega y renombra columnas
person_with_columns_df = person_df \
    .withColumnRenamed("personId", "person_id") \
    .withColumn("name", concat(col("personName.forename"), lit(" "), col("personName.surname"))) \
    .withColumn("environment", lit(v_environment)) \
    .withColumn("file_date", lit(v_file_date))



# COMMAND ----------

# MAGIC %md
# MAGIC #### paso 3 - Eliminar columnas no utilizadas

# COMMAND ----------

persons_final_df = person_with_columns_df.drop("personName")

#display(persons_final_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en un archivo parquet

# COMMAND ----------

# DBTITLE 1,parquet
# persons_final_df.write.mode("overwrite").parquet("abfss://silver@moviehistory2.dfs.core.windows.net/persons")

# COMMAND ----------

# DBTITLE 1,Borra particion segun parametro
delete_partition(f"{catalogo}.{schema_silver}.persons","file_date", v_file_date)

# COMMAND ----------

# DBTITLE 1,Graba datos de la particion
persons_final_df.write \
    .mode("append") \
    .format("delta") \
    .partitionBy("file_date") \
    .saveAsTable(f"{catalogo}.{schema_silver}.persons")

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date,count(1) 
# MAGIC from moviehistory.movie_silver.persons
# MAGIC group by file_date

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit("success")