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
    .json("abfss://bronze@moviehistory2.dfs.core.windows.net/person.json")

display(person_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 2 - Renombrar columnas y agregar nuevas
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit, concat, col


# COMMAND ----------

person_with_columns_df = person_df \
    .withColumnRenamed("personId", "person_id") \
    .withColumn("name", concat(col("personName.forename"), lit(" "), col("personName.surname"))) \
    .withColumn("ingestion_date", current_timestamp()) \
    .withColumn("environment", lit(v_environment)) 



# COMMAND ----------

# MAGIC %md
# MAGIC #### paso 3 - Eliminar columnas no utilizadas

# COMMAND ----------

persons_final_df = person_with_columns_df.drop("personName")

display(persons_final_df)


# COMMAND ----------

# MAGIC %md
# MAGIC #### Paso 4 - Escribir la salida en un archivo parquet

# COMMAND ----------

persons_final_df.write.mode("overwrite").parquet("abfss://silver@moviehistory2.dfs.core.windows.net/persons")

# COMMAND ----------


display(spark.read.parquet("abfss://silver@moviehistory2.dfs.core.windows.net/persons"))

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit("success")