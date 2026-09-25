# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC ##Ingesta del archivo movie.csv

# COMMAND ----------

# DBTITLE 1,Widget parametros
dbutils.widgets.text("p_environment","")
v_environment = dbutils.widgets.get("p_environment")

dbutils.widgets.text("p_file_date","")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ####Paso 1 - Leer el archivo CSV usando "DataFrameReader" de Spark
# MAGIC

# COMMAND ----------

# DBTITLE 1,Imports
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType 
from pyspark.sql.functions import *


# COMMAND ----------

# DBTITLE 1,Creo el schema para movie
movie_schema = StructType([
    StructField("movieId", IntegerType(), False),
    StructField("title", StringType(), True),
    StructField("budget", DoubleType(), True),
    StructField("homePage", StringType(), True),
    StructField("overview", StringType(), True),
    StructField("popularity", DoubleType(), True),
    StructField("yearReleaseDate", IntegerType(), True),
    StructField("releaseDate", DateType(), True),
    StructField("revenue", DoubleType(), True),
    StructField("durationTime", IntegerType(), True),
    StructField("movieStatus", StringType(), True),
    StructField("tagline", StringType(), True),
    StructField("voteAverage", DoubleType(), True),
    StructField("voteCount", IntegerType(), True)     
])

# COMMAND ----------

movie_df = spark.read \
    .option("header", True) \
    .schema(movie_schema) \
    .csv(f"{bronze_folder_path}/{v_file_date}/movie.csv", nullValue="")

#display(movie_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Paso 2 - Seleccionar solo las columnas requeridas

# COMMAND ----------

# DBTITLE 1,Opcion 1 select (aca no se puede aplicar funciones a campos)
#movies_selected_df = movie_df.select("movieId", "title", "budget", "popularity","yearReleaseDate","releaseDate", "revenue", "durationTime", "voteAverage","voteCount")

                            

# COMMAND ----------

# DBTITLE 1,Opcion 2 select
#movies_selected_df = movie_df.select(movie_df.movieId, movie_df.title, movie_df.budget, movie_df.popularity,movie_df.yearReleaseDate,movie_df.releaseDate, movie_df.revenue, movie_df.durationTime, movie_df.voteAverage, movie_df.voteCount)


# COMMAND ----------

# DBTITLE 1,Opcion 3 select
#movies_selected_df = movie_df.select(movie_df["movieId"], movie_df["title"], movie_df["budget"], movie_df["popularity"],movie_df["yearReleaseDate"],movie_df["releaseDate"], movie_df["revenue"], movie_df["durationTime"], movie_df["voteAverage"], movie_df["voteCount"])


# COMMAND ----------

# DBTITLE 1,Opcion 4 select
from pyspark.sql.functions import col, lit

movies_selected_df = movie_df.select(col("movieId"), col("title"), col("budget"), col("popularity"),col("yearReleaseDate"),col("releaseDate"), col("revenue"), col("durationTime"), col("voteAverage"), col("voteCount"))


#display(movies_selected_df.limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ####Paso 3 - Cambiar el nombre de las columnas según lo requerido

# COMMAND ----------

# DBTITLE 1,Renamed forma 1
movies_renamed_df = movies_selected_df \
    .withColumnRenamed("movieId", "movie_id") \
    .withColumnRenamed("yearReleaseDate", "year_release_date") \
    .withColumnRenamed("releaseDate", "release_date") \
    .withColumnRenamed("durationTime", "duration_time") \
    .withColumnRenamed("voteAverage", "vote_average") \
    .withColumnRenamed("voteCount", "vote_count")

#display(movies_renamed_df.limit(5))


# COMMAND ----------

# DBTITLE 1,Renamed forma 2
#movies_renamed_df = movies_selected_df \
#    .withColumnsRenamed({"movieId": "movie_id", "yearReleaseDate": "year_release_date", "releaseDate": "release_date", "durationTime": "duration_time", "voteAverage": "vote_average", "voteCount": "vote_count"})

#display(movies_renamed_df.limit(5))


# COMMAND ----------

# MAGIC %md
# MAGIC ####Paso 4 - Agregar columnas al dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit



# COMMAND ----------

# DBTITLE 1,Agrega Columnas forma 1
movies_final_df = add_ingestion_date(movies_renamed_df) \
    .withColumn("environment", lit(v_environment)) \
    .withColumn("file_date", lit(v_file_date))

#display(movies_final_df.limit(5))

# COMMAND ----------

# DBTITLE 1,Agrega Columnas forma 2
#movies_final_df = movies_renamed_df.withColumns({"ingestion_date": current_timestamp(), "environment": lit("produccion")})

#display(movies_final_df.limit(5))
        

# COMMAND ----------

# MAGIC %md
# MAGIC ####Paso 5 - Escribir datos en el datalake en formato parquet

# COMMAND ----------

# 1. Activar la partición dinámica en la sesión de Spark
# NO SE PUEDE UTILIZAR CON SEVERLESS
#spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

# COMMAND ----------

# DBTITLE 1,Escribe en adls modo append
# movies_final_df.write \
#      .mode("append") \
#      .partitionBy("file_date") \
#      .parquet(f"{silver_folder_path}/movies")



# COMMAND ----------

# DBTITLE 1,Actualizar solo una particion en adls
# 1. Definir la partición que vas a modificar
# v_file_date

# 2. Leer los datos actuales excluyendo la partición que vas a sobrescribir
#df_existente = spark.read.parquet(f"{silver_folder_path}/movies") \
#    .filter(f"file_date != '{v_file_date}'")

# 3. Preparar tus nuevos datos para esa partición
# movies_final_df

# 4. Unir los datos viejos (sin la partición) con los nuevos
#df_final = df_existente.union(movies_final_df)

# 5. Sobrescribir toda la tabla (pero solo habrás cambiado esa partición)
#df_final.write \
#     .mode("overwrite") \
#     .partitionBy("file_date") \
#     .parquet(f"{silver_folder_path}/movies")

# COMMAND ----------

# DBTITLE 1,Alternativa 0 para sobreescribir particion
#spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

#movies_final_df.write \
#     .mode("overwrite") \
#     .format("delta") \
#     .partitionBy("file_date") \
#     .saveAsTable(f"{catalogo}.{schema_silver}.movies")

# COMMAND ----------

# DBTITLE 1,Alternativa 1 para sobreescribir particion
# if spark.catalog.tableExists(f"{catalogo}.{schema_silver}.movies"):
#     spark.sql(f"""
#         DELETE FROM {catalogo}.{schema_silver}.movies
#         WHERE file_date = '{v_file_date}' 
#     """)

#movies_final_df.write \
#     .mode("overwrite") \
#     .format("delta") \
#     .partitionBy("file_date") \
#     .saveAsTable(f"{catalogo}.{schema_silver}.movies")


# COMMAND ----------

# DBTITLE 1,Alternativa 2 (borrado igual que la 1 pero en una funcion)
#delete_partition(f"{catalogo}.{schema_silver}.movies","file_date", v_file_date).show()

# COMMAND ----------

# DBTITLE 1,Alternativa 2 - escribe la tabla delta modo append
#movies_final_df.write \
#    .mode("append") \
#    .format("delta") \
#    .partitionBy("file_date") \
#    .saveAsTable(f"{catalogo}.{schema_silver}.movies")

# COMMAND ----------

# DBTITLE 1,alternativa 3 - escribe particion con MERGE

from delta.tables import DeltaTable

if spark.catalog.tableExists(f"{catalogo}.{schema_silver}.movies"):

    deltaTable = DeltaTable.forName(spark, f"{catalogo}.{schema_silver}.movies")

    deltaTable.alias("tgt") \
    .merge(
        movies_final_df.alias("src"),
        "tgt.movie_id = src.movie_id AND tgt.file_date = src.file_date"
    ) \
    .whenMatchedUpdateAll() \
    .whenNotMatchedInsertAll() \
    .execute()
else:
    movies_final_df.write \
    .mode("overwrite") \
    .format("delta") \
    .partitionBy("file_date") \
    .saveAsTable(f"{catalogo}.{schema_silver}.movies")

# COMMAND ----------

# MAGIC %sql
# MAGIC select file_date,count(1) 
# MAGIC from moviehistory.movie_silver.movies
# MAGIC group by file_date

# COMMAND ----------

# DBTITLE 1,Exit notebook
dbutils.notebook.exit("success")

# COMMAND ----------

# MAGIC %sql
# MAGIC with temp as (
# MAGIC select ID_Producto_FK,fecha_venta,
# MAGIC try_sum(Total_Venta) over (partition by ID_Producto_FK) as Tota_Acumulado,
# MAGIC row_number() over (partition by ID_Producto_FK order by fecha_venta desc) as rown
# MAGIC from catalogo.ventas.fact_ventas
# MAGIC )
# MAGIC select * from temp where rown = 1

# COMMAND ----------

# MAGIC %sql
# MAGIC with temp as (
# MAGIC select ID_Producto_FK,fecha_venta,
# MAGIC try_sum(Total_Venta) over (partition by ID_Producto_FK order by fecha_venta) as Tota_Acumulado,
# MAGIC row_number() over (partition by ID_Producto_FK order by fecha_venta desc) as rown
# MAGIC from catalogo.ventas.fact_ventas
# MAGIC )
# MAGIC select * from temp where rown=1
# MAGIC
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select ID_Producto_FK,fecha_venta,Total_Venta from catalogo.ventas.fact_ventas
# MAGIC order by ID_Producto_FK,fecha_venta asc