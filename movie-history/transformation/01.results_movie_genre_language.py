# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC #### Leer todos los datos que son requeridos

# COMMAND ----------

dbutils.widgets.text("p_file_date","2024-12-16")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

movies_df = spark.read.table(f"{catalogo}.{schema_silver}.movies") \
    .filter(f"file_date = '{v_file_date}'") 
            

# COMMAND ----------

movies_languages_df = spark.read.table(f"{catalogo}.{schema_silver}.movies_languages")\
    .filter(f"file_date = '{v_file_date}'")

# COMMAND ----------

languages_df = spark.read.table(f"{catalogo}.{schema_silver}.languages")

# COMMAND ----------

genres_df = spark.read.table(f"{catalogo}.{schema_silver}.genres")

# COMMAND ----------

movies_genres_df = spark.read.table(f"{catalogo}.{schema_silver}.movies_genres")\
    .filter(f"file_date = '{v_file_date}'")



# COMMAND ----------

# MAGIC %md
# MAGIC #### Join "languages y movies_languages"

# COMMAND ----------

languages_movies_languages_df = movies_languages_df \
    .join(languages_df, 
          movies_languages_df.language_id == languages_df.language_id, 
          "inner") \
    .select(languages_df.language_name, movies_languages_df.movie_id)

#display(languages_movies_languages_df)


# COMMAND ----------

# genres_languages_df = genres_df.join(languages_df, genres_df.language_id == languages_df.language_id, "inner").select("genre_id", "language_name")
# movies_genres_languages_df = movies_genres_df.join(genres_languages_df, movies_genres_df.genre_id == genres_languages_df.genre_id, "inner").select("title", "genre_name")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Join "genres y movies_genres"

# COMMAND ----------

genres_movies_genres_df = genres_df.join(movies_genres_df, 
                                         genres_df.genre_id == movies_genres_df.genre_id, 
                                         "inner") \
    .select(genres_df.genre_name, movies_genres_df.movie_id)

#display(genres_movies_genres_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### JOIN entre
# MAGIC ####  movies_df, languages_movies_languages_df,  genres_movies_genres_df

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Filtrar las peliliculas donde su fecha de lanzamiento sea mayor o igual a 2000
# MAGIC

# COMMAND ----------

movies_filter_df = movies_df.filter(movies_df.year_release_date >= 2000)




# COMMAND ----------

results_movies_genres_languages = movies_filter_df.join(languages_movies_languages_df,
                                movies_filter_df.movie_id == languages_movies_languages_df.movie_id,
                                "inner") \
                                .join(genres_movies_genres_df,
                                    movies_filter_df.movie_id == genres_movies_genres_df.movie_id,
                                    "inner")
                                
                                                                                                   

# COMMAND ----------

# MAGIC %md
# MAGIC #### agregar la columna created_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,desc,asc,lit

# COMMAND ----------

results_df = results_movies_genres_languages \
        .select("title","duration_time","release_date","vote_average","language_name","genre_name") \
        .withColumn("created_date", lit(v_file_date))

#display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### ordenar por la columna "realease_date"

# COMMAND ----------

results_order_by_dataframe = results_df.orderBy("release_date",desc=True)

#otra forma
#results_order_by_dataframe = results_df.orderBy(results_df.release_date.desc())

#display(results_order_by_dataframe)

# COMMAND ----------

# MAGIC %md
# MAGIC ####guarda el resultado en la capa GOLD
# MAGIC
# MAGIC             

# COMMAND ----------

# DBTITLE 1,Borra particion
delete_partition(f"{catalogo}.{schema_gold}.results_movie_genre_language","created_date", v_file_date)

# COMMAND ----------

# DBTITLE 1,como archivo parquet
results_order_by_dataframe.write \
    .mode("append") \
    .format("delta") \
    .partitionBy("created_date") \
    .saveAsTable(f"{catalogo}.{schema_gold}.results_movie_genre_language")


# COMMAND ----------

# DBTITLE 1,como tabla delta
# MAGIC %sql
# MAGIC select created_date,count(1) 
# MAGIC from moviehistory.movie_gold.results_movie_genre_language
# MAGIC group by created_date

# COMMAND ----------

dbutils.notebook.exit("OK")