# Databricks notebook source
# MAGIC %md
# MAGIC #### Leer todos los datos que son requeridos

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

movies_df = spark.read.parquet(f"{silver_folder_path}/movies")

# COMMAND ----------

movies_languages_df = spark.read.parquet(f"{silver_folder_path}/movies_languages")

# COMMAND ----------

languages_df = spark.read.parquet(f"{silver_folder_path}/languages")

# COMMAND ----------

genres_df = spark.read.parquet(f"{silver_folder_path}/genres")

# COMMAND ----------

movies_genres_df = spark.read.parquet(f"{silver_folder_path}/movies_genres")



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
                                
                                                    
display(results_movies_genres_languages)
                                                   

# COMMAND ----------

# MAGIC %md
# MAGIC #### agregar la columna created_date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp,desc,asc

# COMMAND ----------

results_df = results_movies_genres_languages \
        .select("title","duration_time","release_date","vote_average","language_name","genre_name") \
        .withColumn("created_date", current_timestamp())

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

results_order_by_dataframe.write \
    .mode("overwrite") \
    .parquet(f"{gold_folder_path}/results_movie_genre_language")


# COMMAND ----------

display(spark.read.parquet(f"{gold_folder_path}/results_movie_genre_language"))