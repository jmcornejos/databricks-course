# Databricks notebook source
# MAGIC %md
# MAGIC ####Spark JOIN

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

movies_df = spark.read.parquet(f"{silver_folder_path}/movies") \
    .filter("year_release_date = 2007") 


display(movies_df.limit(5))

# COMMAND ----------

production_country_df = spark.read.parquet(f"{silver_folder_path}/productions_countries")

display(production_country_df.limit(5))
  

# COMMAND ----------

country_df = spark.read.parquet(f"{silver_folder_path}/countries") \
  
display(country_df.limit(5))


# COMMAND ----------



# COMMAND ----------

movies_df.join(production_country_df, "movie_id").join(country_df, "country_id") \
    .select("title", "country_name", "release_date","budget", "revenue","popularity","duration_time")\
    .display()

# COMMAND ----------

movie_production_country_df = movies_df.join(production_country_df,
                            movies_df.movie_id == production_country_df.movie_id,
                            "inner") \
    .join(country_df, production_country_df.country_id == country_df.country_id,
          "inner") \
    .select(movies_df.title, country_df.country_name, movies_df.release_date,movies_df.budget, movies_df.revenue, movies_df.duration_time, movies_df.popularity) 
    
    
display(movie_production_country_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####outer join

# COMMAND ----------

movie_left1_production_country_df = movies_df.join(production_country_df,
                            movies_df.movie_id == production_country_df.movie_id,
                            "semi") \
                        .select(movies_df.title, movies_df.budget)

display(movie_left1_production_country_df.orderBy("title"))

# COMMAND ----------

movie_left2_production_country_df = movies_df.join(production_country_df,
                            movies_df.movie_id == production_country_df.movie_id,
                            "left") \
                        .select(movies_df.title, movies_df.budget).filter(production_country_df.movie_id.isNotNull()).distinct()

display(movie_left2_production_country_df.orderBy("title"))

    

# COMMAND ----------

movie_production_country_df = movies_df.join(production_country_df,
                            movies_df.movie_id == production_country_df.movie_id,
                            "right") \
                        .select(movies_df.title, movies_df.budget,production_country_df.country_id)

display(movie_production_country_df)

# COMMAND ----------

movie_production_country_df = movies_df.join(production_country_df,
                            movies_df.movie_id == production_country_df.movie_id,
                            "full") \
                        .select(movies_df.title, movies_df.budget,production_country_df.country_id)

display(movie_production_country_df.filter(movie_production_country_df.country_id.isNull()))

# COMMAND ----------

movie_semi_production_country_df = movies_df.join(production_country_df,
                            movies_df.movie_id == production_country_df.movie_id,
                            "semi") \
                        .select(movies_df.title, movies_df.budget)

display(movie_semi_production_country_df)

# COMMAND ----------

movie_left_production_country_df

movie_revision_production_country_df =movie_anti_production_country_df.join(movie_left_production_country_df,
                            movies_df.movie_id == production_country_df.movie_id,
                            "anti") \
                        .select(movies_df.title, movies_df.budget)

display(movie_production_country_df)

# COMMAND ----------

movie_production_country_df = movies_df.join(production_country_df,
                            movies_df.movie_id == production_country_df.movie_id,
                            "anti") \
                        .select(movies_df.title, movies_df.budget)

display(movie_production_country_df)