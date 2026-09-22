# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
v_result1 = dbutils.notebook.run("01.ingestion_file_movie", 0, {"p_environment": "developer","p_file_date": "2024-12-16"})
print(v_result1)

# COMMAND ----------

v_result2 = dbutils.notebook.run("02.ingestion_file_language", 0, {"p_environment": "developer","p_file_date": "2024-12-16"})
print(v_result2)


# COMMAND ----------

v_result3 = dbutils.notebook.run("03.ingestion_file_genre", 0, {"p_environment": "developer","p_file_date": "2024-12-16"})
print(v_result3)


# COMMAND ----------

v_result4 = dbutils.notebook.run("04.ingestion_file_country", 0, {"p_environment": "developer","p_file_date": "2024-12-16"})
print(v_result4)


# COMMAND ----------

v_result5 = dbutils.notebook.run("05.ingestion_file_person", 0, {"p_environment": "developer","p_file_date": "2024-12-16"})
print(v_result5)


# COMMAND ----------

v_result6 = dbutils.notebook.run("06.ingestion_file_movie_genre", 0, {"p_environment": "developer","p_file_date": "2024-12-16"})
print(v_result6)


# COMMAND ----------

v_result7 = dbutils.notebook.run("07.ingestion_file_movie_cast", 0, {"p_environment": "developer","p_file_date": "2024-12-16"})
print(v_result7)


# COMMAND ----------

v_result8 = dbutils.notebook.run("08.ingestion_file_language_role", 0, {"p_environment": "developer","p_file_date": "2024-12-16"})
print(v_result8)


# COMMAND ----------

v_result9 = dbutils.notebook.run("09.ingestion_folder_production_company", 0, {"p_environment": "developer","p_file_date": "2024-12-16"})
print(v_result9)


# COMMAND ----------

v_result10 = dbutils.notebook.run("10.ingestion_folder_movie_company", 0, {"p_environment": "developer","p_file_date": "2024-12-16"})
print(v_result10)


# COMMAND ----------

v_result11 = dbutils.notebook.run("11.ingestion_folder_movie_language", 0, {"p_environment": "developer","p_file_date": "2024-12-16"})
print(v_result11)


# COMMAND ----------

v_result12 = dbutils.notebook.run("12.ingestion_folder_production_country", 0, {"p_environment": "developer","p_file_date": "2024-12-16"})
print(v_result12)

# COMMAND ----------

