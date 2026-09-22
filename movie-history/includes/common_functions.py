# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
from pyspark.sql.functions import current_timestamp



# COMMAND ----------

def add_ingestion_date(input_df):
    output_df = input_df.withColumn("ingestion_date", current_timestamp())
    return output_df

# COMMAND ----------

def delete_partition(full_table_name , column_partition, file_date ):
    if spark.catalog.tableExists(f"{full_table_name}"):
        output_df = spark.sql(f"""
        DELETE FROM {full_table_name}
        WHERE {column_partition} = '{file_date}' 
        """)
    else:
        print(f"Table {full_table_name} does not exist")
        output_df = None
    
    return output_df