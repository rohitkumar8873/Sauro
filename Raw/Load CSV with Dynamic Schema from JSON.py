# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import pyspark.sql.functions as f
import datetime
import pandas as pd
import dateutil

# COMMAND ----------

# DBTITLE 1,Set Location Path for Azure Data Lake Storage Access
# Load CSV with Dynamic Schema from JSON - Databricks
# LOCATION_PATH = "abfss://sauro-dev-data@sauroenterprise01.dfs.core.windows.net"

# COMMAND ----------

# DBTITLE 1,Load CSV with Dynamic Schema from JSON Definition
# Load CSV with Dynamic Schema from JSON Definition
import json
from pyspark.sql.types import StructType, StructField, LongType, StringType, TimestampType

def read_raw_csv_file(entity,entity_schema,file_name):
    with open(f"/Volumes/devsauro/raw/files/{entity}/{entity_schema}", "r") as f:
        schema_json = json.load(f)

    type_mapping = {
        "Int64": LongType(),
        "String": StringType(),
        "DateTime": TimestampType(),
    }

    attributes = schema_json["definitions"][0]["hasAttributes"]
    schema = StructType([
        StructField(attr["name"], type_mapping.get(attr.get("dataFormat"), StringType()), True)
        for attr in attributes
    ])

    df = spark.read.format("csv").option("header", False).option('inferSchema','true').schema(schema).load(f"/Volumes/devsauro/raw/files/{entity}/{file_name}")

    return df

# COMMAND ----------

# DBTITLE 1,Write Dataframe to Bronze Table in Databricks
# Write Dataframe to Bronze Table in Databricks

def write_raw_to_bronze_databricks(df,table_name):
  df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"devsauro.bronze.{table_name}")

# COMMAND ----------

# DBTITLE 1,Save Dataframe as Delta Table in Bronze Storage
# MAGIC %skip
# MAGIC # Save Dataframe as Delta Table in Bronze Storage
# MAGIC
# MAGIC def write_raw_to_bronze_adls(df,table_name):
# MAGIC   df.write \
# MAGIC     .format("delta") \
# MAGIC     .mode("overwrite") \
# MAGIC     .save(f"{LOCATION_PATH}/bronze/{table_name}")

# COMMAND ----------

# DBTITLE 1,Write Dataframe to Silver Delta Table in Databricks
# Write Dataframe to Silver Delta Table in Databricks
def write_bronze_to_silver_databricks(df,table_name):
  df.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"devsauro.silver.{table_name}")

# COMMAND ----------

# DBTITLE 1,Save Dataframe to Silver Layer in ADLS as Delta Table
# MAGIC %skip
# MAGIC # Save Dataframe to Silver Layer in ADLS as Delta Table
# MAGIC def write_bronze_to_silver_adls(df,table_name):
# MAGIC   df.write \
# MAGIC     .format("delta") \
# MAGIC     .mode("overwrite") \
# MAGIC     .save(f"{LOCATION_PATH}/silver/{table_name}")