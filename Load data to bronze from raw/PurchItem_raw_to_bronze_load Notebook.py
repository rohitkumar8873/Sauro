# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Execute Notebook for Dynamic CSV Schema Loading
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

# DBTITLE 1,Load PurchItem Dataframe from Raw CSV Using JSON Schema
# Load PurchItem Dataframe from Raw CSV Using JSON Schema
df_PurchItem_read=read_raw_csv_file("Purchase","PurchItem.cdm.json","PurchItem")
display(df_PurchItem_read)

# COMMAND ----------

# DBTITLE 1,Persist PurchItem Dataframe to Bronze Layer Storage
# Persist PurchItem Dataframe to Bronze Layer Storage
write_raw_to_bronze_databricks(df_PurchItem_read,"PurchItem")

# COMMAND ----------

# DBTITLE 1,Store PurchItem Dataframe in Bronze ADLS Layer
# MAGIC %skip
# MAGIC write_raw_to_bronze_adls(df_PurchItem_read,"PurchItem")