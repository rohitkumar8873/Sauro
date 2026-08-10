# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Load CSV with Dynamic Schema from JSON Script
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

# DBTITLE 1,Import and Display Purchase Contract Dataframe from CSV
# Import and Display Purchase Contract Dataframe from CSV
df_PurchContract_read=read_raw_csv_file("Purchase","PurchContracts.cdm.json","PurchContract")
display(df_PurchContract_read)

# COMMAND ----------

# DBTITLE 1,Write Raw Dataframe to Bronze Layer in Databricks
# Write Raw Dataframe to Bronze Layer in Databricks
write_raw_to_bronze_databricks(df_PurchContract_read,"PurchContract")

# COMMAND ----------

# MAGIC %skip
# MAGIC write_raw_to_bronze_adls(df_PurchContract_read,"PurchContract")