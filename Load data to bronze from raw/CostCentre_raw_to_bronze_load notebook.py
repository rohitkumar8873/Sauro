# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Execute Notebook to Load CSV Using JSON Schema
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

# DBTITLE 1,Load and Display Cost Center Dataframe from JSON File
# Load and Display Cost Center Dataframe from JSON File
df_costcentre_read=read_raw_csv_file("Others","CostCenter.cdm.json","CostCenter")
display(df_costcentre_read)

# COMMAND ----------

# DBTITLE 1,Save Cost Centre Dataframe to Bronze Layer in Databrick ...
# Save Cost Centre Dataframe to Bronze Layer in Databrick ...
write_raw_to_bronze_databricks(df_costcentre_read,"CostCentre")

# COMMAND ----------

# DBTITLE 1,Save Cost Centre Data to Bronze Layer in ADLS Storage
# MAGIC %skip
# MAGIC # Save Cost Centre Data to Bronze Layer in ADLS Storage
# MAGIC write_raw_to_bronze_adls(df_costcentre_read,"CostCentre")