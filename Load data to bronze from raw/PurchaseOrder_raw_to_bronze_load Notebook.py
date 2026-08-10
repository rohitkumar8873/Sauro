# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Load CSV Dynamically Using JSON Schema Script
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

# DBTITLE 1,Load and Display Raw Purchase Order Dataframe
# Load and Display Raw Purchase Order Dataframe
df_PurchaseOrder_read=read_raw_csv_file("Purchase","PurchaseOrder.cdm.json","PurchaseOrder")
display(df_PurchaseOrder_read)

# COMMAND ----------

# DBTITLE 1,Write Raw Purchase Order Data to Bronze Layer
# Write Raw Purchase Order Data to Bronze Layer
write_raw_to_bronze_databricks(df_PurchaseOrder_read,"PurchaseOrder")

# COMMAND ----------

# DBTITLE 1,Write Raw Purchase Order Data to Bronze ADLS Storage
# MAGIC %skip
# MAGIC write_raw_to_bronze_adls(df_PurchaseOrder_read,"PurchaseOrder")