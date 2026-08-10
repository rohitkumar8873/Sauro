# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Dynamic CSV Loading with JSON Schema Integration
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

# DBTITLE 1,Load Purchase Category Data from JSON File
# Load Purchase Category Data from JSON File
df_PurchCategory_read=read_raw_csv_file("Purchase","PurchCategory.cdm.json","PurchCategory")
display(df_PurchCategory_read)

# COMMAND ----------

# DBTITLE 1,Write Raw Data to Bronze Layer for Purchase Category
# Write Raw Data to Bronze Layer for Purchase Category
write_raw_to_bronze_databricks(df_PurchCategory_read,"PurchCategory")

# COMMAND ----------

# DBTITLE 1,Write Raw Purchase Category Data to Bronze ADLS
# MAGIC %skip
# MAGIC write_raw_to_bronze_adls(df_PurchCategory_read,"PurchCategory")