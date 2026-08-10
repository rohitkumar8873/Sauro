# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Execute Dynamic CSV Loader Using JSON Schema
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

# DBTITLE 1,Read Purchase Items Data from CSV File
# Read Purchase Items Data from CSV File
df_VendTable_read=read_raw_csv_file("Purchase","VendTable.cdm.json","VendTable")
display(df_VendTable_read)

# COMMAND ----------

# DBTITLE 1,Insert Dataframe into PurchItem Bronze Table
# Insert Dataframe into PurchItem Bronze Table
write_raw_to_bronze_databricks(df_VendTable_read,"VendTable")


# COMMAND ----------

# DBTITLE 1,Write PurchItem Dataframe to Bronze ADLS Storage
# MAGIC %skip
# MAGIC write_raw_to_bronze_adls(df_VendTable_read,"VendTable")