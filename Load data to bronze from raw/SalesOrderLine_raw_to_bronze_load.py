# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

# DBTITLE 1,Load Sales Order Line Dataframe from CDM JSON File
# Load Sales Order Line Dataframe from CDM JSON File
df_SalesOrderLine_read=read_raw_csv_file("Sales","SalesOrderLine.cdm.json","SalesOrderLine")
display(df_SalesOrderLine_read)

# COMMAND ----------

# DBTITLE 1,Store Sales Order Line Data to Bronze Delta Table
# Store Sales Order Line Data to Bronze Delta Table
write_raw_to_bronze_databricks(df_SalesOrderLine_read,"SalesOrderLine")

# COMMAND ----------

# DBTITLE 1,Save Sales Order Line Data to Bronze ADLS Layer
# MAGIC %skip
# MAGIC # Save Sales Order Line Data to Bronze ADLS Layer
# MAGIC write_raw_to_bronze_adls(df_SalesOrderLine_read,"SalesOrderLine")