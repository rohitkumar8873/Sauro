# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Execute Notebook to Load CSV Using Dynamic JSON Schema
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

# DBTITLE 1,Load Worker Dataframe Using JSON Schema Definition

df_WorkerTable_read=read_raw_csv_file("Hr","WorkerTable.cdm.json","WorkerTable")
display(df_WorkerTable_read)

# COMMAND ----------

# DBTITLE 1,Write Worker Dataframe to Bronze Storage Layer in Datab ...

write_raw_to_bronze_databricks(df_WorkerTable_read,"WorkerTable")


# COMMAND ----------

# DBTITLE 1,Write Currency Dataframe to Bronze Storage Layer
# MAGIC %skip
# MAGIC # Write Currency Dataframe to Bronze Storage Layer
# MAGIC # write_raw_to_bronze_adls(df_WorkerTable_read,"WorkerTable")