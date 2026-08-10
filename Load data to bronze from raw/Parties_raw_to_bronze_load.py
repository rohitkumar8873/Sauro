# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cell 2
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

# DBTITLE 1,Load Raw Parties Dataframe from JSON Configured CSV
# Load Raw Parties Dataframe from JSON Configured CSV
df_party_read=read_raw_csv_file("Purchase","Parties.cdm.json","Parties")
display(df_party_read)

# COMMAND ----------

# DBTITLE 1,Write Raw Party Data to Bronze Storage in Databricks
# Write Raw Party Data to Bronze Storage in Databricks
write_raw_to_bronze_databricks(df_party_read,"party")
# write_raw_to_bronze_adls(df_party_read,"party")