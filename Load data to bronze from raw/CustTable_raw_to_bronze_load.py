# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

# DBTITLE 1,Load Customer Table Dataframe with JSON Schema
# Load Customer Table Dataframe with JSON Schema
df_custtable_read=read_raw_csv_file("Sales","CustTable.cdm.json","CustTable")
display(df_custtable_read)

# COMMAND ----------

# DBTITLE 1,Save Customer Dataframe to Bronze Delta Table in Databr ...
# Save Customer Dataframe to Bronze Delta Table in Databricks
write_raw_to_bronze_databricks(df_custtable_read,"CustTable")

# COMMAND ----------

# DBTITLE 1,Save customer Dataframe to Bronze ADLS Storage Layer
# MAGIC %skip
# MAGIC # Store customer Dataframe in Bronze Storage Layer
# MAGIC # write_raw_to_bronze_adls(df_custtable_read,"CustTable")