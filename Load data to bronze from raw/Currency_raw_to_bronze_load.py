# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Execute Notebook to Load CSV Using Dynamic JSON Schema
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

# DBTITLE 1,Load Currency Dataframe Using JSON Schema Definition
# Load Currency Dataframe Using JSON Schema Definition
df_currency_read=read_raw_csv_file("Others","Currency.cdm.json","Currency")
display(df_currency_read)

# COMMAND ----------

# DBTITLE 1,Store Currency Dataframe in Bronze Layer of Databricks
# Store Currency Dataframe in Bronze Layer of Databricks
write_raw_to_bronze_databricks(df_currency_read,"Currency")


# COMMAND ----------

# DBTITLE 1,Write Currency Dataframe to Bronze Storage Layer
# Write Currency Dataframe to Bronze Storage Layer
# write_raw_to_bronze_adls(df_currency_read,"Currency")