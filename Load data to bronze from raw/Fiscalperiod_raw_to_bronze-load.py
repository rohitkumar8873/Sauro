# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Execute Dynamic Schema CSV Loader from JSON Source
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

# DBTITLE 1,Load Fiscal Period Dataframe from JSON Configuration
# Load Fiscal Period Dataframe from JSON Configuration
df_fiscalPeriod_read=read_raw_csv_file("Others","FiscalPeriod.cdm.json","FiscalPeriod")
display(df_fiscalPeriod_read)

# COMMAND ----------

# DBTITLE 1,Write Raw Dataframe to Bronze for Fiscal Period
# Write Raw Dataframe to Bronze for Fiscal Period
write_raw_to_bronze_databricks(df_fiscalPeriod_read,"FiscalPeriod")

# COMMAND ----------

# DBTITLE 1,Save Dataframe to Bronze Layer in Azure Data Lake
# MAGIC %skip
# MAGIC write_raw_to_bronze_adls(df_fiscalPeriod_read,"FiscalPeriod")