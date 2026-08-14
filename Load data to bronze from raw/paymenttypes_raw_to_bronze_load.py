# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

# DBTITLE 1,Load Payment Types Dataframe from JSON File Source
# Load Payment Types Dataframe from JSON File Source
df_PaymentTypes_read=read_raw_csv_file("Sales","PaymentTypes.cdm.json","PaymentTypes")
display(df_PaymentTypes_read)

# COMMAND ----------

# DBTITLE 1,Save Payment Types Dataframe to Bronze Delta Table
# Save Payment Types Dataframe to Bronze Delta Table
write_raw_to_bronze_databricks(df_PaymentTypes_read,"PaymentTypes")

# COMMAND ----------

# DBTITLE 1,Write Payment Types Dataframe to Bronze ADLS Storage
# MAGIC %skip
# MAGIC # Write Payment Types Dataframe to Bronze ADLS Storage
# MAGIC write_raw_to_bronze_adls(df_PaymentTypes_read,"PaymentTypes")