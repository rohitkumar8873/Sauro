# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

# DBTITLE 1,Load Promotional Dataframe from JSON File Source
# Load Promotional Dataframe from JSON File Source
df_promotable_read=read_raw_csv_file("Sales","PromoTable.cdm.json","PromoTable")
display(df_promotable_read)

# COMMAND ----------

# DBTITLE 1,Write Promo Dataframe to Bronze Delta Table in Databric ...
# Write Promo Dataframe to Bronze Delta Table in Databricks
write_raw_to_bronze_databricks(df_promotable_read,"PromoTable")

# COMMAND ----------

# DBTITLE 1,Store Promo Dataframe in Bronze ADLS Storage Layer
# MAGIC %skip
# MAGIC # Store Promo Dataframe in Bronze ADLS Storage Layer
# MAGIC write_raw_to_bronze_adls(df_promotable_read,"PromoTable")