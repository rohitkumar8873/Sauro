# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Load CSV with Dynamic Schema from JSON File
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

# DBTITLE 1,Load Party Address Data from CSV File
# Load Party Address Data from CSV File
df_partyAddress_read=read_raw_csv_file("Purchase","PartyAddress.cdm.json","PartyAddress")
display(df_partyAddress_read)

# COMMAND ----------

# DBTITLE 1,Store Party Address Data in Bronze Layer
# Store Party Address Data in Bronze Layer
write_raw_to_bronze_databricks(df_partyAddress_read,"PartyAddress")


# COMMAND ----------

# write_raw_to_bronze_adls(df_party_read,"PartyAddress")