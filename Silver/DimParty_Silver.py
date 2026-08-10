# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Execute Notebook for Loading CSV with Dynamic Schema
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

# DBTITLE 1,Capture Current Timestamp for Processing
UpdatedDateTime=datetime.datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Read Bronze Tables

# COMMAND ----------

# DBTITLE 1,Load Bronze Layer Party and Party Address Tables
# Load Bronze Layer Party and Party Address Tables

partiesDf=spark.table("devsauro.bronze.party")
partyAddressDf=spark.table("devsauro.bronze.partyaddress")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Dimension/Fact table

# COMMAND ----------

# DBTITLE 1,Join and Filter Party Data with Address Details
# Join and Filter Party Data with Address Details

partiesDf=partiesDf.join(
    partyAddressDf,partiesDf.PartyId==partyAddressDf.PartyNumber,"left"
    ).filter(partiesDf.RecordId.isNotNull()
    ).select(
        partiesDf.PartyId,
        f.trim(partiesDf.PartyName).alias("PartyName"),
        f.when(partiesDf.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(partiesDf.LastProcessedChange_DateTime).alias("LastProcessedChange_DateTime"),
        f.from_utc_timestamp(partiesDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
        f.trim(partiesDf.PartyAddressCode).alias("PartyAddressCode"),
        f.from_utc_timestamp(partiesDf.EstablishedDate,'CST').alias("EstablishedDate"),
        f.trim(partiesDf.PartyEmailId).alias("PartyEmailId"),
        f.trim(partiesDf.PartyContactNumber).alias("PartyContactNumber"),
        partiesDf.RecordId.alias("PartyRecordId"),
        f.trim(partiesDf.TaxId).alias("TaxId"),
        f.trim(partyAddressDf.Address).alias("Address"),
        f.trim(partyAddressDf.City).alias("City"),
        f.trim(partyAddressDf.State).alias("State"),
        f.trim(partyAddressDf.Country).alias("Country"),
        partyAddressDf.ZipCode,
        f.trim(partyAddressDf.Region).alias("Region"),
        f.from_utc_timestamp(partyAddressDf.ValidFrom,'CST').alias("ValidFrom"),
        # f.from_utc_timestamp(partyAddressDf.ValidTo,'CST').alias("ValidTo"),
        f.when(partyAddressDf.ValidTo.isNull(),"1900-01-01").otherwise(partyAddressDf.ValidTo).alias("ValidTo"),
        partyAddressDf.RecordId.alias("PartyAddressRecordId")
    ).withColumn("UpdatedDateTime",f.lit(UpdatedDateTime)
    ).withColumn("PartyHashKey",f.xxhash64("PartyRecordId"))


display(partiesDf)

# COMMAND ----------

# DBTITLE 1,Save Party Dataframe to Silver Delta Table in Databrick ...
# Save Party Dataframe to Silver Delta Table in Databrick ...
write_bronze_to_silver_databricks(partiesDf,"DimParty_Silver")

# COMMAND ----------

# DBTITLE 1,Write Parties DataFrame to Silver Layer in ADLS
# MAGIC %skip
# MAGIC # Write Parties DataFrame to Silver Layer in ADLS
# MAGIC write_bronze_to_silver_adls(partiesDf,"DimParty_Silver")