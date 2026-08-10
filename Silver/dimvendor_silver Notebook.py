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

# DBTITLE 1,Load Bronze Layer Party Table into DataFrame
# Load Bronze Layer Party and Party Address Tables

vendorDf=spark.table("devsauro.bronze.vendtable")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Dimension/Fact table

# COMMAND ----------

# DBTITLE 1,Prepare Vendor DataFrame with Cleaned Fields and Discou ...
# Prepare Vendor DataFrame with Cleaned Fields and Discou ...

dimvendorDf=vendorDf.filter(vendorDf.RecordId.isNotNull()
    ).select(
       vendorDf.VendId,
       f.trim(vendorDf.VendorName).alias("VendorName"),
       f.when(vendorDf.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(vendorDf.LastProcessedChange_DateTime).alias("LastProcessedChange_DateTime"),
       f.from_utc_timestamp(vendorDf.DataLakeModified_DateTime,"CST").alias("DataLakeModified_DateTime"),
       f.trim(vendorDf.Address).alias("Address"),
       f.trim(vendorDf.City).alias("City"),
       f.trim(vendorDf.State).alias("State"),
       f.trim(vendorDf.Country).alias("Country"),
       f.trim(vendorDf.ZipCode).alias("ZipCode"),
       f.trim(vendorDf.Region).alias("Region"),
       f.from_utc_timestamp(vendorDf.ValidFrom,"CST").alias("ValidFrom"),
       f.from_utc_timestamp(vendorDf.ValidTo,"CST").alias("ValidTo"),
       vendorDf.Active,
       vendorDf.RecordId.alias("VendorRecordId"),
       f.trim(vendorDf.TaxId).alias("TaxId"),
       f.trim(vendorDf.CurrencyCode).alias("CurrencyCode")
    ).withColumn("UpdatedDateTime",f.lit(UpdatedDateTime)
    ).withColumn("VendorHashKey",f.xxhash64("VendorRecordId")
    ).withColumn("VendorDiscount",f.when(vendorDf.Country=="US",f.lit(0.01)).when(vendorDf.Country=="UK",f.lit(0.006)).otherwise(f.lit(0))
    )


display(dimvendorDf)

# COMMAND ----------

# DBTITLE 1,Save Vendor Dataframe to Silver Delta Table in Databric ...
# Save Vendor Dataframe to Silver Delta Table in Databric ...
write_bronze_to_silver_databricks(dimvendorDf,"dimvendor")

# COMMAND ----------

# DBTITLE 1,Write Dimvendor DataFrame to Silver Layer in ADLS
# MAGIC %skip
# MAGIC # Write Dimvendor DataFrame to Silver Layer in ADLS
# MAGIC write_bronze_to_silver_adls(dimvendorDf,"dimvendor")