# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Execute Notebook to Load CSV Using Dynamic Schema
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

# DBTITLE 1,Capture Current Date and Time for Processing
UpdatedDateTime=datetime.datetime.now()

# COMMAND ----------

# DBTITLE 1,Load Currency DataFrame from Bronze Layer in Spark

# Load Currency DataFrame from Bronze Layer in Spark
currencyDf=spark.table("devsauro.bronze.currency")


# COMMAND ----------

display(currencyDf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Dimension/Fact table

# COMMAND ----------

# DBTITLE 1,Transform Currency Data with Timestamp and Hash Key Col ...
# Transform Currency Data with Timestamp and Hash Key Col ...
dimcurrencyDf=currencyDf.filter(currencyDf.RecordId.isNotNull()
    ).select(
        currencyDf.CurrencyId,
        f.trim(currencyDf.Code).alias("CurrencyCode"),
        f.when(currencyDf.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(currencyDf.LastProcessedChange_DateTime.cast("timestamp")).alias("LastProcessedChange_DateTime"),
        f.from_utc_timestamp(currencyDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
        f.trim(currencyDf.Country).alias("Country"),
        f.trim(currencyDf.CurrencyName).alias("CurrencyName"),
        currencyDf.RecordId.alias("CurrencyRecordId")
    ).withColumn("UpadtedDateTime",f.lit(UpdatedDateTime)
    ).withColumn("CurrencyHashKey",f.xxhash64("CurrencyRecordId")
    )

display(dimcurrencyDf)

# COMMAND ----------

# DBTITLE 1,Write Currency DataFrame to Silver Delta Table
# Write Currency DataFrame to Silver Delta Table

write_bronze_to_silver_databricks(dimcurrencyDf,"dimcurrency_silver")

# COMMAND ----------

# DBTITLE 1,Write Currency DataFrame to Silver Layer in ADLS
# MAGIC %skip
# MAGIC # Write Currency DataFrame to Silver Layer in ADLS
# MAGIC write_bronze_to_silver_adls(dimcurrencyDf,"dimcurrency_silver")