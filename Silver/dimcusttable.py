# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Run Notebook for Loading CSV Using Dynamic JSON Schema
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

# DBTITLE 1,Capture Current Timestamp for Processing
UpdatedDateTime=datetime.datetime.now()

# COMMAND ----------

# DBTITLE 1,Load and Display Customer Table from Bronze Layer

# Load and Display Customer Table from Bronze Layer
custtableDf=spark.table("devsauro.bronze.custtable")
display(custtableDf)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Dimension/Fact table

# COMMAND ----------

# DBTITLE 1,Filter and Transform Customer Data with Timestamps
custtableDf=custtableDf.filter(custtableDf.RecordId.isNotNull()
    ).select(
        custtableDf.CustomerId,
        f.when(custtableDf.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(custtableDf.LastProcessedChange_Date).cast("timestamp").alias("LastProcessedChange_DateTime"),
        f.from_utc_timestamp(custtableDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
        f.trim(custtableDf.CustomerName).alias("CustomerName"),
        f.trim(custtableDf.Email).alias("Email"),
        f.trim(custtableDf.Phone).alias("Phone"),
        f.trim(custtableDf.Address).alias("Address"),
        f.trim(custtableDf.City).alias("City"),
        f.trim(custtableDf.State).alias("State"),
        f.trim(custtableDf.Country).alias("Country"),
        f.trim(custtableDf.ZipCode).alias("ZipCode"),
        f.trim(custtableDf.Region).alias("Region"),
        f.from_utc_timestamp(custtableDf.SignupDate,'CST').alias("SignupDate"),
        custtableDf.RecordId.alias("CustRecordId")
    ).withColumn("UpdatedDateTime",f.lit(UpdatedDateTime)
    ).withColumn("PartyHashKey",f.xxhash64("CustReocrdId")
    )

# COMMAND ----------

# MAGIC %skip
# MAGIC display(custtableDf)

# COMMAND ----------

# DBTITLE 1,Save Customer Dataframe from Bronze to Silver Layer
write_bronze_to_silver_databricks(custtableDf,"dimcusttable_silver")

# COMMAND ----------

# DBTITLE 1,Skip Writing Bronze to Silver Layer in ADLS
# MAGIC %skip
# MAGIC
# MAGIC # write_bronze_to_silver_adls(custtableDf,"dimcusttable_silver")