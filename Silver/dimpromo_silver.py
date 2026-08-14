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

# DBTITLE 1,Load Promotable DataFrame from Bronze Layer in Spark

# Load Promotable DataFrame from Bronze Layer in Spark
promotableDf=spark.table("devsauro.bronze.promotable")


# COMMAND ----------

# MAGIC %skip
# MAGIC display(promotableDf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Dimension/Fact table

# COMMAND ----------

# DBTITLE 1,Filter and Transform Promotable Data with Hash Key
# Filter and Transform Promotable Data with Hash Key
dimpromotableDf=promotableDf.filter(promotableDf.PromotionId.isNotNull()
    ).select(
        promotableDf.PromotionId,
        f.when(promotableDf.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(promotableDf.LastProcessedChange_DateTime.cast("timestamp")).alias("LastProcessedChange_DateTime"),
        f.from_utc_timestamp(promotableDf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
        f.trim(promotableDf.PromotionName).alias("PromotionName"),
        f.trim(promotableDf.PromoCode).alias("PromoCode"),
        f.trim(promotableDf.PromoType).alias("PromoType"),
        promotableDf.PromoPercentage,
        f.from_utc_timestamp(promotableDf.ValidFrom,'CST').alias("ValidFrom"),
        f.from_utc_timestamp(promotableDf.ValidTo,'CST').alias("ValidTo"),
        promotableDf.IsActive,
        promotableDf.RecordId.alias("PromoRecordId")
    ).withColumn("UpadtedDateTime",f.lit(UpdatedDateTime)
    ).withColumn("PromotableHashKey",f.xxhash64("PromoRecordId")
    )

display(dimpromotableDf)




# COMMAND ----------

# DBTITLE 1,Write Promotable DataFrame to Silver Delta Table
#Write Promotable DataFrame to Silver Delta Table

write_bronze_to_silver_databricks(dimpromotableDf,"dimpromotable_silver")

# COMMAND ----------

# DBTITLE 1,Write Promotable DataFrame to Silver Layer in ADLS
# MAGIC %skip
# MAGIC # Write Promotable DataFrame to Silver Layer in ADLS
# MAGIC write_bronze_to_silver_adls(dimpromotableDf,"dimpromotable_silver")