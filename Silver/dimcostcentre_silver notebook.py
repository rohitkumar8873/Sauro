# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Load CSV Using Dynamic Schema from JSON Script
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

# DBTITLE 1,Set Current DateTime for Timestamping Operations
UpdatedDateTime=datetime.datetime.now()

# COMMAND ----------

# DBTITLE 1,Import Currency DataFrame from Bronze Layer in Spark

# Import Currency DataFrame from Bronze Layer in Spark
costcentreDf=spark.table("devsauro.bronze.costcentre")


# COMMAND ----------

# DBTITLE 1,Display Cost Centre DataFrame for Analysis Insights
display(costcentreDf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Dimension/Fact table

# COMMAND ----------

# DBTITLE 1,Transform Cost Centre DataFrame with Hash and Timestamp
dimcostcenterDf=costcentreDf.filter(costcentreDf.RecordId.isNotNull()                                   
    ).select(
        costcentreDf.CostCenterNumber,
        f.when(costcentreDf.LastProcessedChange_DateTime.isNull(), "1900-01-01").alias("LastProcessedChange_DateTime"),
        f.from_utc_timestamp(costcentreDf.DataLakeModified_DateTime,"CST").alias("DataLakeModified_DateTime"),
        costcentreDf.Vat,
        costcentreDf.RecordId.alias("CostCenterRecordId")

    ).withColumn("UpdatedDateTime",f.lit(UpdatedDateTime)
    ).withColumn("CostCenterHashKey",f.xxhash64("CostCenterRecordId")
    )
    

# COMMAND ----------

display(dimcostcenterDf)

# COMMAND ----------

write_bronze_to_silver_databricks(dimcostcenterDf,"dimcostcenter_silver")

# COMMAND ----------

# MAGIC %skip
# MAGIC # Write Currency DataFrame to Silver Layer in ADLS
# MAGIC write_bronze_to_silver_adls(dimcostcenterDf,"dimcostcenter_silver")