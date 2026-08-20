# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

UpdatedDateTime=datetime.datetime.now()

# COMMAND ----------

workerdf=spark.table("devsauro.bronze.workertable")
verticaldf=spark.table("devsauro.silver.dimvertical")
display(workerdf)
display(verticaldf)

# COMMAND ----------

dimworkerdf = workerdf.filter(workerdf.RecordId.isNotNull()
    ).join(
        verticaldf,workerdf.Vertical == verticaldf.Vertical,"left"
    ).select(
       workerdf.WorkerID,
       f.when(workerdf.LastProcessedChange_DateTime.isNull(), "1900-01-01").otherwise(workerdf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
       f.from_utc_timestamp(workerdf.DataLakeModified_DateTime,'CST').alias("DataLakeModified_DateTime"),
       workerdf.SupervisorId,
       f.trim(workerdf.WorkerName).alias("WorkerName"),
       f.trim(workerdf.WorkerEmail).alias("WorkerEmail"),
       f.trim(workerdf.Phone).alias("Phone"),
       f.from_utc_timestamp(workerdf.DOJ,'CST').alias("DOJ"),
       f.from_utc_timestamp(workerdf.DOL,'CST').alias("DOL"), 
       verticaldf.VerticalId ,
       workerdf.Type,
       workerdf.PayPerAnnum,
       workerdf.Rate,
       workerdf.RecordId.alias("WorkerRecordId")  
    ).withColumn("UpdatedDateTime", f.lit(UpdatedDateTime)
    ).withColumn("WorkerHashKey", f.xxhash64("WorkerRecordId")
    )
display(dimworkerdf)

# COMMAND ----------

# DBTITLE 1,Save DimWorker DataFrame to Silver Delta Table in Datab ...
write_bronze_to_silver_databricks(dimworkerdf,"dimworkertable_silver")

# COMMAND ----------

# DBTITLE 1,Save DimWorker DataFrame to Silver ADLS in Databricks
# MAGIC %skip
# MAGIC
# MAGIC write_bronze_to_silver_adls(dimworkerdf,"dimworkertable_silver")