# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Load CSV with Dynamic Schema Using JSON Configuration
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

# DBTITLE 1,Set Current Timestamp for Data Processing
UpdatedDateTime=datetime.datetime.now()

# COMMAND ----------

# DBTITLE 1,Load Bronze Sales Order Line Table into DataFrame
#Read Bronze table:
salesorderlinedf=spark.table("devsauro.bronze.salesorderline")
display(salesorderlinedf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Silver dimension table

# COMMAND ----------

# DBTITLE 1,Create Silver Table for Payment Types in Databricks
# MAGIC %sql
# MAGIC -- CREATE TABLE IF NOT EXISTS devsauro.silver.dimpaymenttypes(
# MAGIC --     PaymentTypeId INT,
# MAGIC --     PaymentTypeDesc STRING
# MAGIC -- )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Dimesion/ Fact table

# COMMAND ----------

# DBTITLE 1,Extract Distinct Payment Type Descriptions from Sales D ...
df=salesorderlinedf.select('PaymentTypeDesc').distinct()
display(df)

# COMMAND ----------

# DBTITLE 1,Load Payment Types Table from Silver Delta Layer
paymenttypedf=spark.table("devsauro.silver.dimpaymenttypes")

# COMMAND ----------

# DBTITLE 1,Filter DataFrame Excluding Existing Payment Types
newrowsdf=df.exceptAll(paymenttypedf.select("PaymentTypeDesc"))
display(newrowsdf)

# COMMAND ----------

# DBTITLE 1,Retrieve Maximum Payment Type ID from Payment DataFrame
maxdf=spark.sql("select ifnull(max(PaymentTypeId),0 ) as maxid from {df}",df=paymenttypedf)
toprow=maxdf.head(1)
maxid=toprow[0][0]
print(maxid)

# COMMAND ----------

import pyspark.sql.window as w

# COMMAND ----------

# DBTITLE 1,Assign Sequential PaymentTypeId by PaymentTypeDesc Orde ...
idsdf=newrowsdf.withColumn('PaymentTypeId',f.row_number().over(window=w.Window.orderBy(f.col("PaymentTypeDesc"))))
display(idsdf)

# COMMAND ----------

# DBTITLE 1,Adjust PaymentTypeId by Adding Max Identifier Value
isdsFinal=idsdf.withColumn('PaymentTypeId',f.col('PaymentTypeId')+maxid+1)
display(isdsFinal)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- drop table devsauro.silver.dimpaymenttypes_silver

# COMMAND ----------

# DBTITLE 1,Write Final Dataset to Silver Table in Databricks
append_write_bronze_to_silver_databricks(isdsFinal,"dimpaymenttypes_silver")

# COMMAND ----------

# MAGIC %skip
# MAGIC
# MAGIC write_bronze_to_silver_adls(isdsFinal,"dimpaymenttypes_silver")