# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Run Dynamic CSV Loader Using JSON Schema
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

# DBTITLE 1,Capture Current Timestamp for Processing
UpdatedDateTime=datetime.datetime.now()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Bronze tables

# COMMAND ----------

# DBTITLE 1,Load Salesorderline Table into Spark Dataframe
salesorderlinedf=spark.table("devsauro.bronze.salesorderline")


# COMMAND ----------

# DBTITLE 1,Cast Salesorderline Columns to Double Type
from pyspark.sql import functions as f

salesorderlinedf = salesorderlinedf.withColumn("Price", f.col("Price").cast("double"))
salesorderlinedf = salesorderlinedf.withColumn("VatPercentage", f.col("VatPercentage").cast("double"))
salesorderlinedf = salesorderlinedf.withColumn("Qty", f.col("Qty").cast("double"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Dimension/ Fact table

# COMMAND ----------

# DBTITLE 1,Load Promotion Data from Bronze Table into Dataframe
promotabledf=spark.table("devsauro.bronze.promotion")

# COMMAND ----------

# DBTITLE 1,Query All Records from Promotable Table in Bronze
# MAGIC %sql
# MAGIC select * FROM devsauro.bronze.promotable

# COMMAND ----------

# DBTITLE 1,Create Temporary View for Volume Discount Promotions
# MAGIC %sql
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW vwPromotable 
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC   PromotionId,
# MAGIC   CASE PromotionName
# MAGIC     WHEN 'Volume Discount 11 to 20' THEN 11
# MAGIC     WHEN 'Volume Discount 21 to 40' THEN 21
# MAGIC     WHEN 'Volume Discount 41 to 60' THEN 41
# MAGIC     WHEN 'Volume Discount > 60' THEN 61
# MAGIC     ELSE NULL
# MAGIC   END AS VolumeStart,
# MAGIC   CASE PromotionName
# MAGIC     WHEN 'Volume Discount 11 to 20' THEN 20
# MAGIC     WHEN 'Volume Discount 21 to 40' THEN 40
# MAGIC     WHEN 'Volume Discount 41 to 60' THEN 60
# MAGIC     WHEN 'Volume Discount > 60' THEN 9999999
# MAGIC     ELSE NULL
# MAGIC   END AS VolumeEnd,
# MAGIC   ValidFrom,
# MAGIC   ValidTo,
# MAGIC   cast(PromoPercentage as double) AS PromoPercentage
# MAGIC FROM devsauro.bronze.promotable;

# COMMAND ----------

# DBTITLE 1,Query All Records from Promotable View
# MAGIC %sql
# MAGIC select * from vwPromotable;

# COMMAND ----------

# DBTITLE 1,Create Temporary View for Sales Order Line with Discoun ...
# MAGIC %sql
# MAGIC
# MAGIC
# MAGIC
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW vwFactSalesOrderLine 
# MAGIC AS
# MAGIC
# MAGIC SELECT
# MAGIC   s.SalesOrderNumber,
# MAGIC   s.SalesOrderLine,
# MAGIC   CASE
# MAGIC     WHEN isnull(s.LastProcessedChange_DateTime)
# MAGIC         THEN '1900-01-01'
# MAGIC     ELSE 
# MAGIC         s.LastProcessedChange_DateTime
# MAGIC   END AS LastProcessedChange_DateTime,
# MAGIC   from_utc_timestamp(s.DataLakeModified_DateTime, 'CST') AS DataLakeModified_DateTime,
# MAGIC   s.ItemId,
# MAGIC   s.Qty,
# MAGIC   s.Price,
# MAGIC   s.Qty*cast(Price as double)AS TotalAmount,  
# MAGIC   CASE
# MAGIC     WHEN pr.PromotionId IS NULL THEN TotalAmount
# MAGIC   ELSE
# MAGIC     TotalAmount * (1- PR.promoPercentage)
# MAGIC   END  AS TotalAmountWithDiscount,  
# MAGIC
# MAGIC   s.VatPercentage,
# MAGIC   TotalAmountWithDiscount*s.VatPercentage AS VatAmount,
# MAGIC   TotalAmountWithDiscount+VatAmount AS TotalOrderAmount,
# MAGIC   c.CurrencyId,
# MAGIC   from_utc_timestamp(s.BookDate,'CST') AS BookDate,
# MAGIC   cast(date_format(s.BOOKDate,'yyyyMMdd') AS INT) AS BookDateKey,
# MAGIC   s.TrackingNumber,
# MAGIC   s.CustId,
# MAGIC   p.PaymentTypeId,
# MAGIC   pr.PromotionId,
# MAGIC   current_timestamp() AS UpdatedDateTime,
# MAGIC   xxhash64(s.RecordId) AS SalesOrderLineRecordId
# MAGIC FROM devsauro.bronze.salesorderline s
# MAGIC LEFT JOIN devsauro.bronze.currency c ON s.CurrencyCode=c.Code
# MAGIC LEFT JOIN devsauro.silver.dimpaymenttypes AS P ON s.PaymentTypeDesc=P.PaymentTypeDesc
# MAGIC LEFT JOIN vwPromotable AS pr ON
# MAGIC     CASE
# MAGIC         WHEN month(s.BookDate)=1 THEN s.BookDate BETWEEN pr.ValidFrom AND pr.ValidTo
# MAGIC     ELSE
# MAGIC         s.Qty BETWEEN pr.VolumeStart AND pr.VolumeEnd
# MAGIC     END
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cast Salesorderline Amounts and VAT to Double Type
factsalesorderlineddf=spark.table("vwFactSalesOrderLine")
factsalesorderlineddf = factsalesorderlineddf.withColumn("TotalAmount", f.col("TotalAmount").cast("double"))
factsalesorderlineddf = factsalesorderlineddf.withColumn("TotalAmountWithDiscount", f.col("TotalAmountWithDiscount").cast("double"))
factsalesorderlineddf = factsalesorderlineddf.withColumn("VatAmount", f.col("VatAmount").cast("double"))
factsalesorderlineddf = factsalesorderlineddf.withColumn("VatPercentage", f.col("VatPercentage").cast("double"))

display(factsalesorderlineddf)

# COMMAND ----------

# DBTITLE 1,Write Salesorderline Dataframe to Gold Table in Databri ...
write_silver_to_gold_databricks(factsalesorderlineddf,"factsalesorderline_gold")

# COMMAND ----------

# DBTITLE 1,Write Salesorderline DataFrame to Gold Delta Table
# MAGIC %skip
# MAGIC write_silver_to_gold_adls(factsalesorderlineddf,"factsalesorderline_gold")