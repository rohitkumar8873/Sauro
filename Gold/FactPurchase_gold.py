# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Execute Dynamic CSV Loading with JSON Schema
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

# DBTITLE 1,Set Current DateTime for Timestamping
UpdatedDateTime=datetime.datetime.now()

# COMMAND ----------

# DBTITLE 1,Load DataFrames from Bronze and Silver Tables in Spark
# Load DataFrames from Bronze and Silver Tables in Spark
purchaseorderDf=spark.table("devsauro.bronze.purchaseorder")
dimcostcenterDf=spark.table("devsauro.silver.dimcostcenter_silver")
dimcurrencyDf=spark.table("devsauro.silver.dimcurrency_silver")


# COMMAND ----------

# MAGIC %skip
# MAGIC display(purchaseorderDf)
# MAGIC display(dimcostcenterDf)
# MAGIC display(dimcurrencyDf)

# COMMAND ----------

# DBTITLE 1,Create Fact Purchase Order Table with Joins and Hashing
# transforms the purchase order data by joining it with cost center and currency dimension tables,
# applies business logic for null handling and date formatting, calculates total amounts including VAT,
# and generates surrogate keys and hash keys for downstream analytics. The resulting DataFrame is ready for loading into a fact table.


factpurchaseorderDf=purchaseorderDf.filter(purchaseorderDf.RecordId.isNotNull()
    ).join(dimcostcenterDf,purchaseorderDf.CostCenter==dimcostcenterDf.CostCenterNumber,"left"
    ).join(dimcurrencyDf,purchaseorderDf.currencycode==dimcurrencyDf.CurrencyCode,"left"
    ).select(
        purchaseorderDf.PoNumber,
        purchaseorderDf.LineItem,
        purchaseorderDf.VendId.alias("VendorKey"),
        f.when(purchaseorderDf.LastProcessedChange_DateTime.isNull(),"1900-01-01").otherwise(purchaseorderDf.LastProcessedChange_DateTime).cast("timestamp").alias("LastProcessedChange_DateTime"),
        f.from_utc_timestamp(purchaseorderDf.DataLakeModified_DateTime,"CST").alias("DataLakeModified_DateTime"),
        purchaseorderDf.Qty,
        purchaseorderDf.PurchasePrice,
        purchaseorderDf.TotalOrder,
        purchaseorderDf.CostCenter.alias("CostCenterKey"),
        dimcostcenterDf.Vat.alias("VatAmount"),
        f.round((purchaseorderDf.TotalOrder.cast("double")+(purchaseorderDf.TotalOrder.cast("double")*dimcostcenterDf.Vat.cast("double"))),4).alias("TotalAmount"),
        purchaseorderDf.ExchangeRate,
        purchaseorderDf.Itemkey,
        dimcurrencyDf.CurrencyId.alias("CurrencyKey"),
        f.from_utc_timestamp(purchaseorderDf.OrderDate,"CST").alias("OrderDate"),
        f.from_utc_timestamp(purchaseorderDf.ShipDate,"CST").alias("ShipDate"),
        f.from_utc_timestamp(purchaseorderDf.DeliveredDate,"CST").alias("DeliveredDate"),
        f.date_format(purchaseorderDf.OrderDate,"yyyyMMdd").cast("int").alias("OrderDateKey"),
        f.date_format(purchaseorderDf.ShipDate,"yyyyMMdd").cast("int").alias("ShipDateKey"),
        f.date_format(purchaseorderDf.DeliveredDate,"yyyyMMdd").cast("int").alias("DeliveredDateKey"),
        purchaseorderDf.TrackingNumber,
        purchaseorderDf.Batchid.alias("BatchId"),
        purchaseorderDf.CreatedBy,
        purchaseorderDf.RecordId.alias("PurchaseOrderRecordId"),
        purchaseorderDf.CategoryId.alias("CategoryKey")
    ).withColumn("UpdatedDateTime",f.lit(UpdatedDateTime)
    ).withColumn("PurchaseOrderHashKey",f.xxhash64("PurchaseOrderRecordId"))


display(factpurchaseorderDf)

# COMMAND ----------

# DBTITLE 1,Write Fact Purchase Order Dataframe to Gold Table
# Write Fact Purchase Order Dataframe to Gold Table
write_silver_to_gold_databricks(factpurchaseorderDf,"factpurchaseorder")

# COMMAND ----------

# DBTITLE 1,Write Fact Purchase Order DataFrame to Silver Layer in  ...
# MAGIC %skip
# MAGIC # Write Fact Purchase Order DataFrame to Silver Layer in  ...
# MAGIC write_silver_to_gold_adls(factpurchaseorderDf,"factpurchaseorder")