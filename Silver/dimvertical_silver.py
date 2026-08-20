# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Execute Notebook to Load CSV with Schema from JSON
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

# DBTITLE 1,Load and Display Worker Table from Bronze Layer
workerdf=spark.table("devsauro.bronze.workertable")
display(workerdf)

# COMMAND ----------

# DBTITLE 1,Create Silver Dimension Table for Vertical Data
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS devsauro.silver.dimvertical(
# MAGIC     VerticalId BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     Vertical STRING
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Dimesnsion/ Fact Table

# COMMAND ----------

# DBTITLE 1,Select Distinct Trimmed Values of Vertical Column
# from pyspark.sql import functions as f
df = workerdf.select(f.trim(f.col("Vertical")).alias("Vertical")).distinct()
display(df)

# COMMAND ----------

# DBTITLE 1,Display Vertical Data from Silver Dimension Table
verticaldf=spark.table("devsauro.silver.dimvertical")
display(verticaldf)


# COMMAND ----------

# DBTITLE 1,Filter New Rows from DataFrame Excluding Vertical Value ...
newrowsdf=df.filter(f.col("Vertical").isNotNull()).exceptAll(verticaldf.select("Vertical"))
display(newrowsdf)

# COMMAND ----------

# DBTITLE 1,Insert Vertical Data into Silver Dimension Table in Dat ...
spark.sql("insert into devsauro.silver.dimvertical(vertical) select Vertical from {newrowsdf}",newrowsdf=newrowsdf)