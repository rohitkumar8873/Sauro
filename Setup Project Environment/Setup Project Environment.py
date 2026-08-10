# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC # Set-up the project environment for **SAURO** Enterprise

# COMMAND ----------

# MAGIC %md
# MAGIC %md
# MAGIC ## Acess Cloud Storage

# COMMAND ----------

# MAGIC %fs ls 'abfss://sauro-dev-data@sauroenterprise01.dfs.core.windows.net/'
# MAGIC

# COMMAND ----------

LOCATION_PATH='abfss://sauro-dev-data@sauroenterprise01.dfs.core.windows.net/'

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create External Location

# COMMAND ----------

spark.sql(f"""         
CREATE EXTERNAL LOCATION IF NOT EXISTS `sauro-azure-storage-external-location-sauroenterprise01`
URL '{LOCATION_PATH}'
WITH (STORAGE CREDENTIAL `sauro-connector`)
COMMENT 'External location for sauro-azure-storage'      
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Catalog: devsauro

# COMMAND ----------

spark.sql(f"""
CREATE CATALOG IF NOT EXISTS devsauro
MANAGED LOCATION '{LOCATION_PATH}'
COMMENT 'This is the main catalog for the sauro project'         
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Schemas Raw, bronze, silver, gold

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS devsauro.raw")
spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS devsauro.bronze
    MANAGED LOCATION '{LOCATION_PATH}/bronze'
""")
spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS devsauro.silver
    MANAGED LOCATION '{LOCATION_PATH}/silver'
""")
spark.sql(f"""
CREATE SCHEMA IF NOT EXISTS devsauro.gold
    MANAGED LOCATION '{LOCATION_PATH}/gold'
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create Volumes files

# COMMAND ----------

spark.sql(f"""
CREATE EXTERNAL VOLUME IF NOT EXISTS devsauro.raw.files
LOCATION '{LOCATION_PATH}/raw'
""")