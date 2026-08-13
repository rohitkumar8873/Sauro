# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Execute Notebook for Dynamic CSV Loading from JSON
# MAGIC %run "/Workspace/Users/rohitsauro21@gmail.com/Sauro/Raw/Load CSV with Dynamic Schema from JSON"

# COMMAND ----------

# DBTITLE 1,Capture Current Date and Time for Processing
UpdatedDateTime=datetime.datetime.now()

# COMMAND ----------

# DBTITLE 1,Load Fiscal Period Data from Bronze Table in Databricks
# Load Fiscal Period Data from Bronze Table in Databricks
FiscalPeriodDf=spark.table("devsauro.bronze.fiscalperiod")
display(FiscalPeriodDf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Date Dimesion from Python

# COMMAND ----------

# DBTITLE 1,Define and Format Date Range for Analysis Period
# Define and Format Date Range for Analysis Period



start_date=datetime.date(2018,1,1)
end_date=start_date+dateutil.relativedelta.relativedelta(years=8,month=12,day=31)

start_date=datetime.datetime.strptime(
    f'{start_date}','%Y-%m-%d'
)
end_date=datetime.datetime.strptime(
    f'{end_date}','%Y-%m-%d'
)
print(start_date)
print(end_date)

# COMMAND ----------

# DBTITLE 1,Generate Spark DataFrame for Daily Date Range
# Define and Format Date Range for Analysis Period



# Generate Spark DataFrame for Daily Date Range
# 1. Creates a pandas date range using pd.date_range() with daily frequency (freq='D)
#       Generates 3,287 consecutive dates from January 1, 2018 onwards
# 2. Converts to DataFrame with a column named Date
# 3. Converts to Spark DataFrame for distributed processing
# Result: A single-column DataFrame with every date in your analysis period.


datepddf=pd.date_range(start_date,end_date,freq='D').to_frame(name='Date')
datedf=spark.createDataFrame(datepddf)
display(datedf)


# COMMAND ----------

# DBTITLE 1,Join Date DataFrame with Valid Fiscal Periods Using Lef ...
# Join Date DataFrame with Valid Fiscal Periods Using Left

# Cell 7: Join Date DataFrame with Valid Fiscal Periods
# This cell enriches each date with fiscal calendar attributes:
# 1. Filters fiscal period data to only valid records (RecordId.isNotNull())
# 2. Performs a LEFT JOIN with a range condition:
#     Matches when: Date >= FiscalStartDate AND Date <= FiscalEndDate
#     This finds which fiscal period each date falls into
# Result: Each date now has 13 additional fiscal attributes including:
#     FiscalPeriodName, FiscalMonth, FiscalQuarter, FiscalYear
#     FiscalStartDate, FiscalEndDate
#     FiscalYearStart, FiscalYearEnd
#     FiscalQuarterStart, FiscalQuarterEnd

# This is a common pattern for building a date dimension that combines calendar dates with your organization's fiscal calendar structure.

joindf=(
    datedf.join(
        FiscalPeriodDf.filter(FiscalPeriodDf.RecordId.isNotNull()),
        (datedf.Date>=FiscalPeriodDf.FiscalStartDate)
        & (datedf.Date<=FiscalPeriodDf.FiscalEndDate),
        'left')
)

display(joindf)

# COMMAND ----------

# Interview Explanation: Diagnosing Duplicate Dates After Fiscal Period Join

"""
Observation:
-------------
Cell 6 produces 3,287 records (one per day), but Cell 7's join results in 3,291 records—4 extra rows. This indicates duplicate dates due to the join logic.

Root Cause:
-------------
The LEFT JOIN in Cell 7 uses a range condition:
    Date >= FiscalStartDate AND Date <= FiscalEndDate
If a date matches multiple fiscal periods (overlapping ranges), the join creates duplicate rows for that date.

Investigation Approach:
-----------------------
1. Group by Date and count occurrences.
2. Filter for dates with count > 1.
3. Examine fiscal period ranges causing overlaps.

Example Investigation:
----------------------
from pyspark.sql import functions as F

# Find duplicate dates after join
dup_dates = (
    joindf.groupBy("Date")
    .agg(F.count("*").alias("row_count"))
    .filter("row_count > 1")
)

display(dup_dates)

# Examine fiscal periods for these dates
overlap_periods = (
    joindf.join(dup_dates, "Date")
    .select("Date", "FiscalPeriodName", "FiscalStartDate", "FiscalEndDate")
    .orderBy("Date")
)

display(overlap_periods)

Specific Issues Found:
----------------------
- February 28, 2023: One period ends, another starts on this date.
- September 28-30, 2025: Three consecutive days fall into both an ending and starting period.
This is due to fiscal periods sharing boundary dates, causing overlapping ranges.

Proposed Solutions:
-------------------
Short-term (Code Fix):
- Use a window function (row_number) partitioned by Date to select one fiscal period per date, based on business logic (e.g., period starting on that date).

Long-term (Data Quality Fix):
- Clean fiscal period data to ensure non-overlapping ranges.
- Enforce: Each period ends the day before the next begins.

Business Impact:
----------------
- Duplicate dates inflate aggregates, cause incorrect period assignments, and mislead reports.
- Early detection prevents faulty business decisions.

Key Interview Points:
---------------------
✅ Data quality mindset
✅ Systematic debugging
✅ Business impact awareness
✅ Multiple solutions (code & data)
✅ Clear communication
"""

# COMMAND ----------

# MAGIC %md
# MAGIC ## Build Date Dimension

# COMMAND ----------

# DBTITLE 1,Create Date Dimension DataFrame with Fiscal and Hash Ke ...

# Build Complete Date Dimension Table
# =====================================
# This cell constructs the final date dimension by selecting and transforming 
# columns from the joined date-fiscal period DataFrame (joindf).
#
# Output Columns:
# ---------------
# Calendar Attributes:
#   - Date: Original date value
#   - DateId: Integer surrogate key (format: yyyyMMdd, e.g., 20180101)
#   - Year, Month, Day: Numeric components of the date
#   - MonthName: Three-letter month abbreviation (e.g., "Jan", "Feb")
#   - DayName: Day of week (e.g., "Mon", "Tue")
#   - Quarter: Calendar quarter (1-4)
#
# Fiscal Calendar Attributes:
#   - FiscalPeriodName: Name of the fiscal period
#   - FiscalStartDate, FiscalEndDate: Fiscal period boundaries
#   - FiscalMonth: Month number within the fiscal year
#   - FiscalYearStart, FiscalYearEnd: Fiscal year boundaries
#   - FiscalQuarter: Fiscal quarter number
#   - FiscalQuarterStart, FiscalQuarterEnd: Fiscal quarter boundaries
#   - FiscalYear: Fiscal year with 'FY' prefix (e.g., "FY2023")
#
# Metadata:
#   - UpdatedDateTime: Timestamp when this dimension was built
#   - DateKey: 64-bit hash key based on DateId for joining/indexing
#
# Note: If joindf contains duplicate dates (from overlapping fiscal periods),
#       this dimension will also contain duplicates. Consider deduplication
#       before loading to the target table.

datedimdf=joindf.select(
    'Date',
    f.date_format(f.col("Date"),"yyyyMMdd").cast("int").alias('DateId'),
    f.year(f.col("Date")).alias("Year"),
    f.month(f.col("Date")).alias("Month"),
    f.date_format(f.col("Date"),"MMM").cast("string").alias("MonthName"),
    f.dayofmonth(f.col("Date")).alias("Day"),
    f.date_format(f.col("Date"),"E").cast("string").alias("DayName"),
    f.quarter(f.col("Date")).alias("Quarter"),
    f.col("FiscalPeriodName").alias("FiscalPeriodName"),
    "FiscalStartDate",
    "FiscalEndDate",
    "FiscalMonth",
    "FiscalYearStart",
    "FiscalYearEnd",
    "FiscalQuarter",
    "FiscalQuarterStart",
    "FiscalQuarterEnd",
    f.concat(f.lit('FY'),"FiscalYear").alias('FiscalYear'),
    f.lit(UpdatedDateTime).alias('UpdatedDateTime'),
    f.xxhash64("DateId").alias('DateKey')
)
display(datedimdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to silver Schema

# COMMAND ----------

# DBTITLE 1,Save Date Dimension DataFrame to Silver Delta Table
# Save Date Dimension DataFrame to Silver Delta Table
write_bronze_to_silver_databricks(datedimdf,"dimdate_silver")

# COMMAND ----------

# DBTITLE 1,Write Date Dimension DataFrame to Silver Layer in ADLS
# MAGIC %skip
# MAGIC # Write Date Dimension DataFrame to Silver Layer in ADLS
# MAGIC write_bronze_to_silver_adls(datedimdf,"dimdate_silver")