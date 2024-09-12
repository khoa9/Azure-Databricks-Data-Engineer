# Databricks notebook source
# MAGIC %md
# MAGIC # Create bronze & silver table - Service Contract 
# MAGIC 1. Read csv to df
# MAGIC 2. Create bronze table if not exists, merge if exists
# MAGIC 3. Clean that df
# MAGIC 4. Create silver table if not exists, merge if exists

# COMMAND ----------

# MAGIC %run "../../setup/functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2020-02-01")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Read csv to df

# COMMAND ----------


from pyspark.sql.utils import AnalysisException

file_path = f"abfss://bronze@<your-data-lake>.dfs.core.windows.net/{v_file_date}/servicecontract.csv"

# Check if the file exists using dbutils (for Databricks) or a similar command for your environment
try:
    # Attempt to read the file
    df = spark.read.format("csv")\
            .option("header", "true")\
            .option("quote", "\"")\
            .option("escape", "\"")\
            .option("multiLine", "true")\
            .option("sep", ",")\
            .load(file_path)

except AnalysisException:
    # If the file does not exist, stop execution
    print(f"File at path {file_path} does not exist. Stopping execution.")
    dbutils.notebook.exit("File path does not exist")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create/merge bronze table

# COMMAND ----------

merge_condition = "tgt.Id = src.Id"

merge_delta_data(df, "bronze", "servicecontract", "<your-catalog>", merge_condition=merge_condition)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 3. Clean df to prepare for silver 

# COMMAND ----------

semi_clean_df = remove_duplicates_and_nulls(df,"Id")

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType, DateType,TimestampType

# Cast createdDate into DateType
clean_df = semi_clean_df.withColumn("CreatedDate", col("CreatedDate").cast(DateType()))

# COMMAND ----------

contract_df = clean_df.select(col("Id").alias("id"),
                            col("Name").alias("name"),
                            col("CreatedDate").alias("created_date"),
                            col("Opportunity__c").alias("opportunity_id"),
                            col("SVMXC__Active__c").alias("active"),
                            col("SVMXC__Contract_Price2__c").cast(DoubleType()).alias("contract_price"),  # Cast to DoubleType
                            col("SVMXC__Discounted_Price2__c").cast(DoubleType()).alias("discounted_price"),  # Cast to DoubleType
                            col("SVMXC__End_Date__c").cast(TimestampType()).alias("end_date"),  # Cast to TimestampType
                            col("SVMXC__Start_Date__c").cast(TimestampType()).alias("start_date"),  # Cast to TimestampType
                            col("SVMXC__Weeks_To_Renewal__c").cast(IntegerType()).alias("week_to_renewal"),
                            col("Contract_Type__c").alias("contract_type"),
                            col("Contract_Status__c").alias("status"),
                            col("Invoice_Period_Start_Date__c").cast(TimestampType()).alias("invoice_date_start"),  # Cast to TimestampType
                            col("Billing_Frequency_Months__c").cast(IntegerType()).alias("billing_frequency"),  # Cast to IntegerType
                            col("Total_of_Covered_Products__c").cast(IntegerType()).alias("total_covered_products"),  # Cast to IntegerType
                            col("Agreement_Order_Type__c").alias("agreement_type"),
                            col("Division__c").alias("division"),
                            col("Invoice_Method__c").alias("invoice_method")
                            )



# COMMAND ----------

#create new column ingestion_date
semi_final_df = add_ingestion_date(contract_df)
final_df = add_file_date(semi_final_df, v_file_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create/Merge Silver Table

# COMMAND ----------

merge_condition = "tgt.id = src.id"

merge_delta_data(final_df, "silver", "servicecontract", "<your-catalog>", merge_condition=merge_condition)

# COMMAND ----------

dbutils.notebook.exit("Success")
