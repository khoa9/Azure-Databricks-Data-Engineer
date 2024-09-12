# Databricks notebook source
# MAGIC %md
# MAGIC # Create bronze & silver table - opportunitylineitem 
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

file_path = f"abfss://bronze@<your-data-lake>.dfs.core.windows.net/{v_file_date}/opportunitylineitem.csv"

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

merge_delta_data(df, "bronze", "opportunitylineitem", "<your-catalog>", merge_condition=merge_condition)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## 3. Clean df to prepare for silver 

# COMMAND ----------

semi_clean_df = remove_duplicates_and_nulls(df,"Id")

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, BooleanType, DateType

# Cast createdDate into DateType
clean_df = semi_clean_df.withColumn("CreatedDate", col("CreatedDate").cast(DateType()))

# COMMAND ----------

lineitem_df = clean_df.select(
    col("Id").alias("id"),
    col("OpportunityId").alias("opportunity_id"),
    col("Product2Id").alias("product_id"),
    col("CreatedDate").alias("created_date"),
    col("ProductCode").alias("product_code"),
    col("Name").alias("name"),
    col("Quantity").cast(IntegerType()).alias("quantity"),  # Cast to Interger
    col("TotalPrice").cast(DoubleType()).alias("total_price"),  # Cast to DoubleType
    col("UnitPrice").cast(DoubleType()).alias("unit_price"),  # Cast to DoubleType
    col("ListPrice").cast(DoubleType()).alias("list_price"),  # Cast to DoubleType
    col("Description").alias("description"),
    col("Margin__c").alias("margin"),
    col("List_Price__c").cast(DoubleType()).alias("list_price_c"),  # Cast to DoubleType
    col("Opportunity_Record_Type_Name__c").alias("opportunity_record_type_name"),
    col("Discount_Amount__c").cast(DoubleType()).alias("discount_amount")  # Cast to DoubleType
)


# COMMAND ----------

#create new column ingestion_date
semi_final_df = add_ingestion_date(lineitem_df)
final_df = add_file_date(semi_final_df, v_file_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create/Merge Silver Table

# COMMAND ----------

merge_condition = "tgt.id = src.id"

merge_delta_data(final_df, "silver", "opportunitylineitem", "<your-catalog>", merge_condition=merge_condition)

# COMMAND ----------

dbutils.notebook.exit("Success")
