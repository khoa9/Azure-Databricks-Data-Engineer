# Databricks notebook source
# MAGIC %md
# MAGIC # Create bronze & silver table - opportunity 
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

file_path = f"abfss://bronze@<your-data-lake>.dfs.core.windows.net/{v_file_date}/opportunity.csv"

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

merge_delta_data(df, "bronze", "opportunity", "<your-catalog>", merge_condition=merge_condition)

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

opportunity_df = clean_df.select(col("Id").alias("id"),
                            col("Name").alias("name"),
                            col("AccountId").alias("account_id"),
                            col("CloseDate").alias("close_date"),
                            col("StageName").alias("stage_name"),
                            col("Amount").cast(DoubleType()).alias("amount"),  # Cast to DoubleType
                            col("ExpectedRevenue").alias("expected_revenue"),
                            col("CurrencyIsoCode").alias("currency_code"),
                            col("CreatedDate").alias("created_date"),
                            col("FiscalQuarter").alias("fiscal_quarter"),
                            col("FiscalYear").alias("fiscal_year"),
                            col("Product_Category_Line_Family__c").alias("product_category"),
                            col("Service_Maintenance_Contract__c").alias("service_maintenance_contract"),
                            col("Total_Sales_Price__c").cast(DoubleType()).alias("total_sales_price"),  # Cast to DoubleType
                            col("Total_Discount__c").cast(DoubleType()).alias("total_discount"),  # Cast to DoubleType
                            col("Parent_or_Stand_Alone__c").alias("is_parent"),
                            col("Opportunity_Total_Cost__c").cast(DoubleType()).alias("opportunity_cost"),  # Cast to DoubleType
                            col("Total_Quote_Value__c").cast(DoubleType()).alias("total_quote_value"),  # Cast to DoubleType
                            col("Implementation_Project__c").alias("implementation_project"),
                            col("POM__c").alias("pom_number"),
                            col("SBQQ__PrimaryQuote__c").alias("primary_quote")
                            )



# COMMAND ----------

#create new column ingestion_date
semi_final_df = add_ingestion_date(opportunity_df)
final_df = add_file_date(semi_final_df, v_file_date)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create/Merge Silver Table

# COMMAND ----------

merge_condition = "tgt.id = src.id"

merge_delta_data(final_df, "silver", "opportunity", "<your-catalog>", merge_condition=merge_condition)

# COMMAND ----------

dbutils.notebook.exit("Success")
