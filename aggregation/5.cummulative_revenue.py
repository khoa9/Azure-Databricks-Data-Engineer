# Databricks notebook source
# MAGIC %md
# MAGIC # Calculate the cummulative revenue per continent 

# COMMAND ----------

# MAGIC %run "../setup/functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### ## Write SQL Query to create cummulative_revenue table in gold layer (aggregation layer)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS <your-catalog>.gold.cummulative_revenue;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS <your-catalog>.gold.cummulative_revenue AS 
# MAGIC SELECT 
# MAGIC     MONTH(o.close_date) AS month,
# MAGIC     YEAR(o.close_date) AS year,
# MAGIC     a.continent,
# MAGIC     ROUND(SUM(CAST(o.total_quote_value AS DOUBLE)),2) AS total_quote_value,
# MAGIC     SUM(ROUND(SUM(CAST(o.total_quote_value AS DOUBLE)),2)) 
# MAGIC         OVER (PARTITION BY a.continent ORDER BY YEAR(o.close_date), MONTH(o.close_date)) AS cumulative_quote_value --window function
# MAGIC FROM <your-catalog>.silver.opportunity AS o
# MAGIC JOIN <your-catalog>.silver.account AS a
# MAGIC ON o.account_id = a.id
# MAGIC WHERE 
# MAGIC     o.stage_name = 'Closed Won' 
# MAGIC     AND o.total_quote_value != '0' 
# MAGIC     AND o.is_parent = 'True'
# MAGIC GROUP BY 
# MAGIC     MONTH(o.close_date),
# MAGIC     YEAR(o.close_date),
# MAGIC     a.continent
# MAGIC ORDER BY 
# MAGIC     a.continent, 
# MAGIC     year, 
# MAGIC     month;
# MAGIC
# MAGIC
# MAGIC
