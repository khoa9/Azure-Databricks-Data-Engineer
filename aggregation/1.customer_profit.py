# Databricks notebook source
# MAGIC %md
# MAGIC # Calculate Profit Margin by Customer/ Continent
# MAGIC

# COMMAND ----------

# MAGIC %run "../setup/functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write SQL Query to create customer_profit table in gold layer (aggregation layer)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS <your-catalog>.gold.customer_profit;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS <your-catalog>.gold.customer_profit AS
# MAGIC SELECT 
# MAGIC   a.name as account_name,
# MAGIC   a.continent as account_continent,
# MAGIC   q.currency_conversion_rate, 
# MAGIC   SUM(q.net_amount) as total_opportunity_revenue, -- This number is in the orginial value, not converted
# MAGIC   SUM(CAST(o.opportunity_cost AS DOUBLE)) as total_opportunity_cost, -- This number is in the orginal value, not converted
# MAGIC   ROUND(total_opportunity_revenue - total_opportunity_cost,2) as profit,
# MAGIC   ROUND(((total_opportunity_revenue - total_opportunity_cost)/total_opportunity_revenue) * 100,2) as profit_margin
# MAGIC FROM <your-catalog>.silver.account as a
# MAGIC JOIN <your-catalog>.silver.opportunity as o
# MAGIC ON a.id = o.account_id
# MAGIC JOIN <your-catalog>.silver.quote as q
# MAGIC ON q.opportunity_id = o.id
# MAGIC WHERE 
# MAGIC   o.is_parent = 'True' AND 
# MAGIC   o.stage_name ='Closed Won' AND 
# MAGIC   q.is_primary = True AND 
# MAGIC   q.net_amount != 0
# MAGIC GROUP BY
# MAGIC   a.name,
# MAGIC   a.continent,
# MAGIC   q.currency_conversion_rate
# MAGIC ORDER BY profit DESC;
