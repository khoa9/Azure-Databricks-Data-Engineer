# Databricks notebook source
# MAGIC %md
# MAGIC # Calculate Product Sold
# MAGIC

# COMMAND ----------

# MAGIC %run "../setup/functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write SQL Query to create product_sold table in gold layer (aggregation layer)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS <your-catalog>.gold.product_sold;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS <your-catalog>.gold.product_sold AS
# MAGIC SELECT 
# MAGIC   p.name as product_name,
# MAGIC   p.product_group,
# MAGIC   a.continent,
# MAGIC   SUM(CAST(ol.quantity AS DOUBLE)) as total_quantity,
# MAGIC   ROUND(SUM(CAST(ol.total_price AS DOUBLE)),2) as total_sale_price_USD
# MAGIC FROM <your-catalog>.silver.opportunitylineitem as ol
# MAGIC JOIN <your-catalog>.silver.opportunity as o 
# MAGIC ON ol.opportunity_id = o.id
# MAGIC JOIN <your-catalog>.silver.account as a
# MAGIC ON  o.account_id = a.id
# MAGIC JOIN <your-catalog>.silver.product as p  
# MAGIC ON  ol.product_id = p.id
# MAGIC WHERE 
# MAGIC   ol.product_id is not null AND 
# MAGIC   o.stage_name = 'Closed Won' AND 
# MAGIC   o.total_quote_value != '0'
# MAGIC GROUP BY 
# MAGIC   p.name,
# MAGIC   p.product_group,
# MAGIC   a.continent
# MAGIC ORDER BY total_quantity DESC, total_sale_price_USD desc;
# MAGIC
