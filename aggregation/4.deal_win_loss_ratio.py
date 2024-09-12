# Databricks notebook source
# MAGIC %md
# MAGIC # Calculate the win/loss ratio per continent
# MAGIC

# COMMAND ----------

# MAGIC %run "../setup/functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ### ## Write SQL Query to create deal_win_loss_ratio table in gold layer (aggregation layer)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS <your-catalog>.gold.deal_win_loss_ratio;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS <your-catalog>.gold.deal_win_loss_ratio AS 
# MAGIC SELECT 
# MAGIC     continent,
# MAGIC     total_deal_won,
# MAGIC     total_deal_lost,
# MAGIC     total_deal_won + total_deal_lost as total_closed_deals,
# MAGIC     ROUND((total_deal_won/(total_deal_won + total_deal_lost)) * 100,2) as win_rate, --This metric measures the percentage of total deals that were won. It’s a key performance indicator (KPI) for sales effectiveness.
# MAGIC     ROUND((total_deal_won/total_deal_lost) * 100,2) as success_ratio --This metric compares the total number of deals won to deals lost, giving an indication of how many successful deals occur for each failed one.
# MAGIC FROM
# MAGIC   (SELECT
# MAGIC         a.continent,
# MAGIC         SUM(CASE 
# MAGIC             WHEN o.stage_name = 'Closed Won' THEN 1 
# MAGIC             ELSE 0 
# MAGIC         END) AS total_deal_won,
# MAGIC         SUM(CASE 
# MAGIC             WHEN o.stage_name IN ('Closed Lost', 'Closed Abandoned') THEN 1 
# MAGIC             ELSE 0 
# MAGIC         END) AS total_deal_lost
# MAGIC     FROM <your-catalog>.silver.opportunity AS o
# MAGIC     JOIN <your-catalog>.silver.account AS a
# MAGIC     ON o.account_id = a.id
# MAGIC     WHERE o.is_parent ='True'
# MAGIC     GROUP BY a.continent) as subquery
# MAGIC ORDER BY continent;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------


