# Databricks notebook source
# MAGIC %md
# MAGIC # Calculate percentage by Continent with/without Project Creation
# MAGIC

# COMMAND ----------

# MAGIC %run "../setup/functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write SQL Query to create project_profit table in gold layer (aggregation layer)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS <your-catalog>.gold.project_created;
# MAGIC
# MAGIC CREATE OR REPLACE TEMP VIEW opportunity_count AS 
# MAGIC SELECT 
# MAGIC     a.continent,
# MAGIC     CASE 
# MAGIC       WHEN p.created_date IS NOT NULL THEN 'project_created'
# MAGIC       ELSE 'no_project_created' 
# MAGIC     END AS is_project_created,
# MAGIC     COUNT(o.id) AS opportunity_count
# MAGIC   FROM <your-catalog>.silver.opportunity AS o
# MAGIC   JOIN <your-catalog>.silver.account AS a ON o.account_id = a.id
# MAGIC   LEFT JOIN <your-catalog>.silver.project AS p ON o.implementation_project = p.id
# MAGIC   WHERE 
# MAGIC     o.stage_name = 'Closed Won' AND
# MAGIC     o.is_parent ='True' AND
# MAGIC     o.total_quote_value != '0'
# MAGIC   GROUP BY 
# MAGIC     a.continent,
# MAGIC     CASE WHEN p.created_date IS NOT NULL THEN 'project_created' ELSE 'no_project_created' END;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS <your-catalog>.gold.project_created AS
# MAGIC SELECT
# MAGIC   continent,
# MAGIC   is_project_created,
# MAGIC   opportunity_count,
# MAGIC   ROUND((opportunity_count * 100.0 / SUM(opportunity_count) OVER (PARTITION BY continent)), 2) AS percentage
# MAGIC FROM opportunity_count
# MAGIC ORDER BY continent, is_project_created;
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------


