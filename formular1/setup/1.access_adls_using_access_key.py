# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data lake Storage using key access
# MAGIC 1. Set the spark config fs.azure.account.key
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

formular1dlkd_account_key = dbutils.secrets.get("formular1-scope", "formular1dl-account-key")

# COMMAND ----------

spark.conf.set("fs.azure.account.key.formular1dlkd.dfs.core.windows.net",formular1dlkd_account_key)

# COMMAND ----------



# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formular1dlkd.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formular1dlkd.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


