# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data lake Storage using shared access token
# MAGIC 1. Set the spark config for SAS token
# MAGIC
# MAGIC 2. List files from demo container
# MAGIC 3. Read data from circuits.csv file

# COMMAND ----------

formulardl_sas_token = dbutils.secrets.get(scope = "formular1-scope", key = "formular1dl-sas-token")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formular1dlkd.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.formular1dlkd.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.formular1dlkd.dfs.core.windows.net" ,formulardl_sas_token)

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formular1dlkd.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formular1dlkd.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


