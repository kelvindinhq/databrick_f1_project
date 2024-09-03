# Databricks notebook source
# MAGIC %md
# MAGIC ### Explore the capabilities of the dbutils.secretes utility
# MAGIC

# COMMAND ----------

dbutils.secrets.help()

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

dbutils.secrets.list("formular1-scope")

# COMMAND ----------

dbutils.secrets.get("formular1-scope", "formular1dl-account-key")
