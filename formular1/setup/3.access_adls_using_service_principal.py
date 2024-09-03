# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data lake Storage using service principal
# MAGIC 1. Register Azure AD Application/ Service Principal
# MAGIC 2. Generate a secrete/ password for the application
# MAGIC 3. set spark config with App/ clientid, directory/ tenant ID and secret
# MAGIC 3. Assign Role 'Storage Blob Data Contributor' to the Data Lake

# COMMAND ----------

client_id = dbutils.secrets.get("formular1-scope", "formular1-client-id")
tenant_id = dbutils.secrets.get("formular1-scope", "formular1-tenant-id")
client_secret = dbutils.secrets.get("formular1-scope", "formular1-client-secret")

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.formular1dlkd.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.formular1dlkd.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.formular1dlkd.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.formular1dlkd.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.formular1dlkd.dfs.core.windows.net", f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# COMMAND ----------

display(dbutils.fs.ls("abfss://demo@formular1dlkd.dfs.core.windows.net"))

# COMMAND ----------

display(spark.read.csv("abfss://demo@formular1dlkd.dfs.core.windows.net/circuits.csv"))

# COMMAND ----------


