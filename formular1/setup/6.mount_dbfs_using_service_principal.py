# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data lake Storage using service principal
# MAGIC 1. Get client_id, tenant id, client_secret from key vault
# MAGIC 2. Set spark config with App/ clientid, directory/ tenant ID and secret
# MAGIC 3. Call the file system utility to mount the storage
# MAGIC 3. Explore the file system utilities to mount(list all mount, unmount)

# COMMAND ----------

client_id = dbutils.secrets.get("formular1-scope", "formular1-client-id")
tenant_id = dbutils.secrets.get("formular1-scope", "formular1-tenant-id")
client_secret = dbutils.secrets.get("formular1-scope", "formular1-client-secret")

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret":client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://demo@formular1dlkd.dfs.core.windows.net/",
  mount_point = "/mnt/formular1dlkd/demo",
  extra_configs = configs)

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formular1dlkd/demo"))

# COMMAND ----------

display(spark.read.csv("/mnt/formular1dlkd/demo/circuits.csv"))

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# For unmount 
# dbutils.fs.unmount("/mnt/formular1dlkd/demo")

# COMMAND ----------


