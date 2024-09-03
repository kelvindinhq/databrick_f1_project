# Databricks notebook source
# MAGIC %md
# MAGIC ### Access Azure Data lake Storage using service principal
# MAGIC 1. Get client_id, tenant id, client_secret from key vault
# MAGIC 2. Set spark config with App/ clientid, directory/ tenant ID and secret
# MAGIC 3. Call the file system utility to mount the storage
# MAGIC 3. Explore the file system utilities to mount(list all mount, unmount)

# COMMAND ----------

def mount_adls(storage_account_name, container_name):
    # get secrets from key vault
    client_id = dbutils.secrets.get("formular1-scope", "formular1-client-id")
    tenant_id = dbutils.secrets.get("formular1-scope", "formular1-tenant-id")
    client_secret = dbutils.secrets.get("formular1-scope", "formular1-client-secret")

    #set spark configuration
    configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret":client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}
    
    # unmount the mount poibnt if already exists
    if any(mount.mountPoint == f"/mnt/{storage_account_name}/{container_name}" for mount in dbutils.fs.mounts()):
        dbutils.fs.unmount(f"/mnt/{storage_account_name}/{container_name}")

    # Mount the storage account container
    dbutils.fs.mount(
    source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
    mount_point = f"/mnt/{storage_account_name}/{container_name}",
    extra_configs = configs)

    display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('formular1dlkd','raw')

# COMMAND ----------

mount_adls('formular1dlkd','processed')

# COMMAND ----------

mount_adls('formular1dlkd','presentation')

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

mount_adls('formular1dlkd','demo')
