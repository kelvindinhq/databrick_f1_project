# Databricks notebook source
# MAGIC %md
# MAGIC #Introduction
# MAGIC

# COMMAND ----------

message = "hello world"

# COMMAND ----------

print(message)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT "Hello"

# COMMAND ----------

# MAGIC %scala
# MAGIC val mg = "Hello"
# MAGIC print(mg)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /databricks-datasets

# COMMAND ----------

for file in dbutils.fs.ls('/databricks-datasets/COVID'):
    if file.name.endswith('/'):
        print(file.name)

# COMMAND ----------

# MAGIC %sh
# MAGIC ps
# MAGIC
