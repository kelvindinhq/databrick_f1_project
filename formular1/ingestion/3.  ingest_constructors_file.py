# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest_constructors.json file
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Read the JSON file using the spark dataframe reader

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

constructor_df = spark.read\
.schema(constructors_schema)\
.json(f"{raw_folder_path}/{v_file_date}/constructors.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2: Drop column
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

constructor_drop_df = constructor_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step3: Rename column

# COMMAND ----------

constructor_final_df = add_ingestion_date(constructor_drop_df.withColumnsRenamed({'constructorId':'constructor_id', 'constructorRef':'constructor_ref'}))
                                          

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

constructor_final_df = constructor_final_df.withColumn('file_date', lit(f"{v_file_date}"))

# COMMAND ----------

display(constructor_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4: Write ouput to parquet file

# COMMAND ----------

#dbutils.fs.rm("dbfs:/mnt/formular1dlkd/processed/constructors", recurse=True)

# COMMAND ----------

constructor_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.constructors")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.constructors
