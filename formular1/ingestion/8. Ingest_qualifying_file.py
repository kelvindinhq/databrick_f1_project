# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest qualifying folder
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1: Read the Json file using the spark dataframe reader api

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

qualifying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                       StructField("raceId", IntegerType(), True),
                                      StructField("driverId", IntegerType(), False),
                                      StructField("constructorId", IntegerType(), False),
                                      StructField("number", IntegerType(), False),
                                      StructField("position", IntegerType(), False),
                                      StructField("q1", StringType(), False),
                                      StructField("q2", StringType(), False),
                                      StructField("q3", StringType(), False)
                                      ])

# COMMAND ----------

qualifying_df = spark.read\
.schema(qualifying_schema)\
.option('multiLine', True)\
.json(f"{raw_folder_path}/{v_file_date}/qualifying")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2: Rename Columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df = qualifying_df.withColumnsRenamed({"qualifyId":"qualify_id", "raceId":"race_id","driverId":"driver_id","driverRef":"driver_ref","positionText":"position_text"}).withColumn("file_date", lit(f"{v_file_date}"))

final_df = add_ingestion_date(final_df)


# COMMAND ----------

# overwrite_partition(final_df, 'f1_processed', 'qualifying', 'race_id')

# COMMAND ----------

merge_condition = "tgt.qualify_id = src.qualify_id"
merge_delta_data(final_df, 'f1_processed', 'qualifying', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.qualifying;
