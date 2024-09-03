# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest lap_times folder
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1: Read the CSV file using the spark dataframe reader api

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

lap_times_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                       StructField("driverId", IntegerType(), False),
                                      StructField("lap", StringType(), False),
                                      StructField("position", IntegerType(), False),
                                      StructField("time", StringType(), False),
                                      StructField("milliseconds", IntegerType(), False)
                                      ])

# COMMAND ----------

lap_times_df = spark.read\
.schema(lap_times_schema)\
.csv(f"{raw_folder_path}/{v_file_date}/lap_times/lap_times_split*.csv")

# COMMAND ----------

display(lap_times_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2: Rename Columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df = lap_times_df.withColumnsRenamed({"raceId":"race_id","driverId":"driver_id"})\
                        .withColumn("ingestion_date", lit(current_date()))

final_df = add_ingestion_date(final_df)


# COMMAND ----------

# overwrite_partition(final_df, 'f1_processed', 'lap_times', 'race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.lap = src.lap AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'lap_times', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.lap_times
