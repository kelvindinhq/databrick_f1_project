# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest pit stops file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1: Read the Json file using the spark dataframe reader api

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

pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                       StructField("driverId", IntegerType(), False),
                                      StructField("stop", StringType(), False),
                                      StructField("lap", StringType(), False),
                                      StructField("time", StringType(), False),
                                      StructField("duration", StringType(), False),
                                      StructField("milliseconds", IntegerType(), False)
                                      ])

# COMMAND ----------

pit_stops_df = spark.read.schema(pit_stops_schema).option("multiline",True).json(f"{raw_folder_path}/{v_file_date}/pit_stops.json")

# COMMAND ----------

display(pit_stops_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2: Rename Columns and add new columns

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

final_df = pit_stops_df.withColumnsRenamed({"raceId":"race_id","driverId":"driver_id"})\
                        .withColumn("file_date", lit(f"{v_file_date}"))
                        
final_df = add_ingestion_date(final_df)

# COMMAND ----------

#overwrite_partition(final_df, 'f1_processed', 'pit_stops', 'race_id')

# COMMAND ----------

merge_condition = "tgt.race_id = src.race_id AND tgt.driver_id = src.driver_id AND tgt.stop = src.stop AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_processed', 'pit_stops', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.pit_stops;
