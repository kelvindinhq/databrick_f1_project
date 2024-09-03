# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest_results.json file
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1: Read the JSON file using the spark dataframe reader

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-28")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[StructField('resultId', IntegerType(), False),
                                    StructField('raceId', IntegerType(), True),
                                    StructField('driverId', IntegerType(), True),
                                    StructField('constructorId', IntegerType(), True),
                                    StructField('number', IntegerType(), True),
                                    StructField('grid', IntegerType(), True),
                                    StructField('position', IntegerType(), True),
                                    StructField('positionText', StringType(), True),
                                    StructField('positionOrder', IntegerType(), True),
                                    StructField('points', FloatType(), True),
                                    StructField('laps', IntegerType(), True),
                                    StructField('time', StringType(), True),
                                    StructField('milliseconds', IntegerType(), True),
                                    StructField('fastestLap', IntegerType(), True),
                                    StructField('rank', IntegerType(), True),
                                    StructField('fastestLapTime', StringType(), True),
                                    StructField('fastestLapSpeed', FloatType(), True),
                                    StructField('statusId', IntegerType(), True)
                                    ])

# COMMAND ----------

results_df = spark.read\
.schema(results_schema)\
.json(f"{raw_folder_path}/{v_file_date}/results.json")

# COMMAND ----------

display(results_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2: Drop column
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

results_drop_df = results_df.drop(col('statusId'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step3: Rename column

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

results_final_df = results_drop_df.withColumnsRenamed({'resultId':'result_id', 'raceId':'race_id', 'driverId':'driver_id', 'constructorId':'constructor_id', 'positionText':'position_text','positionOrder':'position_order','fastestLap':'fastest_lap', 'fastestLapTime':'fastest_lap_time', 'fastestLapSpeed':'fastest_lap_speed'}).withColumn('file_date', lit(f"{v_file_date}"))

results_final_df = add_ingestion_date(results_final_df)

# COMMAND ----------

results_final_df = results_final_df.dropDuplicates(['race_id', 'driver_id'])

# COMMAND ----------

display(results_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4: Write ouput to parquet file

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Method 1

# COMMAND ----------

# for race_id_list in results_final_df.select('race_id').distinct().collect():
#     if(spark._jsparkSession.catalog().tableExists('f1_processed.results')):
#         spark.sql(f"ALTER TABLE f1_processed.results DROP IF EXISTS PARTITION (race_id = {race_id_list.race_id})")

# COMMAND ----------

#dbutils.fs.rm('dbfs:/mnt/formular1dlkd/processed/results', recurse=True)

# COMMAND ----------

# results_final_df.write.mode("append").partitionBy('race_id').format('parquet').saveAsTable('f1_processed.results')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Method2

# COMMAND ----------

# MAGIC %sql
# MAGIC --DROP TABLE IF EXISTS f1_processed.results

# COMMAND ----------

# need the last column in the dataframe to be the partition key
# def re_arrange_partition_column(input_df, partition_column):
#     column_list = []
#     for column_name in input_df.schema.names:
#         if column_name != partition_column:
#             column_list.append(column_name)
#     column_list.append(partition_column)
#     output_df = input_df.select(column_list)
#     return output_df

# COMMAND ----------

# def overWrite_partition(input_df, db_name, table_name, partition_column):
#     output_df = re_arrange_partition_column(input_df, partition_column)
#     # Overwrite the way spark overwrites data in partetiion, instade of satic which overwrites, do append as in dynamic
#     spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
#     if(spark._jsparkSession.catalog().tableExists(f"{db_name}.{table_name}")):
#         output_df.write.mode("overwrite").insertInto(f"{db_name}.{table_name}")
#     else:
#         output_df.write.mode("overwrite").partitionBy(partition_column).format("parquet").saveAsTable(f"{db_name}.{table_name}")

# COMMAND ----------

# overwrite_partition(result_final_df, 'f1_processed', 'results', 'race_id')

# COMMAND ----------

merge_condition = "tgt.result_id = src.result_id AND tgt.race_id = src.race_id"
merge_delta_data(results_final_df, 'f1_processed', 'results', processed_folder_path, merge_condition, 'race_id')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT race_id, count(1) from f1_processed.results group by race_id order by race_id desc
