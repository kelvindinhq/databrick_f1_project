# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, TimestampType
from pyspark.sql.functions import col

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False), 
                                     StructField("year", IntegerType(), True), 
                                     StructField("round", IntegerType(), True), 
                                     StructField("circuitId", IntegerType(), True), 
                                     StructField("name", StringType(), True), 
                                     StructField("date", DateType(), True), 
                                     StructField("time", StringType(), True), 
                                     StructField("url", StringType(), True)
                                     ])

# COMMAND ----------

races_df = spark.read\
.option("header",True)\
.schema(races_schema)\
.csv(f'{raw_folder_path}/{v_file_date}/races.csv')

# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step2: Select only required columns
# MAGIC

# COMMAND ----------

races_selected_df = races_df.select(col("raceId"), col("year"),col("round"), col("circuitId"), col("name"), col("date"), col("time"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step3: Rename the columns as required
# MAGIC

# COMMAND ----------

races_rename_df = races_selected_df.withColumnsRenamed({"raceId": "race_id", "circuitId": "circuit_Id","year": "race_year"})

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step4: Add ingestion timestamp

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, to_timestamp, lit, concat

# COMMAND ----------

races_final_df = races_rename_df.withColumn("ingestion_date",current_timestamp())\
                                .withColumn("race_timestamp",to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))\
                                .withColumn("file_date", lit(f"{v_file_date}"))

# COMMAND ----------

display(races_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5: Write data to datalake as parquet

# COMMAND ----------

#dbutils.fs.rm("dbfs:/mnt/formular1dlkd/processed/races", recurse=True)

# COMMAND ----------

races_final_df.write.mode("overwrite").partitionBy('race_year').format('delta').saveAsTable("f1_processed.races")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.races
