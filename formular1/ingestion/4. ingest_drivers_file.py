# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1: Read the Json file using the spark dataframe reader API

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, IntegerType, DateType, StructField

# COMMAND ----------

nameSchema = StructType(fields=[StructField("forename", StringType(), True),StructField("surname", StringType(), True)])
driverSchema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                       StructField("driverRef", StringType(), True),
                                       StructField("number", IntegerType(), True),
                                       StructField("code", StringType(), True),
                                       StructField("name", nameSchema),
                                       StructField("dob", DateType(), True),
                                       StructField("nationality", StringType(), True),
                                       StructField("url", StringType(), True)])

# COMMAND ----------

driver_df = spark.read.schema(driverSchema).json(f"{raw_folder_path}/{v_file_date}/drivers.json")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2: Rename columns and add new columns
# MAGIC       1. driverId renamed to driver_id
# MAGIC       2. driverRef renamed_to driver_ref
# MAGIC       3. ingestion date added
# MAGIC       4. name added with concatination of forename and surename

# COMMAND ----------

from pyspark.sql.functions import col, lit, concat

# COMMAND ----------

drivers_with_columns_df = driver_df.withColumnsRenamed({'driverId': 'driver_id', 'driverRef': 'driver_ref'})\
                                    .withColumn('name', concat(col('name.forename'), lit(' '), col('name.surname')))\
                                    .withColumn('file_date', lit("{v_file_date}"))

drivers_with_columns_df  = add_ingestion_date(drivers_with_columns_df) 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3: Drop unwanted columns
# MAGIC       1. name.forename
# MAGIC       2. name.surename
# MAGIC       3. url

# COMMAND ----------

driver_final_df = drivers_with_columns_df.drop(col('url'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4: Write to output to processed container in parquet format

# COMMAND ----------

dbutils.fs.rm("dbfs:/mnt/formular1dlkd/processed/drivers", recurse=True)

# COMMAND ----------

driver_final_df.write.mode('overwrite').format('delta').saveAsTable('f1_processed.drivers')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_processed.drivers
