# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest circuits.csv file
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using spark dataframe reader

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")


# COMMAND ----------

# MAGIC %run "../includes/configuration"
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import col

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False), 
                                     StructField("circuitRef", StringType(), True), 
                                     StructField("name", StringType(), True), 
                                     StructField("location", StringType(), True), 
                                     StructField("country", StringType(), True), 
                                     StructField("lat", DoubleType(), True), 
                                     StructField("lng", DoubleType(), True), 
                                     StructField("alt", IntegerType(), True), 
                                     StructField("url", StringType(), True)
                                     ])

# COMMAND ----------

circuits_df = spark.read\
.option("header",True)\
.schema(circuits_schema)\
.csv(f'{raw_folder_path}/{v_file_date}/circuits.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step2: Select only required columns
# MAGIC

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"),col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step3: Rename the columns as required
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_rename_df = circuits_selected_df\
                                        .withColumnsRenamed({"circuitId": "circuit_id", "circuitRef": "circuit_Ref", "lat": "latitude", "lng": "longitude", "alt": "altitude"})\
                                        .withColumn("file_date", lit(f"{v_file_date}"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step4: Add ingestion timestamp

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_rename_df)

# COMMAND ----------

display(circuits_final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5: Write data to datalake as parquet

# COMMAND ----------

circuits_final_df.write.mode("overwrite").format('delta').saveAsTable("f1_processed.circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_processed.circuits;
