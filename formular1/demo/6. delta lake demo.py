# Databricks notebook source
# MAGIC %md
# MAGIC ##### Objectives
# MAGIC 1. Write data to delta lake (managed table) 
# MAGIC 2. Write data to delta lake (external table)
# MAGIC 3. Read data from delta lake (`table`)
# MAGIC 4. Read data from delta lake (File)

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP DATABASE IF EXISTS f1_demo CASCADE;
# MAGIC CREATE Database IF NOT EXISTS f1_demo
# MAGIC LOCATION '/mnt/formular1dlkd/demo'

# COMMAND ----------

results_df = spark.read\
.option('inferSchema', True)\
.json("/mnt/formular1dlkd/raw/2021-03-28/results.json")

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").partitionBy('constructorId').saveAsTable("f1_demo.results_partitioned_managed")

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW PARTITIONS f1_demo.results_partitioned_managed;

# COMMAND ----------

results_df.write.format("delta").mode("overwrite").save("/mnt/formular1dlkd/demo/results_external")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.results_external
# MAGIC USING DELTA
# MAGIC LOCATION '/mnt/formular1dlkd/demo/results_external'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from f1_demo.results_external

# COMMAND ----------

results_external_df = spark.read.format('delta').load('/mnt/formular1dlkd/demo/results_external')

# COMMAND ----------

display(results_external_df)

# COMMAND ----------

# MAGIC %md
# MAGIC 1. Update delta table
# MAGIC 2. Delete from delta table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * from f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC UPDATE f1.demo.results_managed
# MAGIC SET points = 11- position
# MAGIC WHERE position <= 10
# MAGIC

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, "/mnt/formular1dlkd/demo/results_managed")
deltaTable.update('position <=10', {"points":"21-position"})

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.results_managed;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.results_managed
# MAGIC WHERE position > 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1.demo.results_managed;

# COMMAND ----------

from delta.tables import DeltaTable
deltaTable = DeltaTable.forPath(spark, "/mnt/formular1dlkd/demo/results_managed")
deltaTable.delete('points = 0')

# COMMAND ----------

drivers_day1_df = spark.read\
                        .option("inferSchema", True)\
                        .json("/mnt/formular1dlkd/raw/2021-03-28/drivers.json")\
                        .filter("driverId <= 10")\
                        .select("driverId", "name.forename", "name.surname", "dob")

# COMMAND ----------

from pyspark.sql.functions import upper

# COMMAND ----------

drivers_day2_df = spark.read\
                        .option("inferSchema", True)\
                        .json("/mnt/formular1dlkd/raw/2021-03-28/drivers.json")\
                        .filter("driverId BETWEEN 6 and 15" )\
                        .select("driverId", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"), "dob")

# COMMAND ----------


drivers_day3_df = spark.read\
.option("inferSchema", True)\
.json("/mnt/formular1dlkd/raw/2021-03-28/drivers.json")\
.filter("driverId BETWEEN 1 and 5 or driverId BETWEEN 16 and 20")\
.select("driverId", upper("name.forename").alias("forename"), upper("name.surname").alias("surname"), "dob")

# COMMAND ----------

drivers_day1_df.createOrReplaceTempView("drivers_day1")
drivers_day2_df.createOrReplaceTempView("drivers_day2")

# COMMAND ----------

display(drivers_day2_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.results_merge (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_merge (
# MAGIC driverId INT,
# MAGIC dob DATE,
# MAGIC forename STRING,
# MAGIC surname STRING,
# MAGIC createdDate DATE,
# MAGIC updatedDate DATE
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day1 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET tgt.dob = upd.dob,
# MAGIC           tgt.forename = upd.forename,
# MAGIC           tgt.surname = upd.surname,
# MAGIC           tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.results_merge (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surename STRING,
# MAGIC   createDate DATE,
# MAGIC   updatedDate DATE
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge;

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO f1_demo.drivers_merge tgt
# MAGIC USING drivers_day2 upd
# MAGIC ON tgt.driverId = upd.driverId
# MAGIC WHEN MATCHED THEN 
# MAGIC UPDATE SET tgt.dob = upd.dob,
# MAGIC           tgt.forename = upd.forename,
# MAGIC           tgt.surname = upd.surname,
# MAGIC           tgt.updatedDate = current_timestamp
# MAGIC WHEN NOT MATCHED 
# MAGIC THEN INSERT (driverId, dob, forename, surname, createdDate) VALUES (driverId, dob, forename, surname, current_timestamp);

# COMMAND ----------

from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

deltaTable = DeltaTable.forPath(spark, "/mnt/formular1dlkd/demo/drivers_merge")

deltaTable.alias("tgt").merge(
    drivers_day3_df.alias("upd"),
    "tgt.driverId = upd.driverId")\
    .whenMatchedUpdate(set = {"dob": "upd.dob", "forename": "upd.forename", "surname": "upd.surname","updatedDate": 'current_timestamp()'})\
    .whenNotMatchedInsert(values ={
        "driverId": "upd.driverId",
        "dob": "upd.dob",
        "forename": "upd.forename",
        "surname": "upd.surname",
        "createdDate": 'current_timestamp()',
    })\
    .execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC ###
# MAGIC 1. History & Versioning
# MAGIC 2. Time Travel
# MAGIC 3. Vaccum
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC HISTORY f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge  version as of 2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM f1_demo.drivers_merge timestamp as of '2024-08-31T16:03:21.000+00:00';

# COMMAND ----------

df = spark.read.format('delta').option("timestampAsOf", "2024-08-31T16:03:21.000+00:00").load("/mnt/formular1dlkd/demo/drivers_merge")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql
# MAGIC SET spark.databricks.delta.retentionDurationCheck.enabled = false
# MAGIC VACCUM f1_demo.drivers_merge RETAIN 0 hours;

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_merge WHERE driverId = 1;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from f1_demo.drivers_merge version as of 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- if need to update do when match then update
# MAGIC merge into f1_demo.drivers_merge tgt
# MAGIC using f1_demo.drivers_merge Version as of 3 src
# MAGIC on (tgt.driverId = src.driverId)
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC Transaction logs

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_txn (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate Date,
# MAGIC   updatedDate Date
# MAGIC )
# MAGIC USING DELTA

# COMMAND ----------

# MAGIC %sql
# MAGIC DESC history f1_demo.drivers_txn

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn
# MAGIC SELECT * FROM f1_demo.drivers_merge
# MAGIC WHERE driverId =1

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM f1_demo.drivers_txn
# MAGIC WHERE driverId =1

# COMMAND ----------

for driverId in range(3,20):
    spark.sql(f"""INSERT INTO f1_demo.drivers_txn 
              SELECT * FROM f1_demo.drivers_merge 
              WHERE driverId = {driverId}""")

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_txn 
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %md
# MAGIC Convert parquet to delta

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS f1_demo.drivers_convert_to_delta (
# MAGIC   driverId INT,
# MAGIC   dob DATE,
# MAGIC   forename STRING,
# MAGIC   surname STRING,
# MAGIC   createdDate Date,
# MAGIC   updatedDate Date
# MAGIC )
# MAGIC USING PARQUET

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO f1_demo.drivers_convert_to_delta
# MAGIC SELECT * FROM f1_demo.drivers_merge

# COMMAND ----------

# MAGIC %sql
# MAGIC CONVERT TO DELTA f1_demo.drivers_convert_to_delta

# COMMAND ----------

df = spark.table("f1_demo.drivers_convert_to_delta")

# COMMAND ----------

df.write.mode("overwrite").format('parquet').save("/mnt/formular1dlkd/demo/drivers_convert_to_delta_new")

# COMMAND ----------

# MAGIC %sql
# MAGIC Convert to delta parquet.`/mnt/formular1dlkd/demo/drivers_convert_to_delta_new`
