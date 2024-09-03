-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Objectivexs
-- MAGIC 1. SQL Spark documentation
-- MAGIC 2. Create Database demo
-- MAGIC 3. Data tab in the UI
-- MAGIC 4. SHOW command
-- MAGIC 5. Describe command
-- MAGIC 6. Find the current database

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS demo;

-- COMMAND ----------

SHOW DATABASES;

-- COMMAND ----------

DESC DATABASE EXTENDED demo;

-- COMMAND ----------

SELECT CURRENT_DATABASE();

-- COMMAND ----------

SHOW TABLES in demo;

-- COMMAND ----------

USE demo;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Objectives
-- MAGIC 1. Create managed table using python
-- MAGIC 2. Create managed table using SQL
-- MAGIC 3. Effect of dropping a managed table
-- MAGIC 4. Describe table

-- COMMAND ----------

-- MAGIC %run "../includes/configuration"

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_result")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format('parquet').saveAsTable('demo.race_results_python')

-- COMMAND ----------

use demo;
SHOW TABLES;

-- COMMAND ----------

DESC EXTENDED race_results_python;

-- COMMAND ----------

SELECT * 
FROM demo.race_results_python
WHERE race_year = 2020

-- COMMAND ----------

CREATE TABLE race_results_sql
AS
SELECT * 
FROM demo.race_results_python
WHERE race_year = 2020;

-- COMMAND ----------

DESCRIBE EXTENDED race_results_sql;

-- COMMAND ----------

SELECT * from race_results_sql

-- COMMAND ----------

DROP TABLE demo.race_results_sql;

-- COMMAND ----------

SHOW TABLES IN DEMO;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Objectives
-- MAGIC 1. Create external table using Python
-- MAGIC 2. Create external table using SQL
-- MAGIC 3. Effect of dropping external table 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC race_results_df.write.format('parquet').option("path", f"{presentation_folder_path}/race_result_ext_python").saveAsTable('demo.race_results_ext_python')

-- COMMAND ----------

DESCRIBE EXTENDED demo.race_results_ext_python;

-- COMMAND ----------

CREATE TABLE demo.race_results_ext_sql 
(
  race_year INT,
  race_name STRING,
  race_date TIMESTAMP,
  circuit_location STRING,
  driver_name STRING,
  driver_number INT,
  driver_nationality STRING,
  team STRING,
  grid INT,
  fastest_lap INT,
  race_time STRING,
  points FLOAT,
  position INT,
  created_date TIMESTAMP
)
USING PARQUET
LOCATION '/mnt/formular1dlkd/presentation/race/results_ext_sql';

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

INSERT into demo.race_results_ext_sql
SELECT * FROM demo.race_results_ext_python;

-- COMMAND ----------

SELECT COUNT(*) FROM race_results_ext_sql

-- COMMAND ----------

SHOW TABLES in demo;

-- COMMAND ----------

DROP TABLE demo.race_results_ext_sql

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### View on tables
-- MAGIC #### Objectives
-- MAGIC 1. Create Temp View
-- MAGIC 2. Create Global Temp View
-- MAGIC 3. Create Permernant Vie

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_race_results
AS SELECT * FROM demo.race_results_python WHERE race_year = 2018

-- COMMAND ----------

SELECT * FROM v_race_results;

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW gv_race_results
AS SELECT * FROM demo.race_results_python WHERE race_year = 2012

-- COMMAND ----------

SELECT * FROM global_temp.gv_race_results;

-- COMMAND ----------

SHOW TABLES IN GLOBAL_TEMP;

-- COMMAND ----------

CREATE OR REPLACE VIEW demo.pv_race_results
AS SELECT * FROM demo.race_results_python WHERE race_year = 2000
