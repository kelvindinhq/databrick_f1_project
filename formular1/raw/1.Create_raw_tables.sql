-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS f1_raw;

-- COMMAND ----------

-- MAGIC %md Create circuits tables

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.circuits;
CREATE TABLE IF NOT EXISTS f1_raw.circuits (circuitId INT, 
circuitRef STRING,
name STRING,
location STRING,
country STRING,
lat DOUBLE,
lng DOUBLE,
alt INT,
url STRING
)
USING csv
OPTIONS (path "/mnt/formular1dlkd/raw/circuits.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.circuits;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create race table

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.races;
CREATE TABLE IF NOT EXISTS f1_raw.races (raceId INT,
year INT,
round INT,
circuitId INT,
name STRING,
date DATE,
time STRING,
url STRING
)
USING csv
OPTIONS (path "/mnt/formular1dlkd/raw/races.csv", header true)

-- COMMAND ----------

SELECT * FROM f1_raw.races;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create constructors table
-- MAGIC - Single Line JSON
-- MAGIC - simple structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.constructors;
CREATE TABLE IF NOT EXISTS f1_raw.constructors (
constructorId INT,
constructorRef STRING,
name STRING,
nationality STRING,
url STRING
)
USING JSON
OPTIONS(path "/mnt/formular1dlkd/raw/constructors.json")

-- COMMAND ----------

SELECT * FROM f1_raw.constructors;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create drivers table
-- MAGIC - Single Line JSON
-- MAGIC - complex structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.drivers;
CREATE TABLE IF NOT EXISTS f1_raw.drivers (
driverId INT, 
driverRef STRING, 
number INT, 
code STRING,
name STRUCT<forename: String, surname: String>,
dob DATE,
nationality STRING,
url STRING)
USING json
OPTIONS(path "/mnt/formular1dlkd/raw/drivers.json")

-- COMMAND ----------

SELECT * FROM f1_raw.drivers;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create results table
-- MAGIC - Single Line JSON
-- MAGIC - simple structure
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.results;
CREATE TABLE IF NOT EXISTS f1_raw.results (
resultId INT,
raceId INT,
driverId INT,
constructorId INT,
number INT,
grid INT,
position INT,
positionText STRING,
positionOrder INT,
points INT,
laps INT,
time STRING,
milliseconds INT,
fastestLap INT,
rank INT,
fastestLapTime STRING,
fastestLapSpeed STRING,
statusId STRING
)
USING JSON
OPTIONS(path "/mnt/formular1dlkd/raw/results.json")


-- COMMAND ----------

SELECT * FROM f1_raw.results;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create pit stops table:
-- MAGIC - Multiple line json
-- MAGIC - Simple Structure

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.pit_stops;
CREATE TABLE IF NOT EXISTS f1_raw.pit_stops (
driverId INT,
duration String,
lap INT,
milliseconds INT,
raceId INT,
stop INT,
time String
)
USING JSON
OPTIONS(path "/mnt/formular1dlkd/raw/pit_stops.json", multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.pit_stops;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create table from multiple csv
-- MAGIC

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.lap_time;
CREATE TABLE IF NOT EXISTS f1_raw.lap_time (
  raceId INT,
  driverId INT,
  lap INT,
  milliseconds INT
)
USING CSV
OPTIONS (path "/mnt/formular1dlkd/raw/lap_times")

-- COMMAND ----------

SELECT * FROM f1_raw.lap_time;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create qualifying Table
-- MAGIC - Json
-- MAGIC - Multiple file
-- MAGIC - Multiple line

-- COMMAND ----------

DROP TABLE IF EXISTS f1_raw.qualifying;
CREATE TABLE IF NOT EXISTS f1_raw.qualifying (
  qualifyId INT,
  raceId INT,
  driverId INT,
  constructorId INT,
  position INT,
  q1 INT,
  q2 INT,
  q3 INT
)
USING JSON
OPTIONS (path "/mnt/formular1dlkd/raw/qualifying", multiLine true)

-- COMMAND ----------

SELECT * FROM f1_raw.qualifying;
