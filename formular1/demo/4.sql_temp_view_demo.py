# Databricks notebook source
# MAGIC %md
# MAGIC ### Access dataframe using SQL
# MAGIC ##### Objectives
# MAGIC 1. create temporary views on dataframe
# MAGIC 2. access the view from sql cell
# MAGIC 3. access the view from Python cell

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

race_results_df = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

race_results_df.createOrReplaceTempView("v_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * 
# MAGIC from v_race_results 
# MAGIC where race_year = 2020

# COMMAND ----------

p_race_year = 2019

# COMMAND ----------

race_results_2019_df = spark.sql(f"SELECT * FROM v_race_results WHERE race_year={p_race_year}")

# COMMAND ----------

display(race_results_2019_df)

# COMMAND ----------

race_results_df.createOrReplaceGlobalTempView("gv_race_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC /*SHOW TABLE*/
# MAGIC SHOW TABLES IN global_temp;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM global_temp.gv_race_results;

# COMMAND ----------

spark.sql("SELECT * FROM global_temp.gv_race_results").show()
