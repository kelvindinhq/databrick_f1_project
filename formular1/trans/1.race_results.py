# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

circuits_df = spark.read.format('delta').load(f"{processed_folder_path}/circuits")\
                        .withColumnsRenamed({"name":"circuit_name", "location":"circuit_location"})


# COMMAND ----------

races_df = spark.read.format('delta').load(f"{processed_folder_path}/races")\
                     .withColumnsRenamed({"name":"race_name", "race_timestamp":"race_date"})


# COMMAND ----------

drivers_df = spark.read.format('delta').load(f"{processed_folder_path}/drivers")\
                       .withColumnsRenamed({"name":"driver_name", "number":"driver_number", "nationality":"driver_nationality"})

# COMMAND ----------

constructors_df = spark.read.format('delta').load(f"{processed_folder_path}/constructors")\
                            .withColumnsRenamed({"name":"team"})

# COMMAND ----------

results_df = spark.read.format('delta').load(f"{processed_folder_path}/results")\
    .filter(f"file_date = '{v_file_date}'")\
    .withColumnsRenamed({"time":"race_time","race_id":"result_race_id","file_date":"results_file_date"})

# COMMAND ----------

races_circuits_df = races_df.join(circuits_df, races_df.circuit_Id == circuits_df.circuit_id, how="inner")\
.select(races_df.race_id, races_df.race_year, races_df.race_name, races_df.race_date, circuits_df.circuit_location)

# COMMAND ----------

race_results_df = results_df.join(races_circuits_df, results_df.result_race_id == races_circuits_df.race_id, how="inner")\
                            .join(drivers_df, results_df.driver_id == drivers_df.driver_id, how="inner")\
                            .join(constructors_df, results_df.constructor_id == constructors_df.constructor_id, how="inner")

# COMMAND ----------

final_df = race_results_df.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points","position","race_id","results_file_date").withColumnRenamed("results_file_date", "file_date")

# COMMAND ----------

final_df = add_ingestion_date(final_df)

# COMMAND ----------

display(final_df.filter("race_year == 2020 and race_name = 'Abu Dhabi Grand Prix'").orderBy(final_df.points.desc()))

# COMMAND ----------

# overwrite_partition(final_df, 'f1_presentation', 'race_results', 'race_id')

# COMMAND ----------

merge_condition = "tgt.driver_name = src.driver_name AND tgt.race_id = src.race_id"
merge_delta_data(final_df, 'f1_presentation', 'race_results', presentation_folder_path, merge_condition, 'race_id')
