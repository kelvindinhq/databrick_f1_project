-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW v_dominant_driver as (SELECT driver_name,
COUNT(1) as total_races,
AVG(points_to_go) as avg_points,
SUM(points_to_go) as total_points,
rank() over(order by avg(points_to_go) desc) as driver_rank
FROM f1_presentation.calculated_race_results
group by driver_name
having total_races > 50
order by avg_points desc
)


-- COMMAND ----------

SELECT race_year, driver_name,
COUNT(1) as total_races,
AVG(points_to_go) as avg_points,
SUM(points_to_go) as total_points
FROM f1_presentation.calculated_race_results
WHERE driver_name in (SELECT driver_name from v_dominant_driver where driver_rank <= 10)
group by race_year, driver_name
order by avg_points desc

-- COMMAND ----------

SELECT team_name,
COUNT(1) as total_races,
AVG(points_to_go) as avg_points,
SUM(points_to_go) as total_points
FROM f1_presentation.calculated_race_results
WHERE race_year between 2001 and 2010
group by team_name
HAVING total_races > 50
order by avg_points desc
