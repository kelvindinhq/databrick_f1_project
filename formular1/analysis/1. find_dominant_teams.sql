-- Databricks notebook source
SELECT team_name,
COUNT(1) as total_races,
AVG(points_to_go) as avg_points,
SUM(points_to_go) as total_points
FROM f1_presentation.calculated_race_results
group by team_name
HAVING total_races >= 100
order by avg_points desc

-- COMMAND ----------

SELECT team_name,
COUNT(1) as total_races,
AVG(points_to_go) as avg_points,
SUM(points_to_go) as total_points
FROM f1_presentation.calculated_race_results
WHERE race_year between 2011 and 2020
group by team_name
HAVING total_races > 50
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
