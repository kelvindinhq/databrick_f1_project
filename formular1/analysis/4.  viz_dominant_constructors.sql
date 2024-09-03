-- Databricks notebook source
CREATE OR REPLACE TEMP VIEW v_dominant_team as (SELECT team_name,
COUNT(1) as total_races,
AVG(points_to_go) as avg_points,
SUM(points_to_go) as total_points,
rank() over(order by avg(points_to_go) desc) as team_rank
FROM f1_presentation.calculated_race_results
group by team_name
having total_races >= 100
order by avg_points desc
)


-- COMMAND ----------

SELECT race_year, team_name,
COUNT(1) as total_races,
AVG(points_to_go) as avg_points,
SUM(points_to_go) as total_points
FROM f1_presentation.calculated_race_results
WHERE team_name in (SELECT team_name from v_dominant_team where team_rank <= 5)
group by race_year, team_name
order by avg_points desc
