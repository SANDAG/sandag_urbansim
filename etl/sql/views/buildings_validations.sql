/****** Script for SelectTopNRows command from SSMS  ******/
SELECT *
  FROM [spacecore].[urbansim].[buildings]



SELECT COUNT(*)
FROM pecas_sr13.[urbansim].[building]
WHERE non_residential_sqft > 0

SELECT COUNT(*) ressqft
FROM [spacecore].[urbansim].[buildings]
WHERE residential_sqft > 0

SELECT COUNT(*) nonressqft
FROM [spacecore].[urbansim].[buildings]
WHERE non_residential_sqft > 0

SELECT COUNT(*) year_
FROM [spacecore].[urbansim].[buildings]
WHERE year_built > 0

SELECT *
FROM [spacecore].[urbansim].[buildings]
WHERE non_residential_sqft = 99999
OR residential_sqft = 99999

SELECT COUNT(*), development_type_id
FROM [spacecore].[urbansim].[buildings]
WHERE non_residential_sqft = 0
AND residential_sqft = 0
GROUP BY development_type_id
ORDER BY 1 DESC