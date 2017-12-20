/****** CREATE NEW NON RES BUILDINGS IN PARCELS WITH EXISTING JOBS ******/
USE spacecore
;
--PARCELS WITH EXISTING JOBS AND EFFECTIVE AREA
IF OBJECT_ID('tempdb..#parcels') IS NOT NULL
    DROP TABLE #parcels
;

WITH j AS(
	SELECT parcel_id
		,COUNT(*) AS jobs
	FROM urbansim.jobs AS j
	JOIN urbansim.buildings AS b ON b.building_id = j.building_id
	GROUP BY parcel_id
)
SELECT p.parcel_id
	,development_type_id
	,jobs
	,((parcel_acres - COALESCE((parcel_acres * proportion_undevelopable), 0)) * 43560) AS effective_sqft
	,centroid
	--,parcel_acres					--xxCHECK
	--,proportion_undevelopable		--xxCHECK
INTO #parcels									--INSERT INTO TEMP TABLE
FROM [spacecore].[urbansim].[parcels] AS p
JOIN j ON p.parcel_id = j.parcel_id
WHERE development_type_id NOT IN(				--EXCLUDE DEVELOPMENT TYPES (836 parcels, 151,677 jobs)
	1	--Agriculture and Mining
	,14	--Recreation
	,15	--Active Park
	,17	--Dump Space
	,26	--Undeveloped Open Space
	,27	--Beach
	,28	--Water
	,29	--Military Reservation
	,31	--Vacant Developable Land
)
AND COALESCE(proportion_undevelopable, 0) <> 1	--EXCLUDE NON DEVELOPABLE PARCELS (208)		--REVIEW REMAINING SMALL PARCELS
--AND proportion_undevelopable > 0	--xxCHECK
ORDER BY development_type_id
;
--SELECT * FROM #parcels ORDER BY effective_sqft


/*########## CALCULATE NEW JOB_SPACES ##########*/
/*
APPLY FAR AND VACANCY RATES FOR PARCELS IN EMPLOYMENT CENTERS:
	FAR =		80% =	0.8
	VACANCY =	+10% =	1.1
	STANDARD SQUARE FOOTAGE PER JOB  = 300ft
APPLY FAR AND VACANCY RATES FOR ALL OTHER PARCELS:
	FAR =		50% =	0.5
	VACANCY =	+10% =	1.1
	STANDARD SQUARE FOOTAGE PER JOB  = 400ft
*/
--PARCELS WITH EXISTING JOBS AND EFFECTIVE AREA
IF OBJECT_ID('tempdb..#job_spaces') IS NOT NULL
    DROP TABLE #job_spaces
;

SELECT parcel_id
	,development_type_id
	,jobs
	,effective_sqft
	--,CEILING((CEILING(effective_sqft*0.5/400))*1.1) AS test		--xxTEST
	,(effective_sqft*0.5/400)*1.1 AS test		--xxTEST
	,CASE
		WHEN parcel_id IN (SELECT parcel_id
								FROM #parcels AS p
								JOIN OPENQUERY (sql2014b8, 'SELECT * FROM lis.gis.MAJOREMPLOYMENTAREAS WHERE EMP_TYPE = ''c''') AS mea
									ON p.centroid.STIntersects(mea.shape) = 1)
			THEN (effective_sqft*0.8/300)*1.1			--IN EMPLOYMENT CENTERS
		ELSE (effective_sqft*0.5/400)*1.1				--NOT IN EMPLOYMENT CENTERS
	END AS job_spaces
INTO #job_spaces
FROM #parcels
WHERE effective_sqft > 300						--FILTER FOR VERY SMALL PARCELS: EFFECTIVE SQFT, MAY WANT TO ADD MORE VARIABLES
--CHECK
--SELECT * FROM #job_spaces ORDER BY job_spaces

--DROP PREVIOUS TABLE
DROP TABLE #parcels


/*########## LOOK AT NEW JOB_SPACES ##########*/
--CREATE AND LOAD NEW TABLE
IF OBJECT_ID('urbansim.job_spaces_parcel_sqft') IS NOT NULL
    DROP TABLE urbansim.job_spaces_parcel_sqft
;
SELECT parcel_id
	,development_type_id
	,jobs
	,test					--xxTEST
	,IIF(job_spaces < 10000, job_spaces, 10000) AS job_spaces				--CONTROL TO 10,000 MAX JOB SPACES(AS REFERENCE CURRENT MAX IN EXISTING BUILDINGS)
	,IIF(job_spaces < 10000, job_spaces, 10000) - jobs AS job_spaces_vac	--CONTROL TO 10,000 MAX JOB SPACES(AS REFERENCE CURRENT MAX IN EXISTING BUILDINGS)
	,CASE WHEN jobs >= job_spaces THEN 'NO'
		ELSE 'FEASIBLE'
	END AS feasible
INTO urbansim.job_spaces_parcel_sqft		--CREATE TABLE
FROM #job_spaces
ORDER BY 5
--CHECK
SELECT *  FROM urbansim.job_spaces_parcel_sqft ORDER BY job_spaces


--SUMMARIZE
SELECT
	CASE WHEN jobs >= job_spaces THEN 'NO'
		ELSE 'FEASIBLE'
	END AS feasible
	,COUNT(*) AS parcels
	,SUM(jobs) AS jobs
	,SUM(job_spaces) AS job_spaces
	,SUM(job_spaces - jobs) AS job_spaces_vac
FROM urbansim.job_spaces_parcel_sqft
GROUP BY
	CASE WHEN jobs >= job_spaces THEN 'NO'
		ELSE 'FEASIBLE'
	END


--DROP TEMP TABLE
DROP TABLE #job_spaces

