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
	WHERE source = 'WAC'						--ONLY USE PRIVATE JOBS (WAC)
	GROUP BY parcel_id
	--ALL = 42,764p 1,592,288j
	--WAC = 38,744p 1,159,800j
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
WHERE development_type_id NOT IN(				--EXCLUDE PARCELS WITH DEVELOPMENT TYPES:
	1	--Agriculture and Mining
	,14	--Recreation
	,15	--Active Park
	,16	--Government Operations
	,17	--Dump Space
	,24	--Transportation Right of Way
	,25	--Parking Lot
	,26	--Undeveloped Open Space
	,27	--Beach
	,28	--Water
	,29	--Military Reservation
	,30	--Indian Reservation
	,31	--Vacant Developable Land
)
AND development_type_id NOT IN(					--EXCLUDE RESIDENTIAL PARCELS
	19
	,20
	,21
	,22
	,23
	,11
	,32
	,33
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
	--STANDARD SQUARE FOOTAGE PER JOB  = 300ft
APPLY FAR AND VACANCY RATES FOR ALL OTHER PARCELS:
	FAR =		50% =	0.5
	VACANCY =	+10% =	1.1
	--STANDARD SQUARE FOOTAGE PER JOB  = 400ft
*/

/*
CREATE SQUARE FOOTAGE PER EMPLOYEE BY BUSINESS TYPE TABLE 
SOURCE: 'BUILDING AREA PER EMPLOYEE BY BUSINESS TYPE'
PUBLISHED BY: ITE, USDOE, SANDAG
*/
IF OBJECT_ID('tempdb..#sqft_dev_type') IS NOT NULL
    DROP TABLE #sqft_dev_type
;

CREATE TABLE #sqft_dev_type(
	development_type_id int
	,sqft int
);
--SELECT * FROM #sqft_dev_type

INSERT INTO #sqft_dev_type
VALUES
	(2, 463)
	,(3, 549)
	,(4, 228)
	,(5, 588)
	,(6, 2114)
	,(7, 1124)
	,(8, 1250)
	,(9, 1587)
	,(10, 1587)
	,(12, 207)
	,(13, 600)	--NO REFERENCE FOUND
	,(16, 300)	--NO REFERENCE FOUND
	,(18, 600)	--NO REFERENCE FOUND
;

/* CALCULATE */
--PARCELS WITH EXISTING JOBS AND EFFECTIVE AREA
IF OBJECT_ID('tempdb..#job_spaces') IS NOT NULL
    DROP TABLE #job_spaces
;

SELECT parcel_id
	,p.development_type_id
	,jobs
	,effective_sqft
	--,CEILING((CEILING(effective_sqft*0.5/400))*1.1) AS test		--xxTEST
	,(effective_sqft*0.5/400)*1.1 AS test		--xxTEST
	,CASE
		WHEN parcel_id IN (SELECT parcel_id
								FROM #parcels AS p
								JOIN OPENQUERY (sql2014b8, 'SELECT * FROM lis.gis.MAJOREMPLOYMENTAREAS WHERE EMP_TYPE = ''c''') AS mea
									ON p.centroid.STIntersects(mea.shape) = 1)
			THEN (effective_sqft*0.8/sqft)*1.1			--IN EMPLOYMENT CENTERS
		ELSE (effective_sqft*0.5/sqft)*1.1				--NOT IN EMPLOYMENT CENTERS
	END AS job_spaces
INTO #job_spaces
FROM #parcels AS p
JOIN #sqft_dev_type AS sf ON p.development_type_id = sf.development_type_id
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
INTO urbansim.job_spaces_parcel_sqft										--SAVE TO TABLE
FROM #job_spaces
ORDER BY 5
--CHECK
SELECT * FROM urbansim.job_spaces_parcel_sqft ORDER BY job_spaces


--SUMMARIZE
SELECT
	CASE WHEN jobs >= job_spaces THEN 'NO'
		ELSE 'FEASIBLE'
	END AS feasible
	,COUNT(*) AS parcels
	,SUM(jobs) AS jobs
	,SUM(job_spaces) AS job_spaces
	,SUM(job_spaces - jobs) AS job_spaces_vac
--FROM #job_spaces
FROM urbansim.job_spaces_parcel_sqft										--FROM SAVED TABLE
GROUP BY
	CASE WHEN jobs >= job_spaces THEN 'NO'
		ELSE 'FEASIBLE'
	END


--DROP TEMP TABLE
DROP TABLE #job_spaces



--***********************************************************************************
--JOBS CHECK
SELECT COUNT(*)
FROM [spacecore].[urbansim].[jobs] AS j
JOIN urbansim.buildings AS b ON j.building_id = b.building_id
JOIN urbansim.parcels AS p ON b.parcel_id = p.parcel_id
--														--ALL			1,592,288
WHERE source = 'WAC'									--WAC			1,159,800
AND p.development_type_id NOT IN (19, 20, 21, 22, 23)	--NON RES		1,040,778
AND p.development_type_id NOT IN (11, 32, 33)			--NON RES ISH	1,040,769



