USE spacecore
;

--CHECK VACANCY RATES
--DEFINE YEAR FOR JOBS
DECLARE @yr int = 2012;
SELECT @yr AS 'year';

WITH job_spaces AS(
    SELECT source, sector_id, SUM(job_spaces) AS job_spaces
    FROM urbansim.job_spaces
    GROUP BY sector_id, source
)
,jobs_wac AS(
    SELECT sector_id, COUNT(*) AS jobs_wac, 'EDD' AS source
    FROM input.jobs_wac_2012_2016_3
    WHERE yr = @yr
    GROUP BY sector_id
)
,jobs_sem AS(
    SELECT (sector_id-100) AS sector_id, COUNT(*) AS jobs_sem, 'EDD' AS source
    FROM input.jobs_selfemployed_2012_2016
    WHERE yr = @yr
    GROUP BY sector_id
)
SELECT
    i.sandag_industry_id AS sector_id
    ,i.sandag_industry_name
    ,job_spaces
    ,job_spaces.source AS js_source
    ,jobs_wac
    ,jobs_sem
    ,(ISNULL(jobs_wac, 0) + ISNULL(jobs_sem, 0)) AS jobs_total
    ,job_spaces - (ISNULL(jobs_wac, 0) + ISNULL(jobs_sem, 0)) AS vacancy
	,CAST((job_spaces - (ISNULL(jobs_wac, 0) + ISNULL(jobs_sem, 0))) AS numeric(12,2)) / ISNULL(job_spaces, 0) AS vacancy_rate
FROM [socioec_data].[ca_edd].[sandag_industry] AS i
LEFT JOIN job_spaces
ON i.sandag_industry_id = job_spaces.sector_id
LEFT JOIN jobs_wac
    ON i.sandag_industry_id = jobs_wac.sector_id
    AND job_spaces.source = jobs_wac.source
LEFT JOIN jobs_sem
    ON i.sandag_industry_id = jobs_sem.sector_id
    AND job_spaces.source = jobs_sem.source
ORDER BY i.sandag_industry_id, js_source


/*#################### SET EMPLOYMENT VACANCY RATES ####################*/
--ADDED EMPLOYMENT VACANCY RATE, SECTOR SPECIFIC
DECLARE @sector_vacancy table(sector_x int, vacancy_x numeric(6,3))
INSERT INTO @sector_vacancy VALUES
--FOR 2016
	 (1, 0)
	,(2, 0)
	,(3, 0.09)
	,(4, 0.35)
	,(5, 0.01)
	,(6, 0)
	,(7, 0.12)
	,(8, 0.41)
	,(9, 0)
	,(10, 0)
	,(11, 0.27)
	,(12, 0.04)
	,(13, 0)
	,(14, 0)
	,(15, 0.03)
	,(16, 0.07)
	,(17, 0.12)
	,(18, .09)
	,(19, 0.11)
	,(20, 0.43)
;

/*
--FOR 2012
	 (1, 0.06)
	,(2, 0.04)
	,(3, 0.62)
	,(4, 0.06)
	,(5, 0.02)
	,(6, 0.02)
	,(7, 0.05)
	,(8, 0.24)
	,(9, 0.02)
	,(10, 0.02)
	,(11, 0.17)
	,(12, 0.02)
	,(13, 0.02)
	,(14, 0.02)
	,(15, 0.02)
	,(16, 0.02)
	,(17, 0.02)
	,(18, 0.02)
	,(19, 0.02)
	,(20, 0.33)
*/
/*
sector_id	sandag_industry_name
1	Farm
2	Mining and Logging
3	Utilities
4	Construction
5	Manufacturing
6	Wholesale Trade
7	Retail Trade
8	Transportation & Warehousing
9	Information
10	Finance & Insurance
11	Real Estate & Rental & Leasing
12	Professional, Scientific & Technical Services
13	Management of Companies & Enterprises
14	Administrative & Support & Waste Services
15	Educational Services
16	Health Care & Social Assistance
17	Arts, Entertainment & Recreation
18	Accommodation
19	Food Services
20	Other Services
21	Federal Government Excluding Department of Defense
22	Department of Defense
23	State Government Education
24	State Government Excluding Education
25	Local Government Education
26	Local Government Excluding Education
27	Uniform Military
28	Public Administration
*/


/*#################### ASSIGN JOB_SPACES FROM EDD ####################*/

/* ##### ASSIGN JOB_SPACES TO BUILDINGS BY SUBPARCEL ##### */
DROP TABLE IF EXISTS #emp2013;
SELECT
	lc.LCKey AS subparcel_id
	,lc.parcelId AS parcel_id
	,SUM(CAST(CEILING(ISNULL(emp_adj,0))AS int)) AS emp
	,emp.sandag_industry_id AS sector_id
INTO #emp2013
FROM gis.ludu2015 lc
LEFT JOIN socioec_data.ca_edd.emp_2013_v1 AS emp		--USE EDD EMP2013 GEOCODE VERSION 1
ON lc.Shape.STContains(emp.shape) = 1
WHERE emp.emp_adj IS NOT NULL
AND sandag_industry_id IS NOT NULL
--AND sandag_industry_id BETWEEN 1 AND 20				--PRIVATE SECTOR
GROUP BY lc.LCKey, lc.parcelId, emp.sandag_industry_id
;

DROP TABLE IF EXISTS #emp2015;
SELECT
	lc.LCKey AS subparcel_id
	,lc.parcelId AS parcel_id
	,SUM(CAST(CEILING(ISNULL(emp_adj,0))AS int)) AS emp
	,emp.sandag_industry_id AS sector_id
	,emp.own
,ROW_NUMBER() OVER (PARTITION BY lc.LCKey ORDER BY emp.own) AS row_own
INTO #emp2015
FROM gis.ludu2015 lc
LEFT JOIN (
	SELECT
		CASE
			WHEN emp1 >= emp2 AND emp1 >= emp3 THEN emp1
			WHEN emp2 >= emp1 AND emp2 >= emp3 THEN emp2
			WHEN emp3 >= emp1 AND emp3 >= emp2 THEN emp3
		END AS emp_adj
		,sandag_industry_id
		,own
		,shape
	FROM (SELECT ISNULL(emp1,0) AS emp1, ISNULL(emp2,0) AS emp2, ISNULL(emp3,0) AS emp3, sandag_industry_id, shape, own
			FROM ws.dbo.CA_EDD_EMP_2015_v1) x			--USE EDD EMP2015 GEOCODE VERSION 1
	WHERE sandag_industry_id IS NOT NULL
	--WHERE own = 5										--PRIVATE SECTOR
	--AND sandag_industry_id BETWEEN 1 AND 20			--PRIVATE SECTOR
	) AS emp
ON lc.Shape.STContains(emp.shape) = 1
WHERE emp.emp_adj IS NOT NULL
GROUP BY lc.LCKey, lc.parcelId, emp.sandag_industry_id, emp.own
;

DROP TABLE IF EXISTS #emp13
SELECT
	emp2013.subparcel_id
	,emp2013.parcel_id
	,emp2013.emp
	,emp2013.sector_id
	,emp2015.own
INTO #emp13
FROM #emp2013 AS emp2013
LEFT JOIN #emp2015 AS emp2015
	ON emp2013.subparcel_id = emp2015.subparcel_id
	AND emp2015.row_own = 1
	--AND emp2013.sector_id = emp2015.sector_id
WHERE emp2015.own NOT IN (1,2,3)						--PRIVATE SECTOR
AND emp2013.emp IS NOT NULL
AND emp2013.sector_id BETWEEN 1 AND 20					--PRIVATE SECTOR
;

DROP TABLE IF EXISTS #emp15
SELECT
	subparcel_id
	,parcel_id
	,emp
	,sector_id
	,own
INTO #emp15
FROM #emp2015 AS emp2015
	WHERE own = 5										--PRIVATE SECTOR
	AND sector_id BETWEEN 1 AND 20						--PRIVATE SECTOR
	AND emp IS NOT NULL
;

DROP TABLE IF EXISTS #emp;
SELECT
	--emp13.subparcel_id AS subparcel_id2013
	--emp15.subparcel_id AS subparcel_id2015
	COALESCE(emp13.subparcel_id, emp15.subparcel_id) AS subparcel_id
	,COALESCE(emp13.parcel_id, emp15.parcel_id) AS parcel_id
	--emp13.emp AS emp2013
	--emp15.emp AS emp2015
	--emp13.sector_id AS sector_id2013
	--emp15.sector_id AS sector_id2015
	,CASE
		WHEN emp15.emp + emp13.emp = 0 THEN 0
		WHEN ISNULL(emp15.emp, 0) >= ISNULL(emp13.emp, 0) THEN ISNULL(emp15.emp, 0)
		ELSE emp13.emp
	END AS emp
	--,emp13.emp AS e13
	--,emp15.emp AS e15
	,CASE
		WHEN (ISNULL(emp15.emp, 0) + ISNULL(emp13.emp, 0) = 0) THEN COALESCE(emp13.sector_id, emp15.sector_id)
		WHEN ISNULL(emp15.emp, 0) >= ISNULL(emp13.emp, 0) THEN emp15.sector_id
		ELSE emp13.sector_id
	END AS sector_id
	--,emp13.sector_id AS s13
	--,emp15.sector_id AS s15
INTO #emp
FROM #emp13 AS emp13
FULL OUTER JOIN #emp15 AS emp15
	ON emp13.subparcel_id = emp15.subparcel_id
AND emp13.sector_id = emp15.sector_id


/* ############################## START MICRO ############################## */

--REMOVE JOBS FROM PARCELS THAT ARE NON-PRIVATE, ACCORDING TO MICRO
DELETE e
--SELECT
--	e.subparcel_id
--	,e.parcel_id
--	,e.emp
FROM #emp AS e
WHERE parcel_id IN (
	SELECT parcel_id
	FROM (SELECT parcel_2015 AS parcel_id
			FROM [spacecore].[input].[edd_micro_2015]
			GROUP BY parcel_2015
			HAVING COUNT(*) = 1) AS c
	WHERE parcel_id IN (
		SELECT parcel_2015
		FROM [spacecore].[input].[edd_micro_2015]
		WHERE [priv_pub] <> 'Private'))
--ORDER BY
--	e.parcel_id
--	,e.subparcel_id
/*
SELECT *
FROM #emp
WHERE subparcel_id IN (
	53607
	,84616
	,345915
	,525594
	,564534
	,565621
	,578244
	,765584
	,837827
	,852380
	,856623
	,861388
)
*/


--IDENTIFY PARCELS WHERE MICRO HAS JOBS GREATER THAN OLD EDD
DROP TABLE IF EXISTS #emp_micro;
WITH emp_p AS (
	SELECT
		parcel_id
		,sector_id
		,SUM(emp) AS emp
	FROM #emp AS e
	GROUP BY
		parcel_id
		,sector_id
), micro_p AS (
	SELECT
		parcel_id
		,sector_id
		,SUM(emp) AS emp
	FROM (
		SELECT
			parcel_2015 AS parcel_id
			,sector_id
			--,emp_original
			--,emp_controlled
			,(SELECT MAX(emps)
				FROM (VALUES
						(emp_original)
						,(emp_controlled)
					) AS VALUE(emps)
			) AS emp
		FROM [spacecore].[input].[edd_micro_2015]
		WHERE [priv_pub] = 'Private'					--USE PRIVATE
	) x
	GROUP BY
		parcel_id
		,sector_id
	--ORDER BY [emp_controlled]
), bldg AS (
	SELECT
		building_id
		,subparcel_id
		,parcel_id
		,ROW_NUMBER() OVER(PARTITION BY parcel_id ORDER BY shape.STArea() DESC) AS rownum
	FROM urbansim.buildings
), micro_add AS (
	SELECT
		e.parcel_id AS parcel_id_e
		,m.parcel_id AS parcel_id_m
		,COALESCE(e.parcel_id, m.parcel_id) AS parcel_id
		,e.sector_id AS sector_id_e
		,m.sector_id AS sector_id_m
		,COALESCE(e.sector_id, m.sector_id) AS sector_id
		,e.emp AS emp_e
		,m.emp AS emp_m
		,COALESCE(e.emp, m.emp) AS emp
		,m.emp - COALESCE(e.emp, 0) AS emp_add			--ADDITIONAL EMP
	FROM emp_p AS e
	FULL JOIN micro_p AS m
		ON e.parcel_id = m.parcel_id
		AND e.sector_id = m.sector_id
	WHERE m.emp > COALESCE(e.emp, 0)
	--ORDER BY COALESCE(e.parcel_id, m.parcel_id)
)
SELECT
	--m.*
	m.parcel_id
	,sector_id
	,emp_e
	,emp_m
	,emp_add
	,b.building_id
	,b.subparcel_id
INTO #emp_micro
FROM micro_add AS m
LEFT JOIN bldg AS b 
	ON m.parcel_id = b.parcel_id
	AND b.rownum = 1
;

--SELECT * FROM #emp_micro ORDER BY parcel_id
--SELECT * FROM #emp_micro WHERE building_id IS NULL ORDER BY parcel_id

--FOR PARCELS WITH MICRO JOBS AND NO BUILDING, GRAB SUBPARCEL FROM PARCEL CENTROID
UPDATE m
SET subparcel_id = lc.LCKey
FROM #emp_micro AS m
	JOIN urbansim.parcels AS usp
		ON usp.parcel_id = m.parcel_id
		AND m.building_id IS NULL
	JOIN gis.ludu2015 AS lc
		ON usp.centroid.STIntersects(lc.Shape) = 1

--INSERT PLACEHOLDER CENTROIDS INTO BUILDINGS
INSERT INTO urbansim.buildings (building_id, development_type_id, subparcel_id, parcel_id, mgra_id, shape, centroid, data_source, subparcel_assignment)
SELECT 
	m.subparcel_id + 3000000 AS building_id		--INSERTED BUILDING_ID > 3,000,000
	,usp.development_type_id_2015
	,m.subparcel_id
	,m.parcel_id
	,mgra_id
	,usp.centroid.STBuffer(1)
	,usp.centroid
	,'PLACEHOLDER' AS data_source
	,'PLACEHOLDER_MICRO' AS subparcel_assignment
FROM #emp_micro AS m
LEFT JOIN urbansim.parcels AS usp
	ON usp.parcel_id = m.parcel_id
WHERE m.building_id IS NULL
;
/*
DELETE
--SELECT *
FROM urbansim.buildings
WHERE subparcel_assignment = 'PLACEHOLDER_MICRO'
*/

--CASES FOUND IN #MICRO, INSERT TO #EMP
SELECT *
FROM #emp_micro
WHERE parcel_id NOT IN(SELECT DISTINCT parcel_id FROM #emp)

INSERT INTO #emp
SELECT
	subparcel_id
	,parcel_id
	,0
	,sector_id
FROM #emp_micro
WHERE parcel_id NOT IN(SELECT DISTINCT parcel_id FROM #emp)


--OTHER SECTORS FROM #MICRO, INSERT TO EXISTING #EMP
SELECT * FROM #emp ORDER BY emp;
SELECT * FROM #emp_micro ORDER BY parcel_id, sector_id;

SELECT parcel_id, sector_id FROM #emp_micro GROUP BY parcel_id, sector_id;
--SELECT subparcel_id, sector_id, COUNT(*) FROM #emp_micro GROUP BY subparcel_id, sector_id HAVING COUNT(*) > 1;
--SELECT * FROM #emp_micro WHERE subparcel_id = 852672;
--SELECT * FROM [spacecore].[input].[edd_micro_2015] WHERE parcel_2015 = 5067843;

WITH emp_s AS (
	SELECT--one bldg per parcel/sector
		parcel_id
		,subparcel_id
		,sector_id
		,emp
		,ROW_NUMBER() OVER(PARTITION BY parcel_id, sector_id ORDER BY emp DESC, subparcel_id) AS rownum
	FROM #emp
	--ORDER BY
	--	parcel_id
	--	,sector_id
	--	,subparcel_id
)
INSERT INTO #emp
SELECT
	m.subparcel_id
	,m.parcel_id
	,0
	,m.sector_id
FROM (SELECT * FROM emp_s WHERE rownum = 1) AS e
RIGHT JOIN #emp_micro AS m
	ON m.parcel_id = e.parcel_id
	AND m.sector_id = e.sector_id 
WHERE e.parcel_id IS NULL
;

--ADD JOBS FROM MICRO
WITH emp_s AS (
	SELECT--one bldg per parcel/sector
		parcel_id
		,subparcel_id
		,sector_id
		,emp
		,ROW_NUMBER() OVER(PARTITION BY parcel_id, sector_id ORDER BY emp DESC, subparcel_id) AS rownum
	FROM #emp
	--ORDER BY
	--	parcel_id
	--	,sector_id
	--	,subparcel_id
)
--SELECT
--	e.subparcel_id
--	,e.parcel_id
--	,e.emp
--	,e.emp + m.emp_add
--	,emp_e
--	,emp_m
--	,e.sector_id
UPDATE e
SET
	e.emp = e.emp + m.emp_add
FROM (SELECT * FROM emp_s WHERE rownum = 1) AS e
RIGHT JOIN #emp_micro AS m
	ON m.parcel_id = e.parcel_id
	AND m.sector_id = e.sector_id 
;

/* ############################## MICRO FINISHED ############################## */



/* ############################## SAVE TO TABLE ############################## */
DROP TABLE IF EXISTS urbansim.job_spaces;
CREATE TABLE urbansim.job_spaces(
	job_space_id int IDENTITY (1,1) NOT NULL PRIMARY KEY
	,subparcel_id int NULL
	,building_id bigint NOT NULL
	,block_id bigint NULL
	--,development_type_id smallint NULL
	,job_spaces int NULL
	,sector_id smallint NULL
	,source varchar(3) NOT NULL
	,INDEX urbansim_job_spaces_job_space_id (job_space_id)
);


WITH emp_r AS(
	SELECT
		subparcel_id
		,parcel_id
		,CAST(CEILING(ISNULL(emp, 0)*(1 + vacancy_x )) AS int) AS emp
		--,emp
		,sector_id
	FROM #emp AS e
	JOIN @sector_vacancy AS s
	ON s.sector_x = e.sector_id
)
INSERT INTO urbansim.job_spaces(
	subparcel_id
	,building_id
	,block_id
	--,development_type_id
	,job_spaces
	,sector_id
	,source)
SELECT
	usb.subparcel_id
	,usb.building_id
	,usb.block_id
	--,usb.development_type_id
	--,emp.emp AS emp_total
	--,COUNT(*) OVER (PARTITION BY usb.subparcel_id, emp.sector_id) AS bldgs--
	,emp.emp/ COUNT(*) OVER (PARTITION BY usb.subparcel_id, emp.sector_id) +
	CASE 
		WHEN ROW_NUMBER() OVER (PARTITION BY usb.subparcel_id, emp.sector_id ORDER BY usb.shape.STArea() DESC) <= (emp.emp % COUNT(*) OVER (PARTITION BY usb.subparcel_id, emp.sector_id)) THEN 1 
		ELSE 0 
	END AS job_spaces
	,emp.sector_id
	,'EDD' AS source
FROM
	(SELECT
		usb.subparcel_id
		,usb.building_id
		,usb.block_id
		,usb.shape
		--,usp.development_type_id_2015
	FROM urbansim.buildings AS usb
	JOIN urbansim.parcels AS usp
		ON usp.parcel_id = usb.parcel_id
	 WHERE assign_jobs = 1) usb
JOIN emp_r AS emp
	ON usb.subparcel_id = emp.subparcel_id
;

--REMOVE JOB SPACES FROM RESIDENTIAL BUILDINGS THAT HAVE NON COMPATIBLE SECTORS
DELETE js
--SELECT *
FROM urbansim.job_spaces AS js
JOIN (SELECT
			building_id
			,development_type_id 
		FROM urbansim.buildings
		WHERE development_type_id IN (19, 20, 21, 22) 		--RESIDENTIAL
) AS usb
		ON js.building_id = usb.building_id
WHERE sector_id IN(											--RESIDENTIAL BUILDING NOT COMPATIBLE
		4
		,5
		,6
		,7
		,8
		,15
		,19
		,20)
--ORDER BY js.building_id
;

/*
THESE JOB SPACES ARE USED BY 'WAC' (PRIVATE) AND 'SEM' (SELFEMPLOYED)

JOB SPACES FOR 'GOV' AND 'MIL' ARE ADDED TO BUILDINGS LATER, IN THEIR ALLOCATION SCRIPTS:

'GOV' JOBS ARE ALOCATED, IN JOBS_GOV SCRIPT
SECTORS 21, 23, 24, 25, 26
SOURCE = GOV 

'MIL' JOBS ARE ALOCATED, IN JOBS_MIL SCRIPT
SECTORS 22, 27
SOURCE = MIL 
*/

