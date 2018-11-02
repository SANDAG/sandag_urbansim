USE spacecore
;

/*#################### ASSIGN JOB_SPACES FROM EDD ####################*/
DECLARE @employment_vacancy float = 0.1;

DROP TABLE IF EXISTS urbansim.job_spaces;
GO

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


/* ##### ASSIGN JOB_SPACES TO BUILDINGS BY SUBPARCEL ##### */

--USE EMPLOYMENT VACANCY RATE
--ADDED EMPLOYMENT VACANCY RATE FOR ALL SECTORS
DECLARE @employment_vacancy float = 0.1;				--RATE

--ADDED EMPLOYMENT VACANCY RATE FOR EXCEPTION SECTORS
DECLARE @employment_vacancy_x float = 0.16;				--RATE
DECLARE @sector_x table(sector_x varchar(50))
INSERT INTO @sector_x VALUES ('8'), ('20')				--SECTORS
;
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


WITH emp2013 AS(
	SELECT
		lc.LCKey AS subparcel_id
		,CASE
			WHEN CAST(emp.sandag_industry_id AS varchar) IN (SELECT * FROM @sector_x)
				THEN SUM(CAST(CEILING(ISNULL(emp_adj, 0)*(1 + @employment_vacancy_x))AS int))
			ELSE
				SUM(CAST(CEILING(ISNULL(emp_adj, 0)*(1 + @employment_vacancy))AS int)) 
		END AS emp
		--,SUM(CAST(CEILING(ISNULL(emp_adj,0)*(1+@employment_vacancy))AS int)) AS emp
		--,SUM(CAST(CEILING(ISNULL(emp_adj,0))AS int)) AS emp
		,emp.sandag_industry_id AS sector_id
	FROM gis.ludu2015 lc
	LEFT JOIN socioec_data.ca_edd.emp_2013_v1 AS emp		--USE EDD EMP2013 GEOCODE VERSION 1
	ON lc.Shape.STContains(emp.shape) = 1
	WHERE emp.emp_adj IS NOT NULL
	AND sandag_industry_id IS NOT NULL
	--AND sandag_industry_id BETWEEN 1 AND 20				--PRIVATE SECTOR
	GROUP BY lc.LCKey, emp.sandag_industry_id
), emp2015 AS (
	SELECT lc.LCKey AS subparcel_id
		,CASE
			WHEN CAST(emp.sandag_industry_id AS varchar) IN (SELECT * FROM @sector_x)
				THEN SUM(CAST(CEILING(ISNULL(emp_adj, 0)*(1 + @employment_vacancy_x))AS int))
			ELSE
				SUM(CAST(CEILING(ISNULL(emp_adj, 0)*(1 + @employment_vacancy))AS int)) 
		END AS emp
		--,SUM(CAST(CEILING(ISNULL(emp_adj,0)*(1+@employment_vacancy))AS int)) AS emp
		--,SUM(CAST(CEILING(ISNULL(emp_adj,0))AS int)) AS emp
		,emp.sandag_industry_id AS sector_id
		,emp.own
	,ROW_NUMBER() OVER (PARTITION BY lc.LCKey ORDER BY emp.own) AS row_own
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
	GROUP BY lc.LCKey, emp.sandag_industry_id, emp.own
), emp13 AS (
	SELECT
		emp2013.subparcel_id
		,emp2013.emp
		,emp2013.sector_id
		,emp2015.own
	FROM emp2013
	LEFT JOIN emp2015
		ON emp2013.subparcel_id = emp2015.subparcel_id
		AND emp2015.row_own = 1
		--AND emp2013.sector_id = emp2015.sector_id
	WHERE emp2015.own NOT IN (1,2,3)						--PRIVATE SECTOR
	AND emp2013.emp IS NOT NULL
	AND emp2013.sector_id BETWEEN 1 AND 20					--PRIVATE SECTOR
), emp15 AS (
	SELECT
		subparcel_id
		,emp
		,sector_id
		,own
	FROM emp2015
		WHERE own = 5										--PRIVATE SECTOR
		AND sector_id BETWEEN 1 AND 20						--PRIVATE SECTOR
		AND emp IS NOT NULL
), emp AS(
	SELECT
		--emp13.subparcel_id AS subparcel_id2013
		--emp15.subparcel_id AS subparcel_id2015
		COALESCE(emp13.subparcel_id, emp15.subparcel_id) AS subparcel_id
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
	FROM emp13
	FULL OUTER JOIN emp15
		ON emp13.subparcel_id = emp15.subparcel_id
	AND emp13.sector_id = emp15.sector_id
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
JOIN emp
	ON usb.subparcel_id = emp.subparcel_id
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


/*
/*################### ARBITRARILY ADD MORE SPACE FOR LEHD  ###########################*/
WITH bldg AS (
	SELECT
	  usb.building_id
	  ,deficit.block_id
	  ,deficit.deficit
	  ,deficit.deficit/ COUNT(*) OVER (PARTITION BY deficit.block_id) +
		 CASE 
		   WHEN ROW_NUMBER() OVER (PARTITION BY deficit.block_id ORDER BY usb.job_spaces desc) <= (deficit.deficit % COUNT(*) OVER (PARTITION BY deficit.block_id)) THEN 1 
		   ELSE 0 
		 END jobs
	FROM 
		urbansim.buildings usb 
		INNER JOIN (SELECT bldg.block_id, jobs, spaces,  CAST(ROUND((jobs.jobs - bldg.spaces) ,0) as INT) deficit, CAST(ROUND((jobs.jobs - bldg.spaces) * 1.3 ,0) as INT) deficit15
					FROM (SELECT block_id, COUNT(*) jobs FROM spacecore.input.jobs_wac_2013 GROUP BY block_id) jobs												--13,695 blocks	--1,377,100 jobs
					JOIN (SELECT block_id, SUM(job_spaces) spaces FROM urbansim.buildings GROUP BY block_id) bldg ON jobs.block_id = bldg.block_id				--30,307 blocks	--1,929,835 spaces
					WHERE jobs.jobs > bldg.spaces
					) deficit																																	--146 blocks	--2,197 deficit*1.5	--1,434 deficit
		ON usb.block_id = deficit.block_id
)
UPDATE usb
  SET usb.job_spaces = ISNULL(usb.job_spaces, 0) + jobs
FROM
  urbansim.buildings usb 
INNER JOIN
  bldg ON usb.building_id = bldg.building_id
WHERE ISNULL(usb.job_spaces, 0) + jobs > 0
;
*/


/*
/***#################### WHERE SQFT IS NULL, DERIVE FROM UNITS, JOB_SPACES ####################***/
SELECT * FROM urbansim.buildings 
WHERE assign_jobs = 1
	AND ISNULL(residential_sqft, 0) + ISNULL(non_residential_sqft, 0) = 0
	AND ISNULL(residential_units, 0) + ISNULL(job_spaces, 0) > 0

--UPDATE
UPDATE usb
SET floorspace_source = 'units_jobs_derived'
	,residential_sqft = residential_units * 400
	,non_residential_sqft = job_spaces * 400
FROM (SELECT * FROM urbansim.buildings WHERE assign_jobs = 1) usb		--DO NOT USE MIL/PF BUILDINGS
WHERE ISNULL(residential_sqft, 0) + ISNULL(non_residential_sqft, 0) = 0
	AND ISNULL(residential_units, 0) + ISNULL(job_spaces, 0) > 0
*/