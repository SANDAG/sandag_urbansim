USE spacecore
;

/*** LOAD TEMP TABLES FOR PROCESSING: BUILDINGS AND JOBS ***/
--BUILDINGS
DROP TABLE IF EXISTS #buildings
;
SELECT
	parcel_id
	,building_id
	--,development_type_id
	,ROW_NUMBER() OVER (PARTITION BY parcel_id ORDER BY shape.STArea() DESC) AS rownum
	,data_source
INTO #buildings
FROM urbansim.buildings
WHERE data_source = 'SANDAG Public Facility 2016 Geocoding 042617'
	OR development_type_id IN(8, 9, 10, 16)									--PUBLIC FACILITIES DEV TYPE
	AND subparcel_assignment <> 'PLACEHOLDER_MIL'							--DO NOT USE MILITARY PLACEHOLDERS
ORDER BY parcel_id, rownum
;
SELECT * FROM #buildings
;


--JOBS
DROP TABLE IF EXISTS #jobs
;
SELECT
	yr
	,id
	,job_id
	,sector_id
	,parcel_2015 AS parcel_id
INTO #jobs
FROM spacecore.input.jobs_gov_2012_2016_3
WHERE yr = 2015
;
SELECT * FROM #jobs ORDER BY parcel_id
;

SELECT DISTINCT parcel_id
FROM #jobs AS j
WHERE NOT EXISTS
	(SELECT *
	FROM #buildings AS b
	WHERE j.parcel_id = b.parcel_id)

SELECT *
FROM #buildings
WHERE parcel_id = 395
ORDER BY parcel_id


/*** INSERT PLACEHOLDER FOR GOV EMP AT PARCEL LEVEL ***/
--INSERT BUILDING IN PARCEL WHERE GOV EMP AND CURRENTLY NO BUILDING
INSERT INTO urbansim.buildings WITH (TABLOCK) (
	building_id
	,parcel_id
	--,development_type_id
	,mgra_id
	,block_id
	,shape
	,centroid
	,data_source
	,assign_jobs
	)
SELECT
	9000000 + j.parcel_id				--building_id
	,j.parcel_id						--parcel_id
	--,usp.development_type_id_2015 AS development_type_id			--development_type_id
	,mgra_id							--mgra_id
	,block_id							--block_id
	,usp.centroid.STBuffer(1)			--shape
	,usp.centroid						--centroid
	,'PLACEHOLDER_GOV'					--data_source
	,0									--assign_jobs
FROM (SELECT DISTINCT parcel_id FROM #jobs) AS j
JOIN urbansim.parcels AS usp ON j.parcel_id = usp.parcel_id
WHERE NOT EXISTS 									--CURRENTLY NO BUILDING
	(SELECT *
	FROM #buildings AS b
	WHERE j.parcel_id = b.parcel_id)
;


/*** ALLOCATE GOV JOBS TO BUILDINGS ***/
--1/3 ALLOCATE GOV JOBS IN PUBLIC FACILITIES BUILDINGS
WITH spaces as (
	SELECT *
		,ROW_NUMBER() OVER(PARTITION BY  parcel_id ORDER BY rownum) AS rownum_pf
	FROM #buildings
	WHERE data_source = 'SANDAG Public Facility 2016 Geocoding 042617'
	--ORDER BY parcel_id
)
INSERT INTO urbansim.jobs (job_id, sector_id, building_id, source)
SELECT j.job_id
	,j.sector_id
	,s.building_id
	,'GOV'
FROM spaces AS s
JOIN #jobs AS j
	ON s.parcel_id = j.parcel_id
WHERE s.rownum_pf = 1
ORDER BY job_id
;


--2/3 ALLOCATE GOV JOBS IN COMPATIBLE DEV TYPE BUILDINGS
WITH spaces as (
	SELECT *
		,ROW_NUMBER() OVER(PARTITION BY  parcel_id ORDER BY rownum) AS rownum_dev
	FROM #buildings
	WHERE data_source <> 'SANDAG Public Facility 2016 Geocoding 042617'
	--ORDER BY parcel_id
)
INSERT INTO urbansim.jobs (job_id, sector_id, building_id, source)
SELECT j.job_id
	,j.sector_id
	,s.building_id
	,'GOV'
FROM spaces AS s
JOIN #jobs AS j
	ON s.parcel_id = j.parcel_id
WHERE s.rownum_dev = 1
AND NOT EXISTS
	(SELECT *
	FROM urbansim.jobs AS usj
	WHERE j.job_id = usj.job_id)
ORDER BY job_id
;

--3/3 ALLOCATE GOV JOBS IN PLACEHOLDER BUILDINGS
WITH spaces as (
	SELECT *
	FROM urbansim.buildings
	WHERE data_source = 'PLACEHOLDER_GOV'
	--ORDER BY parcel_id
)
INSERT INTO urbansim.jobs (job_id, sector_id, building_id, source)
SELECT j.job_id
	,j.sector_id
	,s.building_id
	,'GOV'
FROM spaces AS s
JOIN #jobs AS j
	ON s.parcel_id = j.parcel_id
WHERE NOT EXISTS
	(SELECT *
	FROM urbansim.jobs AS usj
	WHERE j.job_id = usj.job_id)
ORDER BY job_id
;
DROP TABLE #buildings;
DROP TABLE #JOBS;


/*#################### UPDATE JOB SPACES  #################### */
--UPDATE JS WITH GOV
WITH j AS(
SELECT building_id
	,sector_id
	,COUNT(*) AS jobs
	,source
FROM urbansim.jobs
WHERE source IN ('GOV')
GROUP BY building_id
	,sector_id
	,source
), d AS(
	SELECT
		usb.building_id
		,usb.block_id
		--,usp.development_type_id_2015 AS development_type_id
	FROM urbansim.buildings AS usb
	JOIN urbansim.parcels AS usp
		ON usp.parcel_id = usb.parcel_id
)

INSERT INTO urbansim.job_spaces(
	building_id
	,block_id
	--,development_type_id
	,job_spaces
	,sector_id
	,source)
SELECT
	j.building_id
	,d.block_id
	--,d.development_type_id
	,j.jobs
	,j.sector_id
	,j.source
FROM j
JOIN d ON d.building_id = j.building_id
;