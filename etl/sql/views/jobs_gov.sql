/*
SELECT *
FROM urbansim.buildings
WHERE data_source = 'SANDAG Public Facility 2016 Geocoding 042617'
--2,534

SELECT *
FROM gis.buildings_public_facilities
ORDER BY id

SELECT development_type_id, COUNT(*) AS bldgs
FROM gis.buildings_public_facilities AS pf
LEFT JOIN ref.development_type_lu_code AS dev
	ON pf.lu = dev.lu_code
GROUP BY development_type_id
ORDER BY development_type_id


SELECT CAST(SUM(CEILING(emp)) AS int)
FROM gis.buildings_public_facilities


SELECT *
FROM input.vi_jobs_gov_2012_2016
WHERE yr = 2015
ORDER BY id
--214,300
;
*/


--ALLOCATE GOV JOBS BY LOCATION
WITH spaces as (
	SELECT *
	FROM urbansim.buildings
	WHERE development_type_id NOT IN(7, 19, 20, 21, 22, 28)						--DO NOT USE BUILDINGS RESIDENTIAL AND SIMILAR
	AND subparcel_assignment <> 'PLACEHOLDER_MIL'								--DO NOT USE MILITARY PLACEHOLDERS
),
jobs AS (
	SELECT *
	FROM input.vi_jobs_gov_2012_2016
	WHERE yr = 2015
),
jobs_loc AS (
	SELECT id.id, shape
	FROM (	
		SELECT id, MIN(job_id) AS job_id
		FROM input.vi_jobs_gov_2012_2016
		WHERE yr = 2015
		GROUP BY id
		) AS id
	JOIN (
		SELECT id, job_id, shape
		FROM input.vi_jobs_gov_2012_2016
		WHERE yr = 2015
		) AS loc
		ON id.job_id = loc.job_id
), match AS(
	SELECT row_id, id, building_id, dist
		, development_type_id, data_source, subparcel_assignment
	FROM(
		SELECT
			ROW_NUMBER() OVER (PARTITION BY jobs_loc.id ORDER BY jobs_loc.id, jobs_loc.shape.STDistance(spaces.shape)) row_id
			,jobs_loc.id
			,spaces.building_id
			,jobs_loc.shape.STDistance(spaces.shape) AS dist
			, development_type_id, data_source, subparcel_assignment
		FROM jobs_loc
		JOIN spaces
			ON jobs_loc.shape.STBuffer(15000).STIntersects(spaces.shape) = 1	--CHECK IF BUFFERDIST IS SUFFICIENT
		) x
	WHERE row_id = 1
	--ORDER BY dist DESC
)
INSERT INTO urbansim.jobs (job_id, sector_id, building_id, source)
SELECT j.job_id, sector_id, usb.building_id, 'GOV'
FROM urbansim.buildings AS usb
JOIN (
	SELECT m.id, m.building_id, j.job_id, j.sector_id
	FROM match AS m
	RIGHT JOIN (SELECT *  FROM input.vi_jobs_gov_2012_2016 WHERE yr = 2015) AS j
	ON m.id = j.id
	) AS j
	ON usb.building_id = j.building_id
ORDER BY j.id, j.job_id
;

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
)

INSERT INTO urbansim.job_spaces(
	building_id
	,job_spaces
	,sector_id
	,source)
SELECT building_id
	,jobs
	,sector_id
	,source
FROM j
;
