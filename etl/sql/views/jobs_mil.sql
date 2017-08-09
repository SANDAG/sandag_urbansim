/*
SELECT *
FROM urbansim.buildings
WHERE subparcel_assignment = 'PLACEHOLDER_MIL'
--42

SELECT *
FROM input.jobs_military_2012_2016
WHERE yr = 2015
--124,710
;
*/


--ALLOCATE MIL JOBS BY MGRA
WITH spaces as (
	SELECT *
	FROM urbansim.buildings
	WHERE subparcel_assignment = 'PLACEHOLDER_MIL'
),
jobs AS (
	SELECT *
	FROM input.jobs_military_2012_2016
	WHERE yr = 2015
)
INSERT INTO urbansim.jobs (job_id, sector_id, building_id, source)
SELECT jobs.job_id
	,jobs.sector_id
	,spaces.building_id
	,'MIL'
FROM spaces
JOIN jobs
ON spaces.mgra_id = jobs.mgra
;

/*#################### UPDATE JOB SPACES  #################### */
--UPDATE JS WITH MIL
WITH j AS(
SELECT building_id
	,sector_id
	,COUNT(*) AS jobs
	,source
FROM urbansim.jobs
WHERE source IN ('MIL')
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