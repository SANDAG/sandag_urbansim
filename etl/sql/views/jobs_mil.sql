SELECT *
FROM urbansim.buildings
WHERE subparcel_assignment = 'PLACEHOLDER_MIL'
--42

SELECT *
FROM input.jobs_military_2012_2016
WHERE yr = 2015
--124,710


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
INSERT INTO urbansim.jobs (job_id, sector_id, building_id)
SELECT jobs.job_id
	,jobs.sector_id
	,spaces.building_id
FROM spaces
JOIN jobs
ON spaces.mgra_id = jobs.mgra
;