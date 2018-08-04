/*
SELECT *
FROM urbansim.buildings
WHERE subparcel_assignment = 'PLACEHOLDER_MIL'
--42

SELECT *
FROM input.jobs_military_2012_2016
WHERE yr = 2016
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
	WHERE yr = 2016
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
SELECT
	building_id
	,sector_id
	,COUNT(*) AS jobs
	,source
FROM urbansim.jobs
WHERE source IN ('MIL')
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