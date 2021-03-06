USE spacecore
;

--DEFINE YEAR FOR JOBS
DECLARE @yr int = 2016
;
SELECT @yr AS 'year'
;

/*
SELECT [yr]
	,[sector_id]
	,[job_id]
FROM [spacecore].[input].[jobs_selfemployed_2012_2016]
WHERE yr = @yr

SELECT sector_id, COUNT(*)
FROM [spacecore].[input].[jobs_selfemployed_2012_2016]
WHERE yr = @yr
GROUP BY sector_id
ORDER BY sector_id


--LOOK AT AVAILABLE JOB SPACES
WITH job_spaces_used AS(
	SELECT building_id, COUNT(*) AS jobs
	FROM urbansim.jobs
	GROUP BY building_id
)
, job_spaces_available AS(
	SELECT usb.building_id
	,usb.development_type_id
		,usb.job_spaces
		--,usb.job_spaces - ISNULL(j.jobs, 0) AS job_spaces_available
		,CASE
			WHEN j.jobs > usb.job_spaces THEN 0
			ELSE usb.job_spaces - ISNULL(j.jobs, 0)
		END AS job_spaces_available
	FROM urbansim.buildings AS usb
		LEFT JOIN job_spaces_used AS j ON usb.building_id = j.building_id
	WHERE usb.job_spaces IS NOT NULL
)
SELECT building_id
	,development_type_id
	,job_spaces_available
FROM job_spaces_available AS a
WHERE a.job_spaces_available > 0

***********************************************************************************
*/


/*##### ALLOCATE SELFEMPLOYED JOBS #####*/
-- RUN 1/2 - ALLOCATE SELFEMPLOYED JOBS TO ANY BUILDINGS
WITH jobs AS(
	SELECT job_id
		,(sector_id - 100)  AS sector_id
		,ROW_NUMBER() OVER(PARTITION BY sector_id ORDER BY NEWID()) AS random
	FROM spacecore.input.jobs_selfemployed_2012_2016
	WHERE yr = @yr
	AND sector_id IN(										--RESIDENTIAL BUILDING COMPATIBLE
		109
		,110
		,111
		,112
		,114
		,116
		,117)
	--ORDER BY sector_id, random--
)
, job_spaces_available AS(
	SELECT
		js.building_id
		,js.sector_id
		,js.job_spaces - COALESCE(jsu.job_spaces_used, 0) AS job_spaces_available
		,usb.development_type_id
		,usb.shape
	FROM urbansim.job_spaces AS js
	LEFT JOIN(SELECT building_id, sector_id
					,COUNT(building_id) AS job_spaces_used
				FROM urbansim.jobs
				--WHERE sector_id = @sector_id
				GROUP BY building_id, sector_id) AS jsu
		ON js.building_id = jsu.building_id
		AND js.sector_id = jsu.sector_id
	JOIN urbansim.buildings AS usb ON js.building_id = usb.building_id
	WHERE js.job_spaces - COALESCE(jsu.job_spaces_used, 0) > 0
)
, job_spaces AS(
	SELECT building_id
		,development_type_id
		,sector_id
		,job_spaces_available
		,ROW_NUMBER() OVER(PARTITION BY sector_id ORDER BY NEWID()) AS random
	FROM job_spaces_available AS a
		,ref.numbers AS n
	WHERE n.numbers <= a.job_spaces_available
)
INSERT INTO urbansim.jobs (job_id, sector_id, building_id, source)
SELECT job_id
	,j.sector_id
	,building_id
	,'SEM'
FROM job_spaces AS s
JOIN jobs AS j
	ON j.random = s.random
	AND j.sector_id = s.sector_id
ORDER BY building_id
;

-- RUN 2/2 - ALLOCATE SELFEMPLOYED JOBS TO NON RESIDENTAL BUILDINGS
WITH jobs AS(
	SELECT job_id
		,(sector_id - 100)  AS sector_id
		,ROW_NUMBER() OVER(PARTITION BY sector_id ORDER BY NEWID()) AS random
	FROM spacecore.input.jobs_selfemployed_2012_2016
	WHERE yr = @yr
	AND sector_id IN(										--RESIDENTIAL BUILDING NOT COMPATIBLE
		104
		,105
		,106
		,107
		,108
		,115
		,119
		,120)
	--ORDER BY sector_id, random--
)
, job_spaces_available AS(
	SELECT
		js.building_id
		,js.sector_id
		,js.job_spaces - COALESCE(jsu.job_spaces_used, 0) AS job_spaces_available
		,usb.development_type_id
		,usb.shape
	FROM urbansim.job_spaces AS js
	LEFT JOIN(SELECT building_id, sector_id
					,COUNT(building_id) AS job_spaces_used
				FROM urbansim.jobs
				--WHERE sector_id = @sector_id
				GROUP BY building_id, sector_id) AS jsu
		ON js.building_id = jsu.building_id
		AND js.sector_id = jsu.sector_id
	JOIN (SELECT * 
			FROM urbansim.buildings
			WHERE development_type_id <> 19
			AND development_type_id <> 20
			AND development_type_id <> 21
			AND development_type_id <> 22
			)AS usb 										--NOT IN RESIDENTIAL
		ON js.building_id = usb.building_id
	WHERE js.job_spaces - COALESCE(jsu.job_spaces_used, 0) > 0
)
, job_spaces AS(
	SELECT building_id
		,development_type_id
		,sector_id
		,job_spaces_available
		,ROW_NUMBER() OVER(PARTITION BY sector_id ORDER BY NEWID()) AS random
	FROM job_spaces_available AS a
		,ref.numbers AS n
	WHERE n.numbers <= a.job_spaces_available
)
INSERT INTO urbansim.jobs (job_id, sector_id, building_id, source)
SELECT job_id
	,j.sector_id
	,building_id
	,'SEM'
FROM job_spaces AS s
JOIN jobs AS j
	ON j.random = s.random
	AND j.sector_id = s.sector_id
ORDER BY building_id
;