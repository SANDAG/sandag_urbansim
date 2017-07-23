/****** Script for SelectTopNRows command from SSMS  ******/
SELECT [yr]
	,[sector_id]
	,[job_id]
FROM [spacecore].[input].[jobs_selfemployed_2012_2016]
WHERE yr = 2015

SELECT sector_id, COUNT(*)
FROM [spacecore].[input].[jobs_selfemployed_2012_2016]
WHERE yr = 2015
GROUP BY sector_id
ORDER BY sector_id

104
,105
,106
,107
,108
,115
,119
,120


109
,110
,111
,112
,114
,116
,117

/*
SELECT * FROM urbansim.jobs WHERE building_id = 543915
SELECT * FROM urbansim.jobs WHERE building_id = 803244
SELECT * FROM urbansim.jobs WHERE building_id =	542865
SELECT COUNT(*) FROM urbansim.jobs	

--priv	--1157329	
--mil	--1282039
--gov	--1496339

*/

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



--ALLOCATE SELFEMPLOYED JOBS IN NON RESIDENTAL BUILDINGS
WITH jobs AS(
	SELECT job_id
		,sector_id
	FROM spacecore.input.jobs_selfemployed_2012_2016
	WHERE yr = 2015
	AND sector_id IN(										--RESIDENTIAL BUILDING NOT COMPATIBLE
	109
	,110
	,111
	,112
	,114
	,116
	,117)
)
, job_spaces_used AS(
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
	FROM (SELECT * 
			FROM urbansim.buildings
			WHERE development_type_id <> 19
			AND development_type_id <> 20
			AND development_type_id <> 21
			AND development_type_id <> 22
			)AS usb 										--NOT IN RESIDENTIAL
		LEFT JOIN job_spaces_used AS j ON usb.building_id = j.building_id 
	WHERE usb.job_spaces IS NOT NULL
)
, job_spaces AS(
	SELECT building_id
		,development_type_id
		,job_spaces_available
		,ROW_NUMBER() OVER(ORDER BY NEWID()) AS random
	FROM job_spaces_available AS a
		,ref.numbers AS n
	WHERE a.job_spaces_available > 0
	AND n.numbers <= a.job_spaces_available
)
--INSERT INTO urbansim.jobs (job_id, sector_id, building_id)
SELECT job_id
	,sector_id
	,building_id
FROM job_spaces AS s
JOIN jobs AS j ON j.job_id = s.random
ORDER BY building_id



	