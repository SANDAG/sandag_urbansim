/****** CREATE NEW NON RES BUILDINGS IN PARCELS WITH EXISTING JOBS ******/

--FIND PARCELS WITH EXISTING JOBS
IF OBJECT_ID('tempdb..#parcels_jobs') IS NOT NULL
    DROP TABLE #parcels_jobs
;
SELECT p.parcel_id
	--,[development_type_id]
	--,[jurisdiction_id]
	,COUNT(*) AS jobs
INTO #parcels_jobs
FROM [spacecore].[urbansim].[parcels] AS p
JOIN urbansim.buildings AS b ON p.parcel_id = b.parcel_id
JOIN urbansim.jobs AS j ON b.building_id = j.building_id
GROUP BY p.parcel_id
;
--SELECT * FROM #parcels_jobs

--PARCELS WITH EXISTING JOBS AND AREA
IF OBJECT_ID('tempdb..#parcels') IS NOT NULL
    DROP TABLE #parcels
;
SELECT p.parcel_id
	,pj.jobs
	,p.shape.STArea() AS aa
INTO #parcels
FROM urbansim.parcels AS p
JOIN #parcels_jobs AS pj ON p.parcel_id = pj.parcel_id
;
--SELECT * FROM #parcels

--DROP PREVIOUS TEMP TABLE
DROP TABLE #parcels_jobs


/*########## CALCULATE NEW JOB_SPACES ##########*/
--APPLY FAR AND VACANCY RATES
--FAR =		50% =	0.5
--VACANCY =	+10% =	1.1
SELECT parcel_id
	,jobs
	,aa
	--,aa*0.5 AS bldg
	--,aa*0.5/400 AS job_spaces											--36,222 FEASIBLE
	--,CEILING(aa*0.5/400) AS job_spaces
	--,(aa*0.5/400)*1.1 AS job_spaces_vac
	--,CEILING((aa*0.5/400)*1.1) AS job_spaces_vac
	,CEILING((CEILING(aa*0.5/400))*1.1) AS job_spaces_vac				--36,491 FEASIBLE
	,CASE WHEN jobs >= CEILING((CEILING(aa*0.5/400))*1.1) THEN 'NO'
		ELSE 'FEASIBLE'
	END AS feasible
	,CEILING((CEILING(aa*0.5/400))*1.1) - jobs AS new_job_spaces
	--,COALESCE(NULL, (aa/CEILING((CEILING(aa*0.5/400))*1.1) - jobs)) AS ratio
FROM #parcels
--ORDER BY 11 DESC, 1
ORDER BY 6 DESC, 1


--SUMMARIZE
SELECT
	CASE WHEN jobs >= CEILING((CEILING(aa*0.5/400))*1.1) THEN 'NO'
		ELSE 'FEASIBLE'
	END AS feasible
	,COUNT(*) AS parcels
	,SUM(jobs) AS jobs
	,SUM(CEILING((CEILING(aa*0.5/400))*1.1)) AS job_spaces_vac
	,SUM(CEILING((CEILING(aa*0.5/400))*1.1) - jobs) AS new_job_spaces
FROM #parcels
GROUP BY
	CASE WHEN jobs >= CEILING((CEILING(aa*0.5/400))*1.1) THEN 'NO'
		ELSE 'FEASIBLE'
	END

--DROP TEMP TABLE
--DROP TABLE #parcels

