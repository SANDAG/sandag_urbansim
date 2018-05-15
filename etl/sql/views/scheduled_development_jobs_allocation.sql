USE spacecore
;

SELECT 
	[ogr_fid]
	,[site_id]
	,[parcel_id]
	,[startdate]
	,[compdate]
	,[civemp]
	,[milemp]
	,devtypeid
	--,[sfu]
	--,[mfu]
	--,[mhu]
FROM [spacecore].[staging].[sched_dev_sites_to_parcels]
WHERE civemp > 0
ORDER BY
	site_id
	,parcel_id
;

SELECT DISTINCT site_id
FROM [spacecore].[staging].[sched_dev_sites_to_parcels]
WHERE civemp > 0
;


/************************ SCHEDULED DEVELOPMENT SITES EMPLOYMENT ************************/
--LOAD PARCELS INTO TEMP TABLE, CALCULATE DEVELOPABLE ACRES
DROP TABLE IF EXISTS #emp_parcels
;
SELECT 
	[ogr_fid]
	,sdp.[site_id]
	,sdp.[parcel_id]
	,[startdate]
	,[compdate]
	,[civemp]
	,[milemp]
	,devtypeid
	,usp.proportion_undevelopable
	,usp.parcel_acres 
	,CASE 
		  WHEN usp.proportion_undevelopable IS NULL THEN parcel_acres 
		  ELSE (parcel_acres - (parcel_acres*usp.proportion_undevelopable))
	  END AS developable_acres
INTO #emp_parcels
FROM [spacecore].[staging].[sched_dev_sites_to_parcels] sdp
JOIN urbansim.parcels AS usp ON sdp.parcel_id = usp.parcel_id
WHERE civemp > 0
ORDER BY
	site_id
	,parcel_id
;
SELECT * FROM #emp_parcels
;

--COUNT PARCELS BY SITE ID
DROP TABLE IF EXISTS #emp_parcel_count
;
SELECT
	site_id
	,COUNT(*) AS parcel_count
INTO #emp_parcel_count
FROM #emp_parcels
GROUP BY site_id
;
SELECT * FROM #emp_parcel_count ORDER BY parcel_count
;

--ALLOCATE UNITS
DROP TABLE IF EXISTS #emp_parcel_jobs
;
SELECT 
	[ogr_fid]
	,c.[site_id]
	,[parcel_id]
	,[startdate]
	,[compdate]
	,[civemp]
	,[milemp]
	,devtypeid
	,parcel_acres 
	,developable_acres
	,ROW_NUMBER() OVER(PARTITION BY c.site_id ORDER BY developable_acres DESC) AS rownum
	,parcel_count
	,CASE parcel_count
		WHEN 1			THEN civemp						--ONE PARCEL PER SITE
		--WHEN civemp		THEN 1							--ONE PARCEL PER JOB ???
	END AS emp_allocated 
INTO #emp_parcel_jobs
FROM #emp_parcels AS p
JOIN #emp_parcel_count AS c ON p.site_id = c.site_id
ORDER BY parcel_count
;
SELECT * FROM #emp_parcel_jobs
;

--CALCULATE JOBS PER DEVELOPABLE ACRE
DROP TABLE IF EXISTS #emp_per_acre
;
SELECT
	site_id
	--,SUM(civemp) AS civemp
	--,MIN(parcel_count) AS parcel_count
	--,SUM(developable_acres) AS developable_acres
	,MIN(civemp)/SUM(developable_acres) AS jobs_per_dev_acre
INTO #emp_per_acre
FROM #emp_parcel_jobs
WHERE parcel_count > 1				--OR WHERE emp_allocated IS NULL
GROUP BY site_id

--ALLOCATE UNIS BY SQFT
SELECT 
	*
	,FLOOR(developable_acres*jobs_per_dev_acre) AS emp_allocated
FROM #emp_parcel_jobs AS epj
JOIN #emp_per_acre AS epa ON epj.site_id = epa.site_id

UPDATE epj
SET emp_allocated = FLOOR(developable_acres*jobs_per_dev_acre)
FROM #emp_parcel_jobs AS epj
JOIN #emp_per_acre AS epa ON epj.site_id = epa.site_id
;

--ALLOCATE RESIDUAL UNITS
--SELECT * FROM #emp_parcel_jobs
WITH x AS(
	SELECT
		site_id
		--,COUNT(*) AS parcel_count
		--,SUM(emp_allocated) AS emp_allocated
		,MIN(civemp) - SUM(emp_allocated) AS emp_allocation_residual
	FROM #emp_parcel_jobs
	GROUP BY site_id 
)
UPDATE epj
SET emp_allocated = emp_allocated + 1
FROM #emp_parcel_jobs AS epj
JOIN x ON epj.site_id = x.site_id
	AND rownum <= emp_allocation_residual

--VERIFY
SELECT
	siteid
	,sitename
	,civemp
	,devtypeid
	,source
	,parcel_count
	,emp_allocated
	,civemp - emp_allocated AS diff
FROM [GIS].[scheduled_development_sites] AS sds
JOIN (SELECT
			site_id
			,COUNT(*) AS parcel_count
			,SUM(emp_allocated) AS emp_allocated
		FROM #emp_parcel_jobs
		GROUP BY site_id
	) AS epj ON sds.siteid = epj.site_id
WHERE civemp > 0



/************************ INSERT TO SCHEDULED DEVELOPMENT PARCELS ************************/
--ADD COLUMN AND SET TO 0
ALTER TABLE urbansim.scheduled_development_parcels
ADD civemp int NULL
	,sector_id int NULL
;

--UPDATE PARCELS ALREADY IN TABLE
--TRANSLATE DEVELOPMENT_TYPE_ID TO SANDAG_INDUSTRY_ID
UPDATE sdp
SET civemp = epj.emp_allocated
	,sector_id =
	CASE epj.devtypeid
		WHEN 2 THEN 8
		WHEN 4 THEN 12
		WHEN 7 THEN 18
		WHEN 12 THEN 16
		WHEN 13 THEN 17
		WHEN 16 THEN 26
		WHEN 18 THEN 7
	END
FROM urbansim.scheduled_development_parcels AS sdp
JOIN #emp_parcel_jobs AS epj ON sdp.parcel_id = epj.parcel_id

--INSERT ADDITIONAL PARCELS
--TRANSLATE DEVELOPMENT_TYPE_ID TO SANDAG_INDUSTRY_ID
INSERT INTO spacecore.urbansim.scheduled_development_parcels(
	site_id
	,parcel_id
	,shape
	,civemp
	,sector_id
)
SELECT 
	site_id
	,epj.parcel_id
	--,capacity_3
	--,sfu_effective_adj
	--,mfu_effective_adj
	--,mhu_effective_adj
	--,notes
	--,editor
	,usp.shape
	--,civGQ
	,epj.emp_allocated
	,CASE devtypeid
		WHEN 2 THEN 8
		WHEN 4 THEN 12
		WHEN 7 THEN 18
		WHEN 12 THEN 16
		WHEN 13 THEN 17
		WHEN 16 THEN 26
		WHEN 18 THEN 7
	END AS sector_id
FROM #emp_parcel_jobs AS epj
JOIN (SELECT parcel_id, shape FROM urbansim.parcels) AS usp ON epj.parcel_id = usp.parcel_id
WHERE epj.parcel_id NOT IN (SELECT parcel_id FROM urbansim.scheduled_development_parcels)


/************************ CHECKS ************************/
SELECT
	siteid
	,sds.civemp
	,sdp.civemp
	,sdp.parcel_count
	,sds.civemp - sdp.civemp AS diff
FROM [GIS].[scheduled_development_sites] AS sds
JOIN (SELECT site_id, SUM(civemp) AS civemp, COUNT(*) AS parcel_count
		FROM spacecore.urbansim.scheduled_development_parcels
		GROUP BY site_id
	) AS sdp
	ON sds.siteid = sdp.site_id
WHERE sds.civemp > 0
OR sdp.civemp > 0
ORDER BY siteid


