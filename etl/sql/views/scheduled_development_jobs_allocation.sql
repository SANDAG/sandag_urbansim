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
FROM [spacecore].[urbansim].[scheduled_development_parcels]
WHERE civemp > 0
ORDER BY
	site_id
	,parcel_id
;

SELECT DISTINCT site_id
FROM [spacecore].[urbansim].[scheduled_development_parcels]
WHERE civemp > 0
;

SELECT *
FROM [spacecore].[GIS].[scheduled_development_sites]
WHERE ([totalsqft] > 0 OR [civemp] > 0)		--HAS emp OR sqft
AND ([sfu] > 0 OR [mfu] > 0	OR [mhu] > 0)	--HAS res units
ORDER BY civemp DESC
;

SELECT *
FROM [spacecore].[GIS].[scheduled_development_sites]
WHERE [civemp] > 0								--HAS emp
AND (([sfu] > 0 OR [mfu] > 0	OR [mhu] > 0)	--HAS res units OR IS res
	OR devtypeid IN (19, 20, 21))
ORDER BY civemp DESC
;

SELECT *
FROM [spacecore].[GIS].[scheduled_development_sites]
WHERE devtypeid NOT IN (19, 20, 21)		--DEVELOPMENT TYPE ID NOT IN RESIDENTIAL
AND status <> 'completed'				--NOT COMPLETED
AND source NOT LIKE ""
ORDER BY civemp DESC
;

/************************ SCHEDULED DEVELOPMENT SITES, LOAD IMPUTED EMPLOYMENT ************************/
/***##### CIVEMP FROM .CSV INTO #TEMP TABLE #####***/
DROP TABLE IF EXISTS #table
CREATE TABLE #table (
	--ogr_fid int NOT NULL
	siteid int NOT NULL --PRIMARY KEY
	,sitename varchar(75)
	,civemp varchar(50)
	,civemp_notes text
);

BULK INSERT #table
--FROM '\\nasb8\Shared\RES\Users\rco\Scheduled Development\non_res_civemp.txt'
--M:\RES\estimates & forecast\SR14 Forecast\Scheduled Development\Non-residential Scheduled Development
FROM '\\nasb8\Shared\RES\estimates & forecast\SR14 Forecast\Scheduled Development\Non-residential Scheduled Development\non_res_civemp.txt'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = '\t',
	ROWTERMINATOR = '\n',
	TABLOCK
);

SELECT * FROM #table

SELECT * 
INTO ##table
FROM #table


/***##### UPDATE TABLE USING JOIN #####***/
--ADD COLUMNS
ALTER TABLE [spacecore].[GIS].[scheduled_development_sites]
ADD civemp_imputed int
	,civemp_notes text

--CHECK
SELECT
	sds.siteid, t.siteid,
	sds.sitename, t.sitename,
	sds.civemp_imputed, CAST(t.civemp AS int)
	,sds.civemp_notes, t.civemp_notes
FROM  #table AS t
JOIN [spacecore].[GIS].[scheduled_development_sites] AS sds
	ON sds.siteid = t.siteid
	AND sds.sitename = REPLACE(t.sitename, '"', '')
ORDER BY t.siteid

--UPDATE
UPDATE sds
SET
	sds.civemp_imputed = CAST(t.civemp AS int)
	,sds.civemp_notes = t.civemp_notes
FROM [spacecore].[GIS].[scheduled_development_sites] AS sds
JOIN #table AS t
	ON sds.siteid = t.siteid
	AND sds.sitename = REPLACE(t.sitename, '"', '')






/************************ SCHEDULED DEVELOPMENT SITES EMPLOYMENT ************************/
--LOAD SITES
DROP TABLE IF EXISTS #sds
;
SELECT 
	[siteid]
	,MAX(devtypeid) devtypeid
	,SUM([civemp_imputed]) AS [civemp_imputed]
INTO #sds
FROM [GIS].[scheduled_development_sites]
WHERE [civemp_imputed] > 0
--WHERE sds.devtypeid NOT IN (19, 20, 21)		--DEVELOPMENT TYPE ID NOT IN RESIDENTIAL
--AND sds.status <> 'completed'				--NOT COMPLETED
GROUP BY [siteid]

SELECT * FROM #sds

--LOAD PARCELS INTO TEMP TABLE, CALCULATE DEVELOPABLE ACRES
DROP TABLE IF EXISTS #emp_parcels
;
SELECT 
	sdsp.ogr_fid
	,sdsp.[site_id]
	,sdsp.[parcel_id]
	--,sdsp.[startdate]
	--,sdsp.[compdate]
	--,sdsp.[civemp]
	--,sdsp.[milemp]
	--,sdsp.devtypeid
	,usp.proportion_undevelopable
	,usp.parcel_acres 
	,CASE 
		  WHEN usp.proportion_undevelopable IS NULL THEN parcel_acres 
		  ELSE (parcel_acres - (parcel_acres*usp.proportion_undevelopable))
	  END AS developable_acres
INTO #emp_parcels
FROM [spacecore].[urbansim].[scheduled_development_parcels] sdsp
JOIN #sds AS sds ON sdsp.site_id = sds.siteid
JOIN urbansim.parcels AS usp ON sdsp.parcel_id = usp.parcel_id
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
	c.[site_id]
	,parcel_id
	,parcel_acres 
	,developable_acres
	,ROW_NUMBER() OVER(PARTITION BY c.site_id ORDER BY developable_acres DESC) AS rownum
	,parcel_count
	,IIF(c.parcel_count = 1, civemp_imputed, NULL) AS emp_allocated		--ONE PARCEL PER SITE
INTO #emp_parcel_jobs
FROM #emp_parcels AS p
LEFT JOIN #emp_parcel_count AS c 
	ON p.site_id = c.site_id
JOIN #sds AS sds
	ON c.site_id = sds.siteid
ORDER BY parcel_count
;
SELECT * FROM #emp_parcel_jobs ORDER BY emp_allocated DESC
;

--CALCULATE JOBS PER DEVELOPABLE ACRE
DROP TABLE IF EXISTS #emp_per_acre
;

WITH a AS(
	SELECT
		p.site_id
		,parcel_count
		,SUM(developable_acres) AS developable_acres
	FROM #emp_parcels AS p
	JOIN #emp_parcel_count AS c 
		ON p.site_id = c.site_id
	WHERE parcel_count > 1
	GROUP BY p.site_id, parcel_count
), e AS(
	SELECT
		siteid
		,civemp_imputed
	FROM #sds AS sds
	WHERE civemp_imputed > 0
)
SELECT
	site_id
	,parcel_count
	,civemp_imputed
	,developable_acres
	,civemp_imputed/developable_acres AS jobs_per_dev_acre
INTO #emp_per_acre
FROM a
JOIN e ON a.site_id = e.siteid

SELECT * FROM #emp_per_acre ORDER BY site_id


--ALLOCATE UNIS BY SQFT
SELECT 
	*
	,FLOOR(epj.developable_acres*jobs_per_dev_acre) AS emp_allocated
FROM #emp_parcel_jobs AS epj
JOIN #emp_per_acre AS epa ON epj.site_id = epa.site_id
WHERE epj.parcel_count > 1
ORDER BY epj.site_id

UPDATE epj
SET emp_allocated = FLOOR(epj.developable_acres*jobs_per_dev_acre)
FROM #emp_parcel_jobs AS epj
JOIN #emp_per_acre AS epa ON epj.site_id = epa.site_id
WHERE epj.parcel_count > 1
;

SELECT * FROM #emp_parcel_jobs
WHERE site_id = 15035
ORDER BY emp_allocated DESC 
;


--ALLOCATE RESIDUAL UNITS
--SELECT * FROM #emp_parcel_jobs
WITH x AS(
	SELECT
		site_id
		--,COUNT(*) AS parcel_count
		--,SUM(emp_allocated) AS emp_allocated
		,MIN(sds.civemp_imputed) - SUM(emp_allocated) AS emp_allocation_residual
	FROM #emp_parcel_jobs AS epj
	JOIN #sds AS sds ON sds.siteid = epj.site_id
	GROUP BY site_id 
)
UPDATE epj
SET emp_allocated = emp_allocated + 1
FROM #emp_parcel_jobs AS epj
JOIN x ON epj.site_id = x.site_id
	AND rownum <= emp_allocation_residual


SELECT *
FROM #emp_parcel_jobs

--VERIFY
SELECT
	siteid
	--,sitename
	,civemp_imputed
	,devtypeid
	--,source
	,parcel_count
	,emp_allocated
	,civemp_imputed - emp_allocated AS diff
FROM #sds AS sds
LEFT JOIN (SELECT
			site_id
			,COUNT(*) AS parcel_count
			,SUM(emp_allocated) AS emp_allocated
		FROM #emp_parcel_jobs
		GROUP BY site_id
	) AS epj ON sds.siteid = epj.site_id
WHERE civemp_imputed > 0
--AND civemp_imputed - emp_allocated > 0		--CHECK

/*
/************************ INSERT TO SCHEDULED DEVELOPMENT PARCELS ************************/
--ADD COLUMN AND SET TO 0
ALTER TABLE urbansim.scheduled_development_parcels
ADD civemp_imputed int NULL
	,sector_id int NULL
;

--UPDATE PARCELS ALREADY IN TABLE
--TRANSLATE DEVELOPMENT_TYPE_ID TO SANDAG_INDUSTRY_ID
UPDATE sdp
SET civemp_imputed = epj.emp_allocated
	,sector_id =
	CASE sds.devtypeid
		WHEN 2 THEN 5
		WHEN 3 THEN 5
		WHEN 4 THEN 12
		WHEN 5 THEN 7
		WHEN 6 THEN 8
		WHEN 7 THEN 18
		WHEN 12 THEN 16
		WHEN 13 THEN 20
		WHEN 16 THEN 26
		WHEN 18 THEN 7
	END
FROM urbansim.scheduled_development_parcels AS sdp
JOIN #emp_parcel_jobs AS epj ON sdp.parcel_id = epj.parcel_id
JOIN #sds AS sds ON sdp.site_id = sds.siteid

--INSERT ADDITIONAL PARCELS
--TRANSLATE DEVELOPMENT_TYPE_ID TO SANDAG_INDUSTRY_ID
INSERT INTO spacecore.urbansim.scheduled_development_parcels(
	site_id
	,parcel_id
	,shape
	,civemp_imputed
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
	,CASE sds.devtypeid
		WHEN 2 THEN 5
		WHEN 4 THEN 12
		WHEN 7 THEN 18
		WHEN 12 THEN 16
		WHEN 13 THEN 20
		WHEN 16 THEN 26
		WHEN 18 THEN 7
	END AS sector_id
FROM #emp_parcel_jobs AS epj
JOIN (SELECT parcel_id, shape FROM urbansim.parcels) AS usp ON epj.parcel_id = usp.parcel_id
JOIN #sds AS sds ON epj.site_id = sds.siteid
WHERE epj.parcel_id NOT IN (SELECT parcel_id FROM urbansim.scheduled_development_parcels)


/************************ CHECKS ************************/
SELECT
	siteid
	,sds.civemp_imputed
	,sdp.civemp_imputed
	,sdp.parcel_count
	,sds.civemp_imputed - sdp.civemp_imputed AS diff
FROM [GIS].[scheduled_development_sites] AS sds
JOIN (SELECT site_id, SUM(civemp_imputed) AS civemp_imputed, COUNT(*) AS parcel_count
		FROM spacecore.urbansim.scheduled_development_parcels
		GROUP BY site_id
	) AS sdp
	ON sds.siteid = sdp.site_id
WHERE sds.civemp_imputed > 0
OR sdp.civemp_imputed > 0
ORDER BY siteid
*/


/************************ INSERT TO SCHED DEV ALL ************************/
--ADD COLUMN AND SET TO 0
ALTER TABLE [urbansim].[urbansim].[sched_dev_all]
ADD civemp_imputed int NULL
	,sector_id int NULL
;

--UPDATE PARCELS ALREADY IN TABLE
--TRANSLATE DEVELOPMENT_TYPE_ID TO SANDAG_INDUSTRY_ID
UPDATE sda
SET civemp_imputed = epj.emp_allocated
	,sector_id =
	CASE sds.devtypeid
		WHEN 2 THEN 5
		WHEN 3 THEN 5
		WHEN 4 THEN 12
		WHEN 5 THEN 7
		WHEN 6 THEN 8
		WHEN 7 THEN 18
		WHEN 12 THEN 16
		WHEN 13 THEN 20
		WHEN 16 THEN 26
		WHEN 18 THEN 7
	END
FROM [urbansim].[urbansim].[sched_dev_all] AS sda
JOIN #emp_parcel_jobs AS epj ON sda.parcel_id = epj.parcel_id
JOIN #sds AS sds ON sda.site_id = sds.siteid

--INSERT ADDITIONAL PARCELS
--TRANSLATE DEVELOPMENT_TYPE_ID TO SANDAG_INDUSTRY_ID
INSERT INTO [urbansim].[urbansim].[sched_dev_all](
	site_id
	,parcel_id
	--,shape
	,civemp_imputed
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
	--,usp.shape
	--,civGQ
	,epj.emp_allocated
	,CASE sds.devtypeid
		WHEN 2 THEN 5
		WHEN 4 THEN 12
		WHEN 7 THEN 18
		WHEN 12 THEN 16
		WHEN 13 THEN 20
		WHEN 16 THEN 26
		WHEN 18 THEN 7
	END AS sector_id
FROM #emp_parcel_jobs AS epj
JOIN (SELECT parcel_id, shape FROM urbansim.parcels) AS usp ON epj.parcel_id = usp.parcel_id
JOIN #sds AS sds ON epj.site_id = sds.siteid
WHERE epj.parcel_id NOT IN (SELECT parcel_id FROM [urbansim].[urbansim].[sched_dev_all])


/************************ CHECKS ************************/
SELECT
	siteid
	,sds.civemp_imputed
	,sda.civemp_imputed
	,sda.parcel_count
	,sds.civemp_imputed - sda.civemp_imputed AS diff
FROM [GIS].[scheduled_development_sites] AS sds
JOIN (SELECT site_id, SUM(civemp_imputed) AS civemp_imputed, COUNT(*) AS parcel_count
		FROM spacecore.urbansim.scheduled_development_parcels
		GROUP BY site_id
	) AS sda
	ON sds.siteid = sda.site_id
WHERE sds.civemp_imputed > 0
OR sda.civemp_imputed > 0
ORDER BY siteid


