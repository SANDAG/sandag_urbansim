USE spacecore
;

/*#################### RUN 1/2, DISCARD DUPLICATES BY NON DEVELOPABLE LU ####################*/
--FIND DUPLICATES ON PARCELID
IF OBJECT_ID('tempdb..#parcels_dup') IS NOT NULL
    DROP TABLE #parcels_dup
;
WITH r AS(
	SELECT 
		parcel_id
	FROM urbansim.general_plan_parcels
	GROUP BY parcel_id
	HAVING COUNT(*) >= 2
)
SELECT
	p.gpid
	,r.parcel_id
	,p.year
	,p.sphere
	,p.planid
	,p.gplu
	,CASE
		WHEN gplu BETWEEN 1000 AND 1999 THEN 1		--RESIDENTIAL RELATED
		WHEN gplu = 9700 THEN 1						--MIXED USE
		WHEN gplu BETWEEN 4100 AND 4199 THEN 3		--TRANSPORTATION RELATED
		WHEN gplu BETWEEN 7600 AND 7699 THEN 3		--PARKS RELATED
		WHEN gplu BETWEEN 9200 AND 9399 THEN 3		--WATER RELATED
		WHEN gplu = 9300 THEN 3						--INDIAN RESERVATION
		ELSE 2										--ALL OTHER
	END AS gplu_case
--INTO #parcels_dup
FROM r
JOIN urbansim.general_plan_parcels AS p ON r.parcel_id = p.parcel_id
ORDER BY parcel_id
;
SELECT * FROM #parcels_dup ORDER BY parcel_id, gplu
--7,353 duplicates
--3,538 unique


--DROP NON DEVELOPABLE DUPLICATES
--ORDER BY CASE DEVELOPABLE
IF OBJECT_ID('tempdb..#parcels_dup_case') IS NOT NULL
    DROP TABLE #parcels_dup_case
;
SELECT *
	,ROW_NUMBER() OVER(PARTITION BY parcel_id ORDER BY parcel_id, gplu_case, gplu) AS row_gplu
INTO #parcels_dup_case
FROM #parcels_dup
;
SELECT * FROM #parcels_dup_case


--DELETE NON DEVELOPABLE DUPLICATES, LEAVING SINGLE RECORD EVEN IF NON DEVELOPABLE
--SELECT *
DELETE p
FROM urbansim.general_plan_parcels AS p
JOIN #parcels_dup_case AS d ON p.gpid = d.gpid
WHERE gplu_case > 1
AND row_gplu > 1
;
--3,319


/*#################### RUN 2/2, DISCARD DUPLICATES BY DEVELOPABLE ACRES ####################*/
--FIND DUPLICATES ON PARCELID
IF OBJECT_ID('tempdb..#parcels_dup') IS NOT NULL
    DROP TABLE #parcels_dup
;
WITH r AS(
	SELECT 
		parcel_id
		,COUNT(*) AS _count
	FROM urbansim.general_plan_parcels
	GROUP BY parcel_id
)
SELECT
	p.gpid
	,r.parcel_id
	,p.year
	,p.sphere
	,p.planid
	,p.gplu
INTO #parcels_dup
FROM r
JOIN urbansim.general_plan_parcels AS p ON r.parcel_id = p.parcel_id
WHERE r._count >= 2
ORDER BY parcel_id
;
SELECT * FROM #parcels_dup ORDER BY parcel_id, gplu
--983 duplicates
--487 unique


--CALCULATE DEVOPABLE AREA IN PARCEL 
--FIND NON CONSTRAINED PART OF PARCEL
IF OBJECT_ID('tempdb..#parcels_dev') IS NOT NULL
    DROP TABLE #parcels_dev
;
SELECT 
	parcel_id
	,usp.shape
	,usp.shape.STDifference(cons.geom) AS shape_dev
INTO #parcels_dev
FROM (SELECT parcel_id, shape FROM urbansim.parcels WHERE parcel_id IN(SELECT parcel_id FROM #parcels_dup)) AS usp
CROSS APPLY(
	SELECT Geometry::UnionAggregate(geom) AS geom
	FROM gis.devcons AS cons
	WHERE usp.shape.STIntersects(cons.geom) = 1
	) AS cons
--WHERE parcel_id = 5109337	--368		--TEST
ORDER BY parcel_id
--487

--TEST
--SELECT * FROM urbansim.parcels	WHERE parcel_id = 368
--SELECT * FROM #parcels_dev		WHERE parcel_id = 582		ORDER BY parcel_id		--3,538
--SELECT * FROM #parcels_dev		WHERE shape_dev IS NULL		ORDER BY parcel_id		--1,225


--CALCULATE INTERSECTIONS, PARCEL TO GP
IF OBJECT_ID('tempdb..#parcels_dup_intersection') IS NOT NULL
    DROP TABLE #parcels_dup_intersection
;
SELECT 
	--pd.gpid
	p.parcel_id
	,pd.sphere
	,pd.planid
	,pd.gplu
	--,shape
	,geometry::UnionAggregate(gp.ogr_geometry.STIntersection(p.shape)) AS gp_parcel				--FOR COMPLETE PARCEL
	,geometry::UnionAggregate(gp.ogr_geometry.STIntersection(p.shape_dev)) AS gp_parcel_dev		--FOR DEVELOPABLE AREA OF PARCEL
INTO #parcels_dup_intersection
FROM #parcels_dev AS p
JOIN #parcels_dup AS pd ON pd.parcel_id = p.parcel_id
JOIN gis.general_plan AS gp
	--ON gp.ogr_geometry.STIntersects(p.shape) = 1
	ON pd.planid = gp.planid
--WHERE parcel_id = 368
GROUP BY --pd.gpid
	p.parcel_id
	,pd.sphere
	,pd.planid
	,pd.gplu
ORDER BY parcel_id
;

SELECT * FROM #parcels_dup_intersection ORDER BY parcel_id
--983


--LOAD INTERSECTION VALUE TO PARCEL AND STORE TO TABLE
IF OBJECT_ID('tempdb..#parcels_dup_dev') IS NOT NULL
    DROP TABLE #parcels_dup_dev
;
SELECT 
	--IDENTITY(int,1,1) AS gpmid
	p.gpid
	,p.parcel_id
	,p.year
	,p.sphere
	,p.planid
	,p.gplu
	,pin.gp_parcel.STArea()/43560 AS gplu_acres													--FOR COMPLETE PARCEL
	,ROW_NUMBER() OVER(PARTITION BY p.parcel_id ORDER BY pin.gp_parcel.STArea() DESC) AS rownum
	,pin.gp_parcel_dev.STArea()/43560 AS gplu_dev_acres											--FOR DEVELOPABLE AREA OF PARCEL
	,CASE 
		WHEN pin.gp_parcel_dev.STArea() IS NULL THEN NULL
		WHEN pin.gp_parcel_dev.STArea() = 0 THEN NULL
		ELSE ROW_NUMBER() OVER(PARTITION BY p.parcel_id ORDER BY pin.gp_parcel_dev.STArea() DESC)
	END AS rownum_dev
INTO #parcels_dup_dev
FROM #parcels_dup AS p
JOIN #parcels_dup_intersection AS pin
ON p.parcel_id = pin.parcel_id
	AND p.sphere = pin.sphere
	AND p.planid = pin.planid
	AND p.gplu = pin.gplu
ORDER BY p.parcel_id
	,gplu

--CHECK
SELECT * FROM #parcels_dup_dev
--983

--DISCARD DUPLICATES
WITH s AS(
	SELECT gpid, parcel_id
	FROM #parcels_dup_dev
	WHERE rownum_dev = 1
	UNION ALL
	SELECT gpid, parcel_id
	FROM #parcels_dup_dev
	WHERE rownum = 1
	AND parcel_id NOT IN (
		SELECT parcel_id
		FROM #parcels_dup_dev
		WHERE rownum_dev = 1)
	--ORDER BY parcel_id, gpid
)--487
, d AS(
	SELECT gpid, parcel_id
	FROM #parcels_dup_dev
	WHERE gpid NOT IN (SELECT gpid FROM s)
)--496
DELETE
FROM urbansim.general_plan_parcels
WHERE gpid IN (SELECT gpid FROM d)
;

SELECT *
FROM urbansim.general_plan_parcels
ORDER BY parcel_id

