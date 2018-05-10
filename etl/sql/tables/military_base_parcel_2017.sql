USE spacecore
;

--GRAB MILITARY FACILITIES INTO TEMP TABLE
DROP TABLE IF EXISTS #mil
;
SELECT
	MIN(OBJECTID) AS OBJECTID
	,BASENAME AS base_name
	,geometry::UnionAggregate(Shape) AS shape
INTO #mil
FROM OPENQUERY(sql2014b8, 'SELECT * FROM lis.gis.MILITARY_FACILITY')
GROUP BY BASENAME
;
SELECT * FROM #mil ORDER BY OBJECTID
;
--CHECK SRID
SELECT DISTINCT Shape.STSrid FROM #mil;		--2230 = NAD83 / California zone 6 (ftUS)
;

--CALCULATE INTERSECTIONS AT THE SUBPARCEL LEVEL, GRAB INTO TEMP TABLE
DROP TABLE IF EXISTS #mx
;
SELECT
	l.parcelId
	,l.subParcel AS subparcel_id
	,m.OBJECTID
	,m.base_name
	,l.shape.STArea()/43560 AS acres_subparcel
	,m.shape.STArea()/43560 AS acres_base
	,l.shape.STIntersection(m.shape).STArea()/43560 AS acres_intersection
INTO #mx
FROM gis.ludu2017 AS l
JOIN #mil AS m ON l.shape.STIntersects(m.shape) = 1
ORDER BY
	l.parcelId
	,base_name
;
SELECT * FROM #mx ORDER BY parcelId
;

DROP TABLE IF EXISTS #milx
;
SELECT m.*
	,usp.parcel_id AS parcel_id_15
INTO #milx
FROM #mx AS m
JOIN gis.ludu2017points AS lp ON m.subparcel_id = lp.subParcel
JOIN urbansim.parcels AS usp ON lp.shape.STIntersects(usp.shape) = 1
;
SELECT * FROM #milx ORDER BY parcel_id
;

--FIND PARCEL BY BASE, WHEN GREATER THAN 25%
DROP TABLE IF EXISTS GIS.military_facility_parcel_2017_2015
;
WITH mil AS(
	SELECT
		parcel_id_15
		,base_name
	FROM #milx
	WHERE acres_intersection / acres_subparcel > 0.25		--GREATER THAN 25%
	OR acres_intersection / acres_base > 0.25				--GREATER THAN 25%
	GROUP BY
	parcel_id_15
	,base_name
	--ORDER BY
	--	parcel_id_15
	--	,base_name
)
,mp AS(
	SELECT
		mil.parcel_id_15
		,mil.base_name
		,ROW_NUMBER() OVER(PARTITION BY mil.parcel_id_15 ORDER BY m.acres_intersection DESC) AS rownum
	FROM mil
	LEFT JOIN (SELECT parcel_id_15, SUM(acres_intersection) AS acres_intersection
				FROM #milx
				GROUP BY parcel_id_15) AS m 
		ON mil.parcel_id_15 = m.parcel_id_15
	--ORDER BY parcel_id
)
SELECT
	usp.parcel_id
	,mp.base_name
	,ISNULL(rownum, 0) AS mil_base
INTO GIS.military_facility_parcel_2017_2015
FROM urbansim.parcels AS usp
LEFT JOIN (SELECT * FROM mp WHERE rownum = 1) AS mp ON usp.parcel_id = mp.parcel_id_15
ORDER BY usp.parcel_id
;
SELECT * FROM GIS.military_facility_parcel_2017_2015 WHERE mil_base = 1 ORDER BY parcel_id
;

--MANUAL OVERRIDES
UPDATE  GIS.military_facility_parcel_2017_2015
SET base_name = NULL
	,mil_base = 0
WHERE parcel_id IN(739206, 5090444, 5276406)
;

DROP TABLE #mil
DROP TABLE #milx
;

--************************************************************
--CHECKS

SELECT *
FROM #milx
WHERE parcel_id = 739206


SELECT *
FROM GIS.military_facility_parcel_2017_2015
WHERE mil_base = 1
ORDER BY parcel_id



SELECT parcel_id, COUNT(*) AS c
FROM GIS.military_facility_parcel_2017_2015
GROUP BY parcel_id
HAVING COUNT(*) > 0
ORDER BY c DESC


SELECT [parcel_id]
	,[base_name]
	,[mil_base]
FROM [spacecore].[GIS].military_facility_parcel_2017_2015
WHERE mil_base = 1
ORDER BY parcel_id


SELECT COALESCE(a.[parcel_id], b.parcel_id)
	,a.[base_name]
	,b.base_name
FROM (SELECT * FROM [spacecore].[GIS].military_facility_parcel_2017_2015 WHERE mil_base = 1) AS a
FULL OUTER JOIN (SELECT * FROM [spacecore].[GIS].[military_facility_parcel_2015] WHERE mil_base = 1) AS b
	ON a.parcel_id = b.parcel_id
ORDER BY COALESCE(a.[parcel_id], b.parcel_id)