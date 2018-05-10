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

--CHECK FOR NULLS
SELECT * FROM #mil WHERE OBJECTID IS NULL;

--CREATE A PRIMARY KEY SO WE CAN CREATE A SPATIAL INDEX
ALTER TABLE #mil ALTER COLUMN OBJECTID int NOT NULL;
ALTER TABLE #mil ADD CONSTRAINT pk_mil_id PRIMARY KEY CLUSTERED (OBJECTID);

--SET THE SHAPES TO BE NOT NULL SO WE CAN CREATE A SPATIAL INDEX
ALTER TABLE gis.#mil ALTER COLUMN shape geometry NOT NULL;

--CREATE SPATIAL INDEX
CREATE SPATIAL INDEX ix_spatial_mil_shape ON #mil
(
    shape
) USING  GEOMETRY_GRID
    WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
    CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
;


--CALCULATE INTERSECTIONS AT THE SUBPARCEL LEVEL, GRAB INTO TEMP TABLE
DROP TABLE IF EXISTS #milx
;
SELECT
	parcelId AS parcel_id
	,LCKey	AS subparcel_id
	,m.OBJECTID
	,m.base_name
	,l.shape.STArea()/43560 AS acres_subparcel
	,m.shape.STArea()/43560 AS acres_base
	,l.shape.STIntersection(m.shape).STArea()/43560 AS acres_intersection
INTO #milx
FROM gis.ludu2015 AS l
JOIN #mil AS m ON l.shape.STIntersects(m.shape) = 1
ORDER BY
	parcelId
	,base_name
;
SELECT * FROM #milx ORDER BY parcel_id
;


--FIND PARCEL BY BASE, WHEN GREATER THAN 25%
DROP TABLE IF EXISTS GIS.military_facility_parcel_2015
;
WITH mil AS(
	SELECT
		parcel_id
		,base_name
	FROM #milx
	WHERE acres_intersection / acres_subparcel > 0.25		--GREATER THAN 25%
	OR acres_intersection / acres_base > 0.25				--GREATER THAN 25%
	GROUP BY
		parcel_id
		,base_name
	--ORDER BY
	--	parcel_id
	--	,base_name
)
--,mp AS(
	SELECT
		mil.parcel_id
		,mil.base_name
		,ROW_NUMBER() OVER(PARTITION BY mil.parcel_id ORDER BY m.acres_intersection DESC) AS rownum
	FROM mil
	LEFT JOIN (SELECT parcel_id, SUM(acres_intersection) AS acres_intersection
				FROM #milx
				GROUP BY parcel_id) AS m 
		ON mil.parcel_id = m.parcel_id
	ORDER BY parcel_id
)
SELECT
	usp.parcel_id
	,mp.base_name
	,ISNULL(rownum, 0) AS mil_base
INTO GIS.military_facility_parcel_2015
FROM urbansim.parcels AS usp
LEFT JOIN (SELECT * FROM mp WHERE rownum = 1) AS mp ON usp.parcel_id = mp.parcel_id
ORDER BY usp.parcel_id
;
SELECT * FROM GIS.military_facility_parcel_2015 WHERE mil_base = 1 ORDER BY parcel_id
;

--MANUAL OVERRIDES
UPDATE  GIS.military_facility_parcel_2015
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
WHERE parcel_id = 12303


SELECT [parcel_id]
	,[base_name]
	,[mil_base]
FROM [spacecore].[GIS].[military_facility_parcel_2015]
WHERE mil_base = 1
ORDER BY parcel_id

SELECT [parcel_id]
	,[base_name]
	,[mil_base]
FROM [spacecore].[GIS].[military_facility_parcel_2015c]
WHERE mil_base = 1
ORDER BY parcel_id


SELECT COALESCE(a.[parcel_id], b.parcel_id)
	,a.[base_name]
	,b.base_name
FROM (SELECT * FROM [spacecore].[GIS].[military_facility_parcel_2015] WHERE mil_base = 1) AS a
FULL OUTER JOIN (SELECT * FROM [spacecore].[GIS].[military_facility_parcel_2015c] WHERE mil_base = 1) AS b
	ON a.parcel_id = b.parcel_id
ORDER BY COALESCE(a.[parcel_id], b.parcel_id)
