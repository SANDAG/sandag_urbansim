/****** LOAD FROM GDB TO MSSQL ******/
--USE OGR2OGR
/*
E:\OSGeo4W64\bin\ogr2ogr.exe -f MSSQLSpatial "MSSQL:server=sql2014a8;database=spacecore;trusted_connection=yes" E:\data\urbansim_data_development\GP\gp.shp -nln general_plan -lco SCHEMA=gis -lco OVERWRITE=YES -OVERWRITE
*/
SELECT COUNT(*) FROM gis.general_plan
--//LOADED 87,543	FROM 87,545

USE spacecore

/****** FIX SRID  ******/
SELECT DISTINCT centroid.STSrid FROM gis.ludu2015points;
SELECT DISTINCT ogr_geometry.STSrid FROM GIS.general_plan;
--DEAL WITH NULL GEOMETRIES
SELECT * FROM GIS.general_plan WHERE ogr_geometry IS NULL;
DELETE FROM GIS.general_plan WHERE ogr_geometry IS NULL;
--FIX SRID
UPDATE GIS.general_plan SET ogr_geometry.STSrid = 2230;		--NAD83 / California zone 6 (ftUS)


/****** CREATE SPATIAL INDEX  ******/
--SET THE SHAPES TO BE NOT NULL SO WE CAN CREATE A SPATIAL INDEX
ALTER TABLE GIS.general_plan ALTER COLUMN ogr_geometry geometry NOT NULL

--SELECT max(x_coord), min(x_coord), max(y_coord), min(y_coord) from gis.parcels

CREATE SPATIAL INDEX [ix_spatial_gis_general_plan_ogr_fid] ON GIS.general_plan
(
    ogr_geometry
) USING  GEOMETRY_GRID
    WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
    CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
;

/****** SUBPARCELIDS WITH CENTROID IN GP ******/
--SELECT MAX YEAR FOR GP INTO TEMP TABLE
IF OBJECT_ID('tempdb..#gp') IS NOT NULL
    DROP TABLE #gp
;
SELECT ogr_fid
	,ogr_geometry
	,gp.year
	,gp.sphere
	,gp
	,planid
	,gplabel
	,designatio
	,loden
	,hiden
	,gplu
INTO #gp
FROM GIS.general_plan AS gp
--JOIN (SELECT						--TO SELECT MOST RECENT YEAR
--			sphere
--			,MAX(year) AS year
--		FROM GIS.general_plan
--		GROUP BY sphere
--	) AS gp_max
--	ON gp.sphere = gp_max.sphere
--	AND gp.year = gp_max.year

--SELECT * FROM #gp
--//ALL		= 87,543
--//SPHERES	= 103

--CREATE SPATIAL INDEX ON GP TEMP TABLE
--CREATE A PRIMARY KEY SO WE CAN CREATE A SPATIAL INDEX
ALTER TABLE #gp ADD CONSTRAINT gp_ogr_fid PRIMARY KEY CLUSTERED (ogr_fid) 

--SET THE SHAPES TO BE NOT NULL SO WE CAN CREATE A SPATIAL INDEX
ALTER TABLE #gp ALTER COLUMN ogr_geometry geometry NOT NULL

--SELECT max(x_coord), min(x_coord), max(y_coord), min(y_coord) from #gp

CREATE SPATIAL INDEX [ix_spatial_gp_ogr_fid] ON #gp
(
    ogr_geometry
) USING  GEOMETRY_GRID
    WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
    CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
;


--JOIN GP TO SUBPARCEL
IF OBJECT_ID('tempdb..#gp_sparcels') IS NOT NULL
    DROP TABLE #gp_sparcels
;
SELECT
	parcelID AS parcel_id
	,LCKey
	,year
	,sphere
	,planid
	,gplu
INTO #gp_sparcels
FROM gis.ludu2015points AS sp
JOIN #gp AS gp
	ON sp.centroid.STIntersects(gp.ogr_geometry) = 1
ORDER BY gplu, parcelID, LCKey
--SELECT * FROM #gp_sparcels ORDER BY parcel_id, LCKey
--ALL = 830,083 of 829,868	--XX

--FIND DUPLICATES ON LCKEY
WITH r AS(
	SELECT 
		parcel_id
		,LCKey
		,year
		,sphere
		,ROW_NUMBER() OVER(PARTITION BY LCKey ORDER BY LCKey) AS rownum
	FROM #gp_sparcels
	--ORDER BY rownumber DESC
)
SELECT
	parcel_id
	,LCKey
	,year
	,sphere
	,rownum
FROM r
WHERE rownum > 1
ORDER BY parcel_id, LCKey
--15,144 DUPS	--XX


/****** SITEID TO PARCEL TABLE ******/
--AGGREGATE TO PARCEL
IF OBJECT_ID('tempdb..#gp_parcels') IS NOT NULL
    DROP TABLE #gp_parcels
;
SELECT
	parcel_id
	,year
	,sphere
	,planid
	,gplu
INTO #gp_parcels
FROM #gp_sparcels
GROUP BY 
	parcel_id
	,year
	,sphere
	,planid
	,gplu
ORDER BY 
parcel_id
	,year
	,sphere
	,planid
	,gplu
;
--CREATE NONCLUSTERED INDEX
CREATE NONCLUSTERED INDEX [ix_gp_parcels_ogr_fid]   
    ON #gp_parcels (parcel_id);  
--816,944 FROM 830,083	--XX
--CHECK
--SELECT * FROM #gp_sparcels WHERE parcel_id = 9002468 ORDER BY LCKey
--SELECT * FROM #gp_parcels WHERE parcel_id = 9002468

--FIND DUPLICATES ON PARCELID
--LOOK AT AGGREGATION, SAME PARCEL IN DIFFERENT GPLU
WITH r AS(
	SELECT 
		parcel_id
		,year
		,sphere
		,planid
		,gplu
		,ROW_NUMBER() OVER(PARTITION BY parcel_id ORDER BY parcel_id) AS rownum
	FROM #gp_parcels
	--ORDER BY rownumber DESC
)
SELECT
	r.parcel_id
	,p.year
	,p.sphere
	,p.planid
	,p.gplu
	,ROW_NUMBER() OVER(PARTITION BY p.parcel_id ORDER BY p.parcel_id) AS rownum
FROM r
JOIN #gp_parcels AS p ON r.parcel_id = p.parcel_id
WHERE rownum = 2
ORDER BY parcel_id
;
--35,541 DUPS	--XX


--FIND DUPLICATE PARCELID AND COUNT
WITH r AS(
	SELECT 
		parcel_id
		,year
		,sphere
		,planid
		,gplu
		,ROW_NUMBER() OVER(PARTITION BY parcel_id ORDER BY parcel_id) AS rownum
	FROM #gp_parcels
	--ORDER BY rownumber DESC
)
SELECT
	parcel_id
	,MAX(rownum) AS dups
FROM r
WHERE rownum > 1
GROUP BY parcel_id
ORDER BY parcel_id
--17,522 DUP PARCELIDS	--XX


/*########## OVERRIDES FOR SPHERE OVERLAP 1/3 ##########*/
--FIND 1900 ON 200 OR 1400s
SELECT parcel_id
	,year
	,sphere
	,planid
	,gplu
FROM #gp_parcels
WHERE sphere >= 1900
AND parcel_id IN (SELECT parcel_id FROM #gp_parcels WHERE sphere = 200 OR sphere BETWEEN 1400 AND 1499) 
ORDER BY parcel_id, sphere, planid
--4,621

--CHECK
SELECT * FROM #gp_parcels WHERE parcel_id = 9002468

--DELETE
DELETE
FROM #gp_parcels
WHERE sphere >= 1900
AND parcel_id IN (SELECT parcel_id FROM #gp_parcels WHERE sphere = 200 OR sphere BETWEEN 1400 AND 1499) 


/*########## OVERRIDES FOR SPHERE OVERLAP 2/3 ##########*/
--FIND 200 ON 1400s
SELECT parcel_id
	,year
	,sphere
	,planid
	,gplu
FROM #gp_parcels
WHERE sphere = 200
AND parcel_id IN (SELECT parcel_id FROM #gp_parcels WHERE sphere BETWEEN 1400 AND 1499) 
ORDER BY parcel_id, sphere, planid
--801

--CHECK
SELECT * FROM #gp_parcels WHERE parcel_id = 600

--DELETE
DELETE
FROM #gp_parcels
WHERE sphere = 200
AND parcel_id IN (SELECT parcel_id FROM #gp_parcels WHERE sphere BETWEEN 1400 AND 1499)


/*########## OVERRIDES FOR SPHERE OVERLAP 3/3 ##########*/
--FIND ALL OTHER JURISDICTION
--DELETE
DELETE
FROM #gp_parcels
WHERE sphere = 100
AND parcel_id IN (SELECT parcel_id FROM #gp_parcels WHERE sphere = 1200)

--DELETE
DELETE
FROM #gp_parcels
WHERE sphere = 300
AND parcel_id IN (SELECT parcel_id FROM #gp_parcels WHERE sphere = 200)

--DELETE
DELETE
FROM #gp_parcels
WHERE sphere = 600
AND parcel_id IN (SELECT parcel_id FROM #gp_parcels WHERE sphere = 1700)

--DELETE
DELETE
FROM #gp_parcels
WHERE sphere = 700
AND parcel_id IN (SELECT parcel_id FROM #gp_parcels WHERE sphere BETWEEN 1400 AND 1499 OR sphere = 1500)

--DELETE
DELETE
FROM #gp_parcels
WHERE sphere = 800
AND parcel_id IN (SELECT parcel_id FROM #gp_parcels WHERE sphere BETWEEN 1400 AND 1499)

--DELETE
DELETE
FROM #gp_parcels
WHERE sphere = 900
AND parcel_id IN (SELECT parcel_id FROM #gp_parcels WHERE sphere BETWEEN 1400 AND 1499)

--DELETE
DELETE
FROM #gp_parcels
WHERE sphere = 1000
AND parcel_id IN (SELECT parcel_id FROM #gp_parcels WHERE sphere = 900 OR sphere BETWEEN 1400 AND 1499)

--DELETE
DELETE
FROM #gp_parcels
WHERE sphere = 1100
AND parcel_id IN (SELECT parcel_id FROM #gp_parcels WHERE sphere = 200 OR sphere BETWEEN 1400 AND 1499)

--DELETE
DELETE
FROM #gp_parcels
WHERE sphere = 1200
AND parcel_id IN (SELECT parcel_id FROM #gp_parcels WHERE sphere = 1800)

--DELETE
DELETE
FROM #gp_parcels
WHERE sphere = 1300
AND parcel_id IN (SELECT parcel_id FROM #gp_parcels WHERE sphere BETWEEN 1400 AND 1499)

--DELETE
DELETE
FROM #gp_parcels
WHERE sphere BETWEEN 1400 AND 1499
AND parcel_id IN (SELECT parcel_id FROM #gp_parcels WHERE sphere = 400)

--DELETE
DELETE
FROM #gp_parcels
WHERE sphere = 1600
AND parcel_id IN (SELECT parcel_id FROM #gp_parcels WHERE sphere = 500 OR sphere BETWEEN 1400 AND 1499)

--DELETE
DELETE
FROM #gp_parcels
WHERE sphere = 1700
AND parcel_id IN (SELECT parcel_id FROM #gp_parcels WHERE sphere = 400 OR sphere BETWEEN 1400 AND 1499)

--DELETE
DELETE
FROM #gp_parcels
WHERE sphere BETWEEN 1900 AND 1999
AND parcel_id IN (SELECT parcel_id FROM #gp_parcels WHERE sphere IN(500, 600, 700, 900, 1000, 1100, 1200, 1300, 1500, 1600, 1800))


/*########## OVERRIDES FOR YEAR OVERLAP (SPHERE 1100) ##########*/
--FIND
SELECT parcel_id
	,year
	,sphere
	,planid
	,gplu
FROM #gp_parcels
WHERE sphere = 1100
AND year = 2013
AND parcel_id IN (SELECT parcel_id FROM #gp_parcels WHERE sphere = 1100 AND year = 2017)
ORDER BY parcel_id, year, sphere, planid
--7,849 of 18,906

--CHECK
SELECT * FROM #gp_parcels WHERE parcel_id = 190

--DELETE
DELETE
FROM #gp_parcels
WHERE sphere = 1100
AND year = 2013
AND parcel_id IN (SELECT parcel_id FROM #gp_parcels WHERE sphere = 1100 AND year = 2017)


/*########## SAVE TO TABLE ##########*/
IF OBJECT_ID('urbansim.general_plan_parcels') IS NOT NULL
    DROP TABLE urbansim.general_plan_parcels
;
SELECT 
	IDENTITY(int,1,1) AS gpid
	,parcel_id
	,year
	,sphere
	,planid
	,gplu
INTO urbansim.general_plan_parcels
FROM #gp_parcels
ORDER BY parcel_id
;
CREATE INDEX ix_urbansim_general_plan_parcels_parcel_id
ON urbansim.general_plan_parcels (parcel_id, sphere);
--802, 740

--CHECK
SELECT * FROM spacecore.urbansim.general_plan_parcels --WHERE parcel_id = xxx;


--*****************************************************************************
--CHECKS
--FIND DUPLICATE PARCELID BY SPHERE AND COUNT
WITH p AS(
	SELECT 
		parcel_id
		,sphere
		,COUNT(*) AS _count
	FROM #gp_parcels
	GROUP BY parcel_id
		,sphere
	--ORDER BY parcel_id
	--ORDER BY rownumber DESC
)
,s AS(
	SELECT
		parcel_id
		,sphere
		,ROW_NUMBER() OVER(PARTITION BY parcel_id ORDER BY parcel_id) AS rownum
	FROM p
)
SELECT
	p.parcel_id
	,p.sphere
	,_count
	--,rownum
FROM p
JOIN (SELECT DISTINCT parcel_id FROM s WHERE rownum > 1) AS s ON p.parcel_id = s.parcel_id
--WHERE p.parcel_id = 9002391
ORDER BY p.parcel_id, sphere
--14,287 DUP PARCELIDS in DIFFERENT SPHERE	--XX
--SELECT * FROM #gp_parcels WHERE parcel_id = 9002468--9002391



--FIND DUPLICATE PARCELID BY JURISDICTION AND COUNT
WITH p AS(
	SELECT 
		parcel_id
		,CASE LEN(sphere)
			WHEN 3 THEN LEFT(sphere, 1)
			WHEN 4 THEN LEFT(sphere, 2)
			END AS j_id
		,COUNT(*) AS _count
	FROM #gp_parcels
	GROUP BY parcel_id
		,CASE LEN(sphere)
			WHEN 3 THEN LEFT(sphere, 1)
			WHEN 4 THEN LEFT(sphere, 2)
			END
	--ORDER BY parcel_id
	--ORDER BY rownumber DESC
)
,s AS(
	SELECT
		parcel_id
		,j_id
		,ROW_NUMBER() OVER(PARTITION BY parcel_id ORDER BY parcel_id) AS rownum
	FROM p
)
SELECT
	p.parcel_id
	,p.j_id
	,_count
	--,rownum
FROM p
JOIN (SELECT DISTINCT parcel_id FROM s WHERE rownum > 1) AS s ON p.parcel_id = s.parcel_id
--WHERE p.parcel_id = 9002391
ORDER BY p.parcel_id, j_id
--12,519 DUP PARCELIDS in DIFFERENT JURISDICTION	--XX

--SELECT * FROM #gp_parcels WHERE parcel_id BETWEEN 600 AND 610--9002468--9002391



--FIND DUPLICATE PARCELID BY JURISDICTION AND IDENTIFY JURISDICTION ID
WITH p AS(
	SELECT 
		parcel_id
		,CASE LEN(sphere)
			WHEN 3 THEN LEFT(sphere, 1)
			WHEN 4 THEN LEFT(sphere, 2)
			END AS j_id
		,COUNT(*) AS _count
	FROM #gp_parcels
	GROUP BY parcel_id
		,CASE LEN(sphere)
			WHEN 3 THEN LEFT(sphere, 1)
			WHEN 4 THEN LEFT(sphere, 2)
			END
	--ORDER BY parcel_id
	--ORDER BY rownumber DESC
)
,s AS(
	SELECT
		parcel_id
		,j_id
		,ROW_NUMBER() OVER(PARTITION BY parcel_id ORDER BY parcel_id) AS rownum
	FROM p
)
,x AS(
	SELECT
		p.parcel_id
		,p.j_id
		,_count
	FROM p
	JOIN (SELECT DISTINCT parcel_id FROM s WHERE rownum > 1) AS s ON p.parcel_id = s.parcel_id
)
SELECT parcel_id
	,MAX(j_id) AS j1
	,MIN(j_id) AS j2
FROM x
GROUP BY parcel_id
ORDER BY parcel_id


