USE spacecore

--RUNS ON sql2014a8
--PULLS FROM [ref].[sr14_capacity_from_feedback]
--AND FROM [spacecore].[GIS].[sr14draft_capacity_citycnty_ludu15]


/******   UPLOAD CAPACITY FROM CITYSD, COUNTYSD TO SQL   ******/
/*
LOAD FROM GEODATABASE
M:\RES\GIS\Spacecore\sr14GISinput\LandUseInputs\SR14_CityCountyCapacity.gdb\sr14draft_capacity_citycnty_ludu15

TO SHAPEFILE
E:\data\urbansim_data_development\capacity\sr14draft_capacity_citycnty_ludu15.shp

TO SQL
E:\OSGeo4W64\bin\ogr2ogr.exe -f MSSQLSpatial "MSSQL:server=sql2014a8;database=spacecore;trusted_connection=yes" E:\data\urbansim_data_development\capacity\sr14draft_capacity_citycnty_ludu15.shp -nln sr14draft_capacity_citycnty_ludu15 -lco SCHEMA=gis -lco OVERWRITE=YES -OVERWRITE
*/


--AGGREGATE BY PARCELID INTO TEMP TABLE
IF OBJECT_ID('tempdb..#sr14draft_capacity_citycnty_ludu15') IS NOT NULL
    DROP TABLE #sr14draft_capacity_citycnty_ludu15
GO;

SELECT
	parcelid
	,MIN(apn8) AS apn8
	,MIN(city) AS city
	,SUM(du) AS du
	,SUM(gq) AS gq
	,SUM(sr14capmax) AS sr14capmax							-->>CHECK IF SUM OR MAX?
	,SUM(sr14caphs) AS 	sr14caphs							-->>CHECK IF SUM OR MAX?
	,MIN(capsource) AS capsource							-->>UNIQUE BY PARCEL
	,MIN(capnote) AS capnote								-->>UNIQUE BY PARCEL
	--,geometry::UnionAggregate(ogr_geometry) AS shape		-->>NEEDED?
INTO #sr14draft_capacity_citycnty_ludu15								--LOAD INTO #TEMP TABLE
FROM [spacecore].[GIS].[sr14draft_capacity_citycnty_ludu15]
GROUP BY parcelid
;
--SELECT * FROM #sr14draft_capacity_citycnty_ludu15


/******   JOIN CAPACITY FROM CITYSD, COUNTYSD TO CAPACITY FROM FEEDBACK   ******/
--CHECK
--SELECT COUNT(*) FROM [ref].[sr14_capacity_from_feedback]
--SELECT COUNT(*) FROM #sr14draft_capacity_citycnty_ludu15
--SELECT COUNT(*) FROM [ref].[sr14_capacity_from_feedback] AS a JOIN #sr14draft_capacity_citycnty_ludu15 AS b on a.parcel_id = b.parcelid

IF OBJECT_ID('urbansim.capacity') IS NOT NULL
    DROP TABLE urbansim.capacity
GO;

SELECT --TOP (1000) 
	parcel_id
	,jurisdicti AS jurisdiction_id
	,sr14_cap AS capacity
	,NULL AS cap_max
	,cap_source
	,notes AS cap_note
	,SiteID AS site_id
	--,Cap_Type
	--,Feed_Type
	--,sr13_cap_hs_growth_adjusted
	--,zoning
	--,New_Cap
INTO urbansim.capacity
FROM [spacecore].[staging].[sr14_capacity]
UNION
SELECT --TOP 1000
	parcelid
	,city													-->>CHECK JURISDICTION ID 14, 18?
	,sr14caphs
	,sr14capmax
	,capsource
	,capnote
	,NULL AS site_id
	--,apn8
	--,du
	--,gq
FROM #sr14draft_capacity_citycnty_ludu15
ORDER BY parcel_id
;

DROP TABLE #sr14draft_capacity_citycnty_ludu15;							--DROP #TEMP TABLE

