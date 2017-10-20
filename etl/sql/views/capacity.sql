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
;

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

--DELETE DUPLICATE PARCELS
DELETE 
FROM #sr14draft_capacity_citycnty_ludu15 
WHERE parcelid IN (SELECT parcel_id FROM [ref].[sr14_capacity_from_feedback])

--CREATE CAPACITY TABLE
IF OBJECT_ID('urbansim.capacity') IS NOT NULL
    DROP TABLE urbansim.capacity
;

CREATE TABLE urbansim.capacity (
	capacity_id int IDENTITY(1, 1) NOT NULL,
	parcel_id int NOT NULL,
	cap_jurisdiction_id int NOT NULL,
	capacity int NULL,
	cap_max int NULL,
	cap_source nvarchar(50),
	cap_note nvarchar(100),
	site_id int NULL
);

--LOAD INTO TEMP TABLE
SELECT --TOP (1000) 
	parcel_id
	,jurisdicti AS cap_jurisdiction_id
	,sr14_cap AS capacity
	,NULL AS cap_max
	,cap_source
	,notes AS cap_note
	,site_id
	--,Cap_Type
	--,Feed_Type
	--,sr13_cap_hs_growth_adjusted
	--,zoning
	--,New_Cap
INTO #capacity
FROM [spacecore].[ref].[sr14_capacity_from_feedback]
UNION
SELECT --TOP 1000
	parcelid
	,city
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

DROP TABLE #sr14draft_capacity_citycnty_ludu15;					--DROP #TEMP TABLE

--LOAD INTO CAPACITY TABLE
INSERT INTO urbansim.capacity (
	parcel_id
	,cap_jurisdiction_id
	,capacity
	,cap_max
	,cap_source
	,cap_note
	,site_id
)
SELECT
	parcel_id
	,cap_jurisdiction_id
	,capacity
	,cap_max
	,cap_source
	,cap_note
	,site_id
FROM #capacity;

DROP TABLE #capacity											--DROP #TEMP TABLE


/******   UPDATE PARCEL TABLE WITH CAPACITY   ******/
--DROP COLUMNS
--ALTER TABLE urbansim.parcels
--DROP COLUMN capacity
--	,cap_max
--	,cap_jurisdiction_id
--	,cap_source
--	,cap_note
--	,site_id
--;
--ADD COLUMNS
ALTER TABLE urbansim.parcels
ADD capacity int NULL
	,cap_max int NULL
	,cap_jurisdiction_id int NULL
	,cap_source nvarchar(50)
	,cap_note nvarchar(100)
	,site_id int NULL
;

--UPDATE NEW COLUMNS
UPDATE p
SET p.capacity = c.capacity
	,p.cap_max = c.cap_max
	,p.cap_jurisdiction_id = c.cap_jurisdiction_id
	,p.cap_source = c.cap_source
	,p.cap_note = c.cap_note
	,p.site_id = c.site_id
FROM urbansim.parcels AS p
JOIN urbansim.capacity AS c
	ON p.parcel_id = c.parcel_id
;

--CALCULATE CAPMAX								--xxFOR JURISDICTIONS?
UPDATE p
SET cap_max = du + capacity
FROM urbansim.parcels AS p
--WHERE cap_jurisdiction_id NOT IN (14, 19)
;

--CHECK CAPMAX
SELECT p.parcel_id
	,c.cap_jurisdiction_id
	,p.capacity
	,p.du
	,p.cap_max AS cap_max_p
	,c.cap_max AS cap_max_c
FROM urbansim.parcels AS p
JOIN urbansim.capacity AS c ON p.parcel_id = c.parcel_id
WHERE c.cap_jurisdiction_id IN (14, 19)
--AND p.cap_max <> c.cap_max
ORDER BY c.cap_jurisdiction_id
;

--CHECKS
SELECT SUM(capacity) FROM urbansim.capacity
SELECT SUM(capacity) FROM urbansim.parcels

SELECT MIN(capacity) FROM urbansim.capacity
SELECT MIN(capacity) FROM urbansim.parcels

SELECT * FROM urbansim.capacity ORDER BY capacity

SELECT cap_jurisdiction_id
      ,SUM([capacity])
  FROM [spacecore].[urbansim].[capacity]
  GROUP BY cap_jurisdiction_id
  ORDER BY cap_jurisdiction_id

SELECT
	parcel_id
	,SUM(sr14_cap)
	,SUM(sr14caphs)
FROM [ref].[sr14_capacity_from_feedback] AS a 
JOIN #sr14draft_capacity_citycnty_ludu15 AS b on a.parcel_id = b.parcelid
GROUP BY parcel_id
