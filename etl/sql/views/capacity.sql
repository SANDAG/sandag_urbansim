USE spacecore
;
--RUNS ON sql2014a8
--PULLS FROM [ref].[sr14_capacity_from_feedback]
--AND FROM [spacecore].[GIS].[sr14draft_capacity_citycnty_ludu15]


/******   UPLOAD CAPACITY FROM CITYSD, COUNTYSD TO SQL   ******/
/*
LOAD FROM GEODATABASE
M:\RES\GIS\Spacecore\sr14GISinput\LandUseInputs\SR14_CityCountyCapacity.gdb\sr14draft_capacity_city_ludu15_mmddyyyy			--CITY
M:\RES\GIS\Spacecore\sr14GISinput\LandUseInputs\SR14_CityCountyCapacity.gdb\sr14draft_capacity_cnty_ludu15_mmddyyyy			--COUNTY

TO SHAPEFILE
E:\data\urbansim_data_development\capacity\sr14draft_capacity_city_ludu15.shp								--CITY
E:\data\urbansim_data_development\capacity\sr14draft_capacity_cnty_ludu15.shp								--COUNTY

TO SQL
--CITY
E:\OSGeo4W64\bin\ogr2ogr.exe -f MSSQLSpatial "MSSQL:server=sql2014a8;database=spacecore;trusted_connection=yes" E:\data\urbansim_data_development\capacity\sr14draft_capacity_city_ludu15.shp -nln sr14draft_capacity_city_ludu15 -lco SCHEMA=gis -lco OVERWRITE=YES -OVERWRITE
--COUNTY
E:\OSGeo4W64\bin\ogr2ogr.exe -f MSSQLSpatial "MSSQL:server=sql2014a8;database=spacecore;trusted_connection=yes" E:\data\urbansim_data_development\capacity\sr14draft_capacity_cnty_ludu15.shp -nln sr14draft_capacity_cnty_ludu15 -lco SCHEMA=gis -lco OVERWRITE=YES -OVERWRITE
*/

--INSERT CITY AND COUNTY INTO STAGING TABLE
DROP TABLE IF EXISTS #sr14draft_capacity_citycnty_ludu15_load
;
SELECT
	[parcelid]
	,[apn8]
	,[city]
	,[du] AS du_2015
	,[gq]
	,[sr14capmax]
	,[sr14caphs]
	,[capsource]
	,NULL AS capnote
INTO #sr14draft_capacity_citycnty_ludu15_load
FROM [spacecore].[GIS].[sr14draft_capacity_city_ludu15]
UNION ALL
SELECT
	[parcelid]
	,[apn8]
	,[city]
	,[du] AS du_2015
	,NULL AS [gq]
	,NULL AS [sr14capmax]
	,[sr14caph_2] AS [sr14caphs]		--sr14caphsv3
	,capsource
	,capnote
FROM [spacecore].[GIS].[sr14draft_capacity_cnty_ludu15]
--182,644 + 309,371 = 492,015
SELECT * FROM #sr14draft_capacity_citycnty_ludu15_load


--AGGREGATE BY PARCELID INTO TEMP TABLE
DROP TABLE IF EXISTS #sr14draft_capacity_citycnty_ludu15
;
SELECT
	parcelid
	,MIN(apn8) AS apn8
	,MAX(city) AS city									-->>CHECK IF SUM OR MAX? THIS IS AFFECTED BY DUPLICATE PARCELS
	,SUM(du_2015) AS du_2015
	,SUM(gq) AS gq
	,SUM(sr14capmax) AS sr14capmax							-->>CHECK IF SUM OR MAX?
	,SUM(sr14caphs) AS 	sr14caphs							-->>CHECK IF SUM OR MAX?
	,MIN(capsource) AS capsource							-->>UNIQUE BY PARCEL
	,MIN(capnote) AS capnote								-->>UNIQUE BY PARCEL
	--,geometry::UnionAggregate(ogr_geometry) AS shape		-->>NEEDED?
INTO #sr14draft_capacity_citycnty_ludu15								--LOAD INTO #TEMP TABLE
FROM #sr14draft_capacity_citycnty_ludu15_load
GROUP BY parcelid
;
--RETURN
SELECT city, SUM(sr14caphs)
FROM #sr14draft_capacity_citycnty_ludu15
GROUP BY city

/******   JOIN CAPACITY FROM CITYSD, COUNTYSD TO CAPACITY FROM FEEDBACK   ******/
--CHECK
--SELECT COUNT(*) FROM [ref].[sr14_capacity_from_feedback]
--SELECT COUNT(*) FROM #sr14draft_capacity_citycnty_ludu15
--SELECT COUNT(*) FROM [ref].[sr14_capacity_from_feedback] AS a JOIN #sr14draft_capacity_citycnty_ludu15 AS b on a.parcel_id = b.parcelid

--DELETE DUPLICATE PARCELS
DELETE
--SELECT COUNT(*), SUM(sr14caphs)	--*
FROM #sr14draft_capacity_citycnty_ludu15 
WHERE parcelid IN (SELECT parcel_id FROM [ref].[sr14_capacity_from_feedback])
--60 records	612 sr14caphs


--LOAD INTO TEMP TABLE
SELECT --TOP (1000) 
	parcel_id
	,jurisdicti AS cap_jurisdiction_id
	,sr14_cap AS capacity_1
	--,NULL AS max_res_units
	,cap_source
	,notes AS cap_note
	--,site_id
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
	--,sr14capmax								--WILL BE RECALCULATED LATER
	,capsource
	,capnote
	--,NULL AS site_id
	--,apn8
	--,du_2015
	--,gq
FROM #sr14draft_capacity_citycnty_ludu15
ORDER BY parcel_id
;
--SELECT * FROM #capacity WHERE cap_jurisdiction_id = 19

DROP TABLE #sr14draft_capacity_citycnty_ludu15_load;			--DROP #TEMP TABLE
DROP TABLE #sr14draft_capacity_citycnty_ludu15;					--DROP #TEMP TABLE


/******   UPDATE PARCEL TABLE WITH CAPACITY   ******/
--DROP COLUMNS
--ALTER TABLE urbansim.parcels
--DROP COLUMN capacity
--	,max_res_units
--	,cap_jurisdiction_id
--	,cap_source
--	,cap_note
--	,site_id
--;
--ADD COLUMNS
ALTER TABLE urbansim.parcels
ADD capacity_1 int NULL
	,max_res_units int NULL
	,cap_jurisdiction_id int NULL
	,cap_source nvarchar(50)
	,cap_note nvarchar(100)
	,site_id int NULL
;

--CLEAR NEW COLUMNS, IF NEEDED
UPDATE urbansim.parcels
SET capacity_1 = NULL
	,max_res_units = NULL
	,cap_jurisdiction_id = NULL
	,cap_source = NULL
	,cap_note = NULL
	--,p.site_id = NULL							--SITE ID WILL COME IN FROM SCHEDULED DEVELOPMENT
;

--UPDATE NEW COLUMNS
UPDATE p
SET p.capacity_1 = ISNULL(c.capacity_1, 0)
	--,p.max_res_units = ISNULL(c.max_res_units, 0)
	,p.cap_jurisdiction_id = COALESCE(c.cap_jurisdiction_id, p.jurisdiction_id)
	,p.cap_source = c.cap_source
	,p.cap_note = c.cap_note
	--,p.site_id = c.site_id					--SITE ID WILL COME IN FROM SCHEDULED DEVELOPMENT
FROM urbansim.parcels AS p	
LEFT JOIN #capacity AS c
	ON p.parcel_id = c.parcel_id
;

--RETURN
SELECT *
FROM urbansim.parcels
WHERE jurisdiction_id NOT IN (14, 19)
ORDER BY cap_jurisdiction_id


/******   UPDATE PARCEL TABLE WITH DU_2017 TO CALCULATE REMAINING CAPACITY   ******/
--ADD COLUMNS DU_2017, REMAINING_CAP
ALTER TABLE spacecore.urbansim.parcels
ADD [du_2017] int NULL
	,capacity_2 int NULL
--COPY VALUES
UPDATE usp
SET
	usp.du_2017 = pu17.du_2017
FROM spacecore.urbansim.parcels AS usp
RIGHT JOIN [urbansim].[urbansim].[parcel_update_2017] AS pu17
	ON usp.parcel_id = pu17.parcelid_2015
;
/******----------   OVERRIDES   ----------******/
--SchedDev_SR14input04062018_baseyradj
UPDATE urbansim.parcels 
SET
	development_type_id = 21
	,du_2015= 332
	,du_2017 = 332
--WHERE site_id = 1743
WHERE parcel_id = 1476

UPDATE urbansim.parcels 
SET
	development_type_id = 19
	,du_2015= 1
	,du_2017 = 1
--WHERE site_id = 3091
WHERE parcel_id = 209594

UPDATE urbansim.parcels 
SET
	development_type_id = 19
	,du_2015= 1
	,du_2017 = 1
--WHERE site_id = 3201
WHERE parcel_id = 5009857

UPDATE urbansim.parcels 
SET
	--development_type_id = 
	du_2015= 0
	,du_2017 = 0
--WHERE site_id = 3311
WHERE parcel_id = 725795

UPDATE urbansim.parcels 
SET
	--development_type_id = 19
	du_2015= 0
	,du_2017 = 0
--WHERE site_id = 3324
WHERE parcel_id = 5109215
/******----------   OVERRIDES   ----------******/

--UPDATE MAXIMUM RESIDENTIAL UNITS
UPDATE spacecore.urbansim.parcels
SET
	max_res_units =
		CASE 
			WHEN du_2017 > du_2015 + capacity_1 THEN du_2017
			ELSE du_2015 + capacity_1
		END
;

--UPDATE REMAINING CAPACITY
UPDATE spacecore.urbansim.parcels
SET capacity_2 =
		CASE 
			WHEN du_2017 < max_res_units THEN max_res_units - du_2017
			ELSE 0
		END
;




--CHECK CAPMAX
SELECT p.parcel_id
	,c.cap_jurisdiction_id
	,p.capacity_1
	,p.du_2015
	,p.max_res_units AS max_res_units_p
	,c.max_res_units AS max_res_units_c
FROM urbansim.parcels AS p
JOIN #capacity AS c ON p.parcel_id = c.parcel_id
WHERE c.cap_jurisdiction_id IN (14, 19)
--AND p.max_res_units <> c.max_res_units
ORDER BY c.cap_jurisdiction_id
;


DROP TABLE #capacity											--DROP #TEMP TABLE

--*********************************************************************************************************
/********************   CHECKS ********************/
SELECT SUM(capacity_1) FROM #capacity
SELECT SUM(capacity_1) FROM urbansim.parcels

SELECT MIN(capacity_1) FROM #capacity
SELECT MIN(capacity_1) FROM urbansim.parcels

SELECT * FROM #capacity ORDER BY capacity_1

--CAP IN CAPACITY BY JURISDICTION
SELECT cap_jurisdiction_id
      ,SUM([capacity_1]) AS cap
  FROM #capacity
  GROUP BY cap_jurisdiction_id
  ORDER BY cap_jurisdiction_id

--CAP IN PARCEL BY JURISDICTION
SELECT cap_jurisdiction_id
      ,SUM([capacity_1]) AS cap
  FROM [spacecore].[urbansim].[parcels]
  GROUP BY cap_jurisdiction_id
  ORDER BY cap_jurisdiction_id

--OVERLAPPING CAP
SELECT
	parcel_id
	,SUM(sr14_cap)
	,SUM(sr14caphs)
FROM [ref].[sr14_capacity_from_feedback] AS a 
JOIN #sr14draft_capacity_citycnty_ludu15 AS b on a.parcel_id = b.parcelid
GROUP BY parcel_id


/*   CHECK FOR MULTIPLE NOTES ON SAME PARCELID    */
WITH p AS(
	SELECT --TOP 1000
		parcelid
		,capnote
		,COUNT(*) AS count_
	FROM [GIS].[sr14draft_capacity_citycnty_ludu15]
	--WHERE capnote IS NOT NULL								--NULL IS NOT MULTIPLE
	GROUP BY 
		parcelid
		,capnote
)
,pp AS(
	SELECT 
		parcelid
	FROM p
	GROUP BY 
		parcelid
	HAVING COUNT(*) > 1
)
SELECT 
	parcelid
	,capnote
	,count_
FROM p
WHERE parcelid IN(SELECT parcelid FROM pp)
ORDER BY
	parcelid
	,capnote
;
	
/*   CHECK FOR MULTIPLE CAPSOURCE ON SAME PARCELID    */
WITH p AS(
	SELECT --TOP 1000
		parcelid
		,capsource
		,COUNT(*) AS count_
	FROM [GIS].[sr14draft_capacity_citycnty_ludu15]
	--WHERE capsource IS NOT NULL								--NULL IS NOT MULTIPLE
	GROUP BY 
		parcelid
		,capsource
)
,pp AS(
	SELECT 
		parcelid
	FROM p
	GROUP BY 
		parcelid
	HAVING COUNT(*) > 1
)
SELECT 
	parcelid
	,capsource
	,count_
FROM p
WHERE parcelid IN(SELECT parcelid FROM pp)
ORDER BY
	parcelid
	,capsource
