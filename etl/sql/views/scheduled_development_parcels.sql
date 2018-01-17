/****** LOAD FROM GDB TO MSSQL ******/
--USE OGR2OGR
/*
E:\OSGeo4W64\bin\ogr2ogr.exe -f MSSQLSpatial "MSSQL:server=sql2014a8;database=spacecore;trusted_connection=yes" E:\data\urbansim_data_development\scheduled_development\Site_ID\SchedDev_SR13input.shp -nln scheduled_development -lco SCHEMA=gis -lco OVERWRITE=YES -OVERWRITE
*/

USE spacecore

/*
/****** FIX SRID  ******/
SELECT DISTINCT centroid.STSrid FROM gis.ludu2015points;
SELECT DISTINCT ogr_geometry.STSrid FROM GIS.scheduled_development;
UPDATE GIS.scheduled_development SET ogr_geometry.STSrid = 2230;		--NAD83 / California zone 6 (ftUS)
*/


/****** SUBPARCELIDS WITH CENTROID IN SITEID ******/
IF OBJECT_ID('tempdb..#sched_dev_sparcels') IS NOT NULL
    DROP TABLE #sched_dev_sparcels
;
SELECT
	parcelID AS parcel_id
	,LCKey
	,lu
	,plu
	,siteid
	--,[sitename]
	--,[totalsqft]
	--,centroid
INTO #sched_dev_sparcels
FROM gis.ludu2015points AS sp
JOIN GIS.scheduled_development_sites AS sds
	ON sp.centroid.STIntersects(sds.ogr_geometry) = 1
ORDER BY siteid, parcelID, LCKey
--ALL = 5,244
--SEE BELOW FOR DIFFERENT METHOD ON JOINING SITES TO PARCELS

/****** SITEID TO PARCEL TABLE ******/
IF OBJECT_ID('tempdb..#sched_dev_parcels') IS NOT NULL
    DROP TABLE #sched_dev_parcels
;
SELECT siteid
	,parcel_id
INTO #sched_dev_parcels
FROM #sched_dev_sparcels
GROUP BY siteid, parcel_id
-- = 4,917
--TEST
--SELECT * FROM #sched_dev_parcels

--SAVE TO TABLE
IF OBJECT_ID('urbansim.scheduled_development_parcels') IS NOT NULL
    DROP TABLE urbansim.scheduled_development_parcels
;
SELECT sdp.siteid AS site_id
	,parcel_id
	--,lu
	--,plu
	,startdate
	,compdate
	,civemp
	,milemp
	,sfu
	,mfu
	,mhu
INTO urbansim.scheduled_development_parcels
FROM #sched_dev_parcels AS sdp
JOIN GIS.scheduled_development_sites AS sds ON sdp.siteid = sds.siteid
ORDER BY site_id, parcel_id

--CHECK
SELECT * FROM urbansim.scheduled_development_parcels WHERE parcel_id = 5293131;


/****** CHECK DUPLICATE PARCELID IN MULTIPLE SITEID ******/
WITH d AS(
	SELECT parcel_id
		,ROW_NUMBER() OVER(PARTITION BY parcel_id ORDER BY parcel_id) AS rownum
	FROM urbansim.scheduled_development_parcels
)
SELECT sdp.parcel_id
	,sdp.site_id
	--,sds.devtypeid																		--LOOK AT LU
	--,dev.development_type_id AS devtypeid_lu											--LOOK AT LU
	--,devp.development_type_id AS devtypeid_plu											--LOOK AT LU
	--,s.lu																				--LOOK AT LU
	--,s.plu																				--LOOK AT LU
	--,s.LCKey																			--LOOK AT LU
FROM urbansim.scheduled_development_parcels AS sdp
JOIN d ON sdp.parcel_id = d.parcel_id
JOIN gis.scheduled_development_sites AS sds ON sdp.site_id = sds.siteid
--JOIN #sched_dev_sparcels AS s ON sdp.parcel_id = s.parcel_id AND sdp.site_id = s.siteid		--LOOK AT LU
--JOIN ref.development_type_lu_code AS dev ON s.lu = dev.lu_code							--LOOK AT LU
--JOIN ref.development_type_lu_code AS devp ON s.plu = devp.lu_code						--LOOK AT LU
WHERE rownum = 2
ORDER BY sdp.parcel_id
	,sdp.site_id



/****** UPDATE PARCELS DATASET  ******/
UPDATE usp
SET usp.site_id = sdp.site_id
FROM urbansim.parcels AS usp
JOIN urbansim.scheduled_development_parcels AS sdp ON usp.parcel_id = sdp.parcel_id

--CHECK
SELECT *
FROM urbansim.parcels
WHERE site_id IS NOT NULL
ORDER BY site_id, parcel_id
;

/****** UPDATE CAPACITY DATASET  ******/
UPDATE usc
SET usc.site_id = sdp.site_id
FROM urbansim.capacity AS usc
JOIN urbansim.scheduled_development_parcels AS sdp ON usc.parcel_id = sdp.parcel_id
--NOTE:CAPACITY IS NOT A COMPLETE PARCELS SET.

--CHECK
SELECT *
FROM urbansim.capacity
WHERE site_id IS NOT NULL
ORDER BY site_id, parcel_id
;


--DROP TEMP TABLES
DROP TABLE #sched_dev_sparcels
DROP TABLE #sched_dev_parcels


--CHECK FOR EMPTY SITE IDS
SELECT sds.siteid AS site_id_SITE
	,usp.site_id AS site_id_PARCEL
FROM GIS.scheduled_development_sites AS sds
FULL OUTER JOIN (	SELECT site_id
		FROM urbansim.parcels
		WHERE site_id IS NOT NULL
		GROUP BY site_id) AS usp
	ON sds.siteid = usp.site_id
WHERE usp.site_id IS NULL OR sds.siteid IS NULL
ORDER BY usp.site_id
	,sds.siteid


/*****************************************************************************************/
/****** MAP CHECKS ******/
SELECT *
FROM urbansim.parcels
WHERE parcel_id = 5051025

SELECT *
FROM urbansim.parcels
WHERE site_id = 1094
--WHERE site_id IN(1093, 1094)
--ORDER BY site_id
--	,parcel_id


SELECT site_id
	,parcel_id
FROM urbansim.parcels
WHERE site_id IN(3364, 14077)
ORDER BY site_id
	,parcel_id
