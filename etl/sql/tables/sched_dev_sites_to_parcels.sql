/****** LOAD FROM GDB TO MSSQL ******/
--USE OGR2OGR
/*
E:\OSGeo4W64\bin\ogr2ogr.exe -f MSSQLSpatial "MSSQL:server=sql2014a8;database=spacecore;trusted_connection=yes" E:\data\urbansim_data_development\scheduled_development\Site_ID\SchedDev_SR14input.shp -nln scheduled_development_sites -lco SCHEMA=gis -lco OVERWRITE=YES -OVERWRITE
*/

USE spacecore
;
/*
/****** FIX SRID  ******/
SELECT DISTINCT centroid.STSrid FROM gis.ludu2015points;
SELECT DISTINCT ogr_geometry.STSrid FROM GIS.scheduled_development_sites;
UPDATE GIS.scheduled_development_sites SET ogr_geometry.STSrid = 2230;		--NAD83 / California zone 6 (ftUS)
*/

--CHECK FOR CONSECUTIVE ID 'ogr_fid' OR ADD
--ALTER TABLE GIS.scheduled_development_sites
--ADD id int IDENTITY(1, 1)
--;


/****** SUBPARCELIDS WITH CENTROID IN SITEID ******/
DROP TABLE IF EXISTS #sched_dev_sparcels
;
SELECT
	parcelID AS parcel_id
	,LCKey
	,lu
	,plu
	,ogr_fid
	,siteid
	--,[sitename]
	--,[totalsqft]
	--,centroid
INTO #sched_dev_sparcels
FROM gis.ludu2015points AS sp
JOIN GIS.scheduled_development_sites AS sds
	ON sp.centroid.STIntersects(sds.ogr_geometry) = 1
ORDER BY siteid, parcelID, LCKey
--ALL = 4,522
--TEST
--SELECT * FROM #sched_dev_sparcels


/****** SITEID TO PARCEL TABLE ******/
DROP TABLE IF EXISTS #sched_dev_parcels
;
SELECT
	ogr_fid
	,siteid
	,parcel_id
INTO #sched_dev_parcels
FROM #sched_dev_sparcels
GROUP BY ogr_fid, siteid, parcel_id
-- = 4,321
--TEST
--SELECT * FROM #sched_dev_parcels

--SAVE TO TABLE
DROP TABLE IF EXISTS staging.sched_dev_sites_to_parcels
;
SELECT
	sdp.ogr_fid
	,sdp.siteid AS site_id
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
INTO staging.sched_dev_sites_to_parcels
FROM #sched_dev_parcels AS sdp
LEFT OUTER JOIN GIS.scheduled_development_sites AS sds ON sdp.ogr_fid = sds.ogr_fid AND sdp.siteid = sds.siteid
ORDER BY site_id, parcel_id
--4,321

--CHECK
SELECT * FROM gis.scheduled_development_sites WHERE siteid = 15035	--15017;
SELECT * FROM staging.sched_dev_sites_to_parcels WHERE site_id = 15035	--15017;


/****** CHECK DUPLICATE PARCELID IN MULTIPLE SITEID ******/
WITH d AS(
	SELECT parcel_id
		,ROW_NUMBER() OVER(PARTITION BY parcel_id ORDER BY parcel_id) AS rownum
	FROM staging.sched_dev_sites_to_parcels
)
SELECT sdp.parcel_id
	,sdp.ogr_fid
	,sdp.site_id
	,sds.*
	--,sds.devtypeid																		--LOOK AT LU
	--,dev.development_type_id AS devtypeid_lu											--LOOK AT LU
	--,devp.development_type_id AS devtypeid_plu											--LOOK AT LU
	--,s.lu																				--LOOK AT LU
	--,s.plu																				--LOOK AT LU
	--,s.LCKey																			--LOOK AT LU
FROM staging.sched_dev_sites_to_parcels AS sdp
JOIN d ON sdp.parcel_id = d.parcel_id
JOIN gis.scheduled_development_sites AS sds ON sdp.ogr_fid = sds.ogr_fid AND sdp.site_id = sds.siteid
--JOIN #sched_dev_sparcels AS s ON sdp.parcel_id = s.parcel_id AND sdp.site_id = s.siteid		--LOOK AT LU
--JOIN ref.development_type_lu_code AS dev ON s.lu = dev.lu_code							--LOOK AT LU
--JOIN ref.development_type_lu_code AS devp ON s.plu = devp.lu_code						--LOOK AT LU
WHERE rownum = 2
ORDER BY sdp.parcel_id
	,sdp.site_id
--DUPLICATES
--parcel_id = 355207	site_id = 14091, 14101
--parcel_id = 523174	site_id = 14095, 14098

--MANUAL FIX
DELETE FROM staging.sched_dev_sites_to_parcels
WHERE parcel_id IN(355207, 523174)


/****** UPDATE PARCELS DATASET  ******/
/*
THIS DOES NOT UPDATE CAPACITY VALUE
WITH SCHEDULED DEVELOPMENT UNITS,
SEE SCHEDULED DEVELOPMENT UNITS ALLOCATION SCRIPT
*/
--CLEAR
UPDATE usp
SET usp.site_id = NULL
FROM urbansim.parcels AS usp

--UPDATE
UPDATE usp
SET usp.site_id = sdp.site_id
FROM urbansim.parcels AS usp
JOIN staging.sched_dev_sites_to_parcels AS sdp ON usp.parcel_id = sdp.parcel_id

--CHECK --KEEP MULTIPLES IN MIND
SELECT DISTINCT site_id FROM urbansim.parcels ORDER BY site_id;

SELECT *
FROM urbansim.parcels
WHERE site_id IS NOT NULL
ORDER BY site_id, parcel_id
;

--CHECK --KEEP MULTIPLES IN MIND
SELECT *
FROM urbansim.parcels AS usp
JOIN staging.sched_dev_sites_to_parcels AS sdp ON usp.parcel_id = sdp.parcel_id
--WHERE usp.site_id IS NULL
ORDER BY sdp.site_id, sdp.parcel_id
;




--DROP TEMP TABLES
DROP TABLE #sched_dev_sparcels
DROP TABLE #sched_dev_parcels


--CHECK FOR EMPTY SITE IDS
SELECT sds.siteid AS site_id_SITE
	,usp.site_id AS site_id_PARCEL
FROM GIS.scheduled_development_sites AS sds
FULL OUTER JOIN (SELECT site_id
		FROM urbansim.parcels
		WHERE site_id IS NOT NULL
		GROUP BY site_id) AS usp
	ON sds.siteid = usp.site_id
WHERE usp.site_id IS NULL OR sds.siteid IS NULL
ORDER BY usp.site_id
	,sds.siteid

