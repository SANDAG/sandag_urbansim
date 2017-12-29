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
	,siteid
	--,[sitename]
	--,[totalsqft]
	--,centroid
INTO #sched_dev_sparcels
FROM gis.ludu2015points AS sp
JOIN GIS.scheduled_development AS sd
	ON sp.centroid.STIntersects(sd.ogr_geometry) = 1
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
--TEST
--SELECT * FROM #sched_dev_parcels

--CHECK
SELECT * FROM #sched_dev_sparcels WHERE parcel_id = 5293131;


/****** CHECK DUPLICATE PARCELID IN MULTIPLE SITEID ******/
WITH d AS(
	SELECT parcel_id
		,ROW_NUMBER() OVER(PARTITION BY parcel_id ORDER BY parcel_id) AS rownum
	FROM #sched_dev_parcels
)
SELECT p.parcel_id
	,p.siteid
	--,sd.devtypeid																		--LOOK AT LU
	--,dev.development_type_id															--LOOK AT LU
	--,s.lu																				--LOOK AT LU
	--,s.LCKey																			--LOOK AT LU
FROM #sched_dev_parcels AS p
JOIN d ON p.parcel_id = d.parcel_id
JOIN gis.scheduled_development AS sd ON p.siteid = sd.siteid
--JOIN #sched_dev_sparcels AS s ON p.parcel_id = s.parcel_id AND p.siteid = s.siteid		--LOOK AT LU
--JOIN ref.development_type_lu_code AS dev ON s.lu = dev.lu_code							--LOOK AT LU
WHERE rownum = 2
ORDER BY p.parcel_id
	,p.siteid



/****** UPDATE PARCELS DATASET  ******/
UPDATE usp
SET usp.site_id = sdp.siteid
FROM urbansim.parcels AS usp
JOIN #sched_dev_parcels AS sdp ON usp.parcel_id = sdp.parcel_id

--CHECK
SELECT *
FROM urbansim.parcels
WHERE site_id IS NOT NULL
ORDER BY site_id, parcel_id
;

/****** UPDATE CAPACITY DATASET  ******/
UPDATE usc
SET usc.site_id = sdp.siteid
FROM urbansim.capacity AS usc
JOIN #sched_dev_parcels AS sdp ON usc.parcel_id = sdp.parcel_id
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
SELECT siteid
	,site_id
FROM GIS.scheduled_development AS sd
FULL OUTER JOIN (	SELECT site_id
		FROM urbansim.parcels
		WHERE site_id IS NOT NULL
		GROUP BY site_id) AS p
	ON sd.siteid = p.site_id
ORDER BY site_id, siteid


/*****************************************************************************************/
/****** PARCELIDS WITH INTERSECTION IN SITEID  ******/
/*
SELECT
	parcel_id
	--[ogr_fid]
	--,[ogr_geometry]
	,siteid
	--,[sitename]
	--,[totalsqft]
	,usp.shape.STIntersection(sd.ogr_geometry).STArea() / usp.shape.STArea()
FROM urbansim.parcels AS usp
JOIN GIS.scheduled_development AS sd
	ON usp.shape.STIntersects(sd.ogr_geometry) = 1
WHERE usp.shape.STIntersection(sd.ogr_geometry).STArea() / usp.shape.STArea() > 0.4		--INTERSECTION IS GREATER THAN 50% OF PARCEL SIZE
ORDER BY siteid, 3
--ALL = 10,970
--WHERE > 40% = 4,885
--WHERE > 50% = 4,871
--WHERE > 60% = 4,859
--WHERE > 70% = 4,846

/*CASE
parcel_id =		1536566
siteid =		14077
INTERSECTION =	0.40595024339161
*/
*/