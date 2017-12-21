USE spacecore

/****** FIX SRID  ******/
SELECT DISTINCT centroid.STSrid FROM urbansim.parcels;
SELECT DISTINCT ogr_geometry.STSrid FROM GIS.scheduled_development;

UPDATE GIS.scheduled_development SET ogr_geometry.STSrid = 2230;		--NAD83 / California zone 6 (ftUS)


/****** PARCELIDS WITH CENTROID IN SITEID  ******/
IF OBJECT_ID('tempdb..#sched_dev_parcels') IS NOT NULL
    DROP TABLE #sched_dev_parcels
;

SELECT
	parcel_id
	--[ogr_fid]
	--,[ogr_geometry]
	,siteid
	--,[sitename]
	--,[totalsqft]
INTO #sched_dev_parcels
FROM urbansim.parcels AS usp
JOIN GIS.scheduled_development AS sd
	ON usp.centroid.STIntersects(sd.ogr_geometry) = 1
ORDER BY siteid
--ALL = 4,872
--SEE BELOW FOR DIFFERENT METHOD ON JOINING SITES TO PARCELS

--CHECK
SELECT * FROM #sched_dev_parcels WHERE parcel_id = 5293131

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

--DROP TEMP TABLE
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