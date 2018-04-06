USE spacecore

SELECT * FROM urbansim.scheduled_development_parcels
SELECT * FROM gis.scheduled_development_sites

--CLEAR ALL VALUES
UPDATE sdp
SET
	civemp = NULL
	,milemp = NULL
	,sfu = NULL
	,mfu = NULL
	,mhu = NULL
FROM urbansim.scheduled_development_parcels AS sdp


--SELECT ALL PARCELS IDENTIFIED WITH SITEID
IF OBJECT_ID('tempdb..#parcels') IS NOT NULL
    DROP TABLE #parcels
;
SELECT site_id
	,parcel_id
	,ROW_NUMBER() OVER (PARTITION BY site_id ORDER BY site_id, parcel_id) AS idx
INTO #parcels
FROM urbansim.scheduled_development_parcels
WHERE site_id IN(28, 1005)							--xxTEST
ORDER BY site_id
	,parcel_id
--SELECT * FROM #parcels


--VIEW SITE LEVEL
SELECT site_id
	,sfu
	,parcels
	--,parcel_id
	--,idx
FROM gis.scheduled_development_sites AS sds
JOIN (SELECT site_id, COUNT(*) AS parcels FROM #parcels GROUP BY site_id) AS p
	ON sds.siteid = p.site_id

--VIEW PARCEL LEVEL
SELECT site_id
	,sfu
	,parcels
	--,parcel_id
	--,idx
	,ROW_NUMBER() OVER (PARTITION BY parcels ORDER BY parcels) AS idx
FROM gis.scheduled_development_sites AS sds
JOIN (SELECT site_id, COUNT(*) AS parcels FROM #parcels GROUP BY site_id) AS p
	ON sds.siteid = p.site_id
JOIN ref.numbers AS n
	ON n.numbers <= sfu 




/*#################### ALLOCATE SFU ####################*/
WITH units AS(
	SELECT siteid
		,sfu
		,ROW_NUMBER() OVER (PARTITION BY siteid ORDER BY siteid) AS idx
	FROM gis.scheduled_development_sites AS sds
	JOIN ref.numbers AS n
		ON n.numbers <= sfu
	ORDER BY siteid
)
SELECT site_id
	,parcel_id
FROM #parcels






--************************************************************************
SELECT SUM(sfu)
FROM gis.scheduled_development_sites

SELECT siteid, sfu
FROM gis.scheduled_development_sites
WHERE sfu <> 0
ORDER BY sfu

SELECT siteid
	,COUNT(*)
FROM gis.scheduled_development_sites
GROUP BY siteid
--HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC
