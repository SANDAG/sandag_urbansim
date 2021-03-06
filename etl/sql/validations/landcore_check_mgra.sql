/****** RE-STAMP MGRA AND COMPARE  ******/
USE spacecore

WITH mgra_bad AS ( 
SELECT [OBJECTID]
	,l.[Shape]
	,[subParcel]
	,[lu]
	,[plu]
	,[planID]
	,[siteID]
	,[genOwnID]
	,[apn]
	,[parcelID]
	,[editFlag]
	,[du]
	,[MGRA] mgra_lc
	,[regionID]
	,[area]
	,m.zone mgra_m
	,CASE 
		WHEN l.MGRA = m.zone THEN 'match'
		ELSE 'fail'
	END AS match
	,l.MGRA - m.zone dist
FROM [spacecore].[GIS].[landcore] l
JOIN (SELECT zone, shape 
	FROM [data_cafe].[ref].[geography_zone]
	WHERE geography_type_id = 90) m
ON l.Shape.STCentroid().STWithin(m.shape) = 1

--EXCLUDE FROM SELECTION:
WHERE l.MGRA != m.zone				--MATCHING
AND lu != 4112						--FREEWAY
AND lu != 4117						--RAILROAD RIGHT OF WAY
AND lu != 4118						--ROAD RIGHT OF WAY
AND lu NOT BETWEEN 7600 AND 7700	--PARKS
AND lu NOT BETWEEN 9200 AND 9202	--WATER
)

--VALIDATE PARCEL_ID FROM CENTROID TO SHAPE
SELECT b.parcelID, b.mgra_lc, b.mgra_m, b.lu, b.Shape 
FROM mgra_bad b
JOIN [spacecore].[GIS].[landcore] l
ON l.Shape.STCentroid().STWithin(b.shape) = 1
WHERE b.parcelID = l.parcelID

