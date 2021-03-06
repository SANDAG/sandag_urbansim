/****** Script for SelectTopNRows command from SSMS  ******/
SELECT * FROM [isam].[urbansim].[costar]



---***
SELECT DISTINCT parcel_id, COUNT(*)
FROM [isam].[urbansim].[costar]
GROUP BY parcel_id
ORDER BY 2 DESC

SELECT DISTINCT parcel_id, subparcel_id, COUNT(*)
FROM [isam].[urbansim].[costar]
GROUP BY parcel_id, subparcel_id
ORDER BY 3 DESC
---***

--PARCEL_ID IS NULL, LAT LONG NOT MAPPING TO PARCEL
SELECT [property_id]
      ,[building_address]
      ,[building_name]
      ,[building_status]
      ,[city]
	  ,[zip]
      ,[parcel_id]
      ,[subparcel_id]
FROM [isam].[urbansim].[costar]
WHERE parcel_id IS NULL
ORDER BY property_id



---GEOCODED
SELECT TOP 1000 [ogr_fid]
      ,[centroid]
      ,[property_id]
      ,[address]
      ,[latitude]
      ,[longitude]
  FROM [isam].[urbansim].[costar_geocode_results]


SELECT * FROM [isam].[urbansim].[costar]
WHERE parcel_id IS NULL						--78 records

SELECT * FROM [isam].[urbansim].[costar]
WHERE centroid IS NULL						--78 records


---************
SELECT [property_id]
      ,[building_address]
      ,[building_name]
      ,[building_status]
      ,[city]
	  ,[zip]
      ,[parcel_id]
      ,[subparcel_id]
	  ,centroid
FROM input.costar
WHERE parcel_id IS NULL
ORDER BY property_id

SELECT building_status, COUNT(*)
FROM input.costar
WHERE parcel_id IS NULL
GROUP BY building_status

