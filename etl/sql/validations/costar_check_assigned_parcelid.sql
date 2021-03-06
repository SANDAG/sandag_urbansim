/****** Script for SelectTopNRows command from SSMS  ******/
USE spacecore

--COUNT COSTAR
SELECT COUNT(*) FROM [spacecore].[input].[costar]

/* JOIN COSTAR TO PARCELS ON APN, CHECK PARCEL_ID MATCH */
SELECT APN_8 APN_8_from_Parcel_APN_join
	,[parcel_number_1(min)] [parcel_number_1(min)_from_costar]
	,parcelID parcelID_from_GISParcel_APN_join
	,[parcel_id] [parcel_id_from_costarPoint_join]
	,[property_id] [property_id_from_costar]
	,[location]
	,CASE WHEN parcelID = [parcel_id] THEN 'MATCH'
		ELSE 'NO'
	END AS check_
FROM [spacecore].[input].[costar] c
LEFT JOIN (SELECT APN_8, parcelID FROM GIS.parcels GROUP BY APN_8, parcelID) p
ON p.APN_8 = LEFT(REPLACE(c.[parcel_number_1(min)], '-', ''), 8)
WHERE [parcel_number_1(min)] IS NOT NULL
AND [parcel_number_1(min)]  != ''
ORDER BY 7 DESC, 2 --[parcel_number_1(min)], [parcel_number_2(max)]
;

--CLEAN UP APN8
SELECT LEFT(REPLACE(c.[parcel_number_1(min)], '-', ''), 8)
FROM [spacecore].[input].[costar] c
WHERE [parcel_number_1(min)] IS NOT NULL
AND [parcel_number_1(min)]  != ''
ORDER BY [parcel_number_1(min)]

--CHECK COSTAR APN
SELECT [property_id]
	,[propertytype]
	,[parcel_number_1(min)]
	,[parcel_number_2(max)]
	,[secondary_type]
	,[centroid]
	,[parcel_id]
	,[location]
FROM [spacecore].[input].[costar]
WHERE [parcel_number_1(min)] IS NOT NULL
AND [parcel_number_1(min)]  != ''
ORDER BY [parcel_number_1(min)], [parcel_number_2(max)]
;

--CHECK GIS PARCELS APN
SELECT APN, APN_8
FROM GIS.parcels
WHERE APN_8 IS NOT NULL
ORDER BY APN_8


/* JOIN COSTAR TO ADDRESSAPN, ON 8 AND 10 DIGIT */
--JOIN COSTAR TO ADDRESSAPN, ON 8 DIGIT
SELECT LEFT(a.APN, 8) APN, c.[parcel_number_1(min)]
FROM [spacecore].[input].[costar] c
JOIN OPENQUERY([sql2014b8], 'SELECT DISTINCT APN FROM lis.GIS.AddrAPN GROUP BY APN') a
ON LEFT(a.APN, 8) = LEFT(REPLACE(c.[parcel_number_1(min)], '-', ''), 8)

--JOIN COSTAR TO ADDRESSAPN, ON 10 DIGIT
WITH costar AS (
	SELECT 
	[property_id]
	,[parcel_number_1(min)]
	,[parcel_number_2(max)]
	,CASE 
		WHEN LEN(LEFT(REPLACE([parcel_number_1(min)], '-', ''), 10)) = 10 THEN LEFT(REPLACE([parcel_number_1(min)], '-', ''), 10)
		WHEN LEN(LEFT(REPLACE([parcel_number_1(min)], '-', ''), 10)) = 8 THEN CONCAT(LEFT(REPLACE([parcel_number_1(min)], '-', ''), 10), '00')
	END AS [parcel_number_1(min)_fixed10]
	,CASE 
		WHEN LEN(LEFT(REPLACE([parcel_number_2(max)], '-', ''), 10)) = 10 THEN LEFT(REPLACE([parcel_number_2(max)], '-', ''), 10)
		WHEN LEN(LEFT(REPLACE([parcel_number_2(max)], '-', ''), 10)) = 8 THEN CONCAT(LEFT(REPLACE([parcel_number_2(max)], '-', ''), 10), '00')
	END AS [parcel_number_2(max)_fixed10]
FROM [spacecore].[input].[costar]
	)
SELECT c.[property_id]
	,a.APN APN_from_AddrAPN
	,c.[parcel_number_1(min)]
	,c.[parcel_number_2(max)]
	,c.[parcel_number_1(min)_fixed10]
	,c.[parcel_number_2(max)_fixed10]
	,CASE
		WHEN ISNUMERIC([parcel_number_1(min)_fixed10]) = 1 THEN CAST(c.[parcel_number_2(max)_fixed10] AS bigint) - CAST(c.[parcel_number_1(min)_fixed10] AS bigint)
		ELSE NULL
	END AS parcel_number_diff
FROM costar c
LEFT JOIN OPENQUERY([sql2014b8], 'SELECT DISTINCT APN FROM lis.GIS.AddrAPN GROUP BY APN') a
ON a.APN = c.[parcel_number_1(min)_fixed10] 
WHERE c.[parcel_number_1(min)] IS NOT NULL
AND c.[parcel_number_1(min)]  != ''
ORDER BY 3 


/* CHECK NUMBER OF OCURRENCES FOR 8 AND 10 DIGIT APN */
--CHECK NUMBER OF OCURRENCES FOR 10 DIGIT APN
SELECT [parcel_number_1(min)]
FROM [spacecore].[input].[costar] 
WHERE [parcel_number_1(min)]  = '369-140-15-09'			--1
SELECT APN FROM OPENQUERY([sql2014b8], 'SELECT APN FROM lis.GIS.AddrAPN GROUP BY APN')
WHERE APN = '3691401509'								--1
SELECT APN FROM OPENQUERY([sql2014b8], 'SELECT APN FROM lis.GIS.AddrAPN')
WHERE APN = '3691401509'								--1

--CHECK NUMBER OF OCURRENCES FOR 8 DIGIT APN
SELECT [parcel_number_1(min)]
FROM [spacecore].[input].[costar] 
WHERE LEFT([parcel_number_1(min)],10)  = '369-140-15'	--2
SELECT APN FROM OPENQUERY([sql2014b8], 'SELECT APN FROM lis.GIS.AddrAPN GROUP BY APN')
WHERE LEFT(APN,8) = '36914015'							--84
SELECT APN FROM OPENQUERY([sql2014b8], 'SELECT APN FROM lis.GIS.AddrAPN')
WHERE LEFT(APN,8) = '36914015'							--85
