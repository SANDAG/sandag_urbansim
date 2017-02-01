--forecast MISSING FROM ludu2015
SELECT COUNT(*)
FROM [input].[forecast_landcore]
WHERE parcelId NOT IN (SELECT parcelID FROM [gis].[ludu2015])


SELECT TOP 100 * FROM [spacecore].[input].[forecast_landcore]
SELECT TOP 100 * FROM [regional_forecast].[sr13_final].[capacity]

/*
--PARCEL LEVEL
WITH sr13 AS(
	SELECT parcelID
		,SUM(dwellUnits) AS du
		,SUM(cap_hs) AS cap_hs
		,geometry::UnionAggregate(shape) AS shape
	FROM [spacecore].[input].[forecast_landcore] AS ludu
	JOIN (SELECT * 
			FROM [regional_forecast].[sr13_final].[capacity]
			WHERE scenario = 0 AND Increment =2012) AS cap
		ON ludu.LCKey = cap.LCKey
	GROUP BY ludu.parcelID
	--ORDER BY ludu.parcelID
)
SELECT --COUNT(*), SUM(du) AS du, SUM(cap_hs) AS cap_hs
		parcelID
		,du
		,cap_hs
		,shape
INTO spacecore.staging.sr13_missing_parcel
FROM sr13
WHERE parcelID NOT IN (SELECT parcelID FROM GIS.ludu2015)
ORDER BY cap_hs DESC
*/

--LCKEY LEVEL
WITH sr13 AS(
	SELECT ludu.LCKey
		,parcelID
		,dwellUnits AS du
		,cap_hs AS cap_hs
		,shape
	FROM [spacecore].[input].[forecast_landcore] AS ludu
	JOIN (SELECT * 
			FROM [regional_forecast].[sr13_final].[capacity]
			WHERE scenario = 0 AND Increment =2012) AS cap
		ON ludu.LCKey = cap.LCKey
)
SELECT --COUNT(*), SUM(du) AS du, SUM(cap_hs) AS cap_hs
		LCKey
		--,ROW_NUMBER() OVER(PARTITION BY parcelID ORDER BY LCKey) AS row_num
		,parcelID
		,du
		,cap_hs
		,shape
--INTO spacecore.staging.sr13_missing_LCKey
FROM sr13
WHERE parcelID NOT IN (SELECT parcelID FROM GIS.ludu2015)
ORDER BY row_num DESC, parcelID DESC


--LCKey LEVEL centroid
WITH ludu12 AS(
	SELECT sr13.[LCKey]
		  ,ludu12.shape AS centroid
	  FROM [spacecore].[staging].[sr13_missing_LCKey] sr13
	JOIN [GIS].[ludu2012points] ludu12
	ON sr13.LCKey = ludu12.LCKey
	--ORDER BY ludu12.du
),
cent AS(
	SELECT LCKey
		,sr13.shape.STCentroid() AS centroid
	FROM [spacecore].[staging].[sr13_missing_LCKey] sr13
	WHERE LCKey NOT IN (SELECT LCKey FROM [GIS].[ludu2012points])
)
SELECT sr13.LCKey
	,sr13.[parcelID]
	,sr13.[du]
	,sr13.[cap_hs]
	,sr13.[shape]
	,COALESCE(ludu12.centroid, cent.centroid) AS centroid
INTO staging.sr13_missing_LCKey_centroid
FROM [spacecore].[staging].[sr13_missing_LCKey] sr13
LEFT JOIN ludu12 ON sr13.LCKey = ludu12.LCKey
LEFT JOIN cent ON sr13.LCKey = cent.LCKey


SELECT COUNT(*) FROM [spacecore].[staging].[sr13_missing]	--9,341
SELECT COUNT(*) FROM [spacecore].[staging].[sr13_missing_LCKey]	--9,945


/*** s13 LCKey, ASSIGN NEAREST ludu2015 subParcel ***/
WITH match AS(
	SELECT row_id, LCKey, subParcel, dist 
	FROM (
		SELECT
			ROW_NUMBER() OVER (PARTITION BY sr13.LCKey ORDER BY sr13.LCKey, sr13.centroid.STDistance(ludu15.shape)) row_id
			,sr13.LCKey
			,ludu15.subParcel
			,sr13.centroid.STDistance(ludu15.shape) AS dist
		FROM [staging].[sr13_missing_LCKey_centroid] sr13
			LEFT JOIN [GIS].[ludu2015] ludu15
			ON sr13.centroid.STBuffer(100).STIntersects(ludu15.shape) = 1	--CHECK IF BUFFERDIST IS SUFFICIENT
			) x
	WHERE row_id = 1
	--ORDER BY subParcel
)
SELECT
	match.LCKey
	,match.subParcel
	,sr13.du
	,sr13.cap_hs
INTO [staging].[sr13_missing_match]
FROM [staging].[sr13_missing_LCKey_centroid] AS sr13
JOIN match ON sr13.LCKey = match.LCKey

--CHECK TABLE AND FIX
UPDATE [spacecore].[staging].[sr13_missing_match]
SET subParcel = ######
WHERE subParcel IS NULL


/*** CREATE TABLE ref.sr13_capacity ***/
CREATE TABLE ref.sr13capacity_ludu2015(
	ludu2015_parcel_id int NOT NULL
	,sr13_du int NULL
	,sr13_cap_hs_with_negatives int NULL
	,ludu2015_du int NULL
	,sr13_cap_hs_growth_adjusted int NULL
	,update_2015 varchar(32)
	,revised varchar(1)
) 
GO

--LOAD LCKey TO parcel FOR RECORDS FOUND IN ludu2015
INSERT INTO ref.sr13capacity_ludu2015(
	ludu2015_parcel_id
	,sr13_du
	,sr13_cap_hs_with_negatives
	,update_2015)
SELECT
	parcelId
	,SUM(dwellUnits)
	,SUM(cap_hs)
	,'parcel_to_parcel'
FROM [input].[forecast_landcore] AS ludu
	JOIN (SELECT * 
			FROM [regional_forecast].[sr13_final].[capacity]
			WHERE scenario = 0 AND Increment =2012) AS cap
		ON ludu.LCKey = cap.LCKey
WHERE parcelId IN (SELECT parcelID FROM [gis].[ludu2015])
GROUP BY parcelId


--UPDATE LCKey TO parcel FOR RECORDS NOT FOUND IN ludu2015 AND ALREADY ADDED
WITH sr13 AS(
	SELECT
		ludu2015.parcelID
		,SUM(sr13.du) AS du
		,SUM(sr13.cap_hs) AS cap_hs
	FROM [staging].[sr13_missing_match] AS sr13
	LEFT JOIN [GIS].[ludu2015] AS ludu2015
		ON sr13.subParcel = ludu2015.subParcel
	WHERE ludu2015.parcelID IN (SELECT ludu2015_parcel_id FROM ref.sr13capacity_ludu2015)
	GROUP BY ludu2015.parcelID
	--ORDER BY 3 DESC
)
UPDATE sr_ludu
SET
	sr13_du = sr13_du + sr13.du
	,sr13_cap_hs_with_negatives = sr13_cap_hs_with_negatives + sr13.cap_hs
	,update_2015 = 'parcel_and_centroid_to_parcel'
FROM ref.sr13capacity_ludu2015 AS sr_ludu
JOIN sr13 ON sr_ludu.ludu2015_parcel_id = sr13.parcelID


--LOAD LCKey TO parcel FOR RECORDS NOT FOUND IN ludu2015 AND NOT ALREADY ADDED
INSERT INTO ref.sr13capacity_ludu2015(
	ludu2015_parcel_id
	,sr13_du
	,sr13_cap_hs_with_negatives
	,update_2015)
SELECT
	ludu2015.parcelID
	,SUM(sr13.du)
	,SUM(sr13.cap_hs)
	,'centroid_to_parcel' 
FROM [staging].[sr13_missing_match] AS sr13
LEFT JOIN [GIS].[ludu2015] AS ludu2015
	ON sr13.subParcel = ludu2015.subParcel
WHERE ludu2015.parcelID NOT IN (SELECT ludu2015_parcel_id FROM ref.sr13capacity_ludu2015)
GROUP BY ludu2015.parcelID


--CHECK CAPACITY TOTALS
--FROM sr13
SELECT 
	COUNT(*)
	,SUM(dwellUnits) AS du
	,SUM(cap_hs) AS cap_hs
FROM [spacecore].[input].[forecast_landcore] AS ludu
JOIN (SELECT * 
		FROM [regional_forecast].[sr13_final].[capacity]
		WHERE scenario = 0 AND Increment =2012) AS cap
	ON ludu.LCKey = cap.LCKey

--FROM sr13capacity_ludu2015
SELECT COUNT(*), SUM(sr13_du), SUM(sr13_cap_hs_growth_adjusted) FROM ref.sr13capacity_ludu2015 


--SELECT update_2015, COUNT(*) FROM ref.sr13capacity_ludu2015 GROUP BY update_2015

--UPDATE ludu2015 du
UPDATE sr_ludu
SET
	ludu2015_du = ludu2015.du
FROM ref.sr13capacity_ludu2015 AS sr_ludu
JOIN (SELECT parcelID, SUM(du) AS du FROM GIS.ludu2015 GROUP BY parcelID) AS ludu2015
ON sr_ludu.ludu2015_parcel_id = ludu2015.parcelID


--CALCULATE UPDATED cap_hs TO 2015
SELECT
	ludu2015_parcel_id
	,sr13_du
	,ludu2015_du
	,(ludu2015_du - sr13_du) AS growth
	,sr13_cap_hs_with_negatives
	,CASE
		WHEN (ludu2015_du = sr13_du) THEN sr13_cap_hs_with_negatives		--NO CHANGE IN du, cap_hs REMAINS
		WHEN (ludu2015_du - sr13_du = sr13_cap_hs_with_negatives) THEN 0	--GROWTH MATCHES cap_hs
		WHEN (ludu2015_du - sr13_du > sr13_cap_hs_with_negatives) THEN 0	--GROWTH IS GREATER THAN EXPECTED cap_hs (NEGATIVE UPDATED cap_hs)
		WHEN (ludu2015_du < sr13_du) THEN sr13_cap_hs_with_negatives		--FOR NEGATIVE GROWTH KEEP cap_hs
		ELSE sr13_cap_hs_with_negatives - (ludu2015_du - sr13_du)			--CALCULATE GROWTH, SUBSTRACT FROM cap_hs
	END AS cap_hs_to2015
	,update_2015
FROM ref.sr13capacity_ludu2015
--WHERE sr13_du <> ludu2015_du	--NO CHANGE IN du
ORDER BY 4 DESC


--UPDATE cap_hs TO 2015
UPDATE ref.sr13capacity_ludu2015
SET
	sr13_cap_hs_growth_adjusted = CASE
						WHEN (ludu2015_du = sr13_du) THEN sr13_cap_hs_with_negatives		--NO CHANGE IN du, cap_hs REMAINS
						WHEN (ludu2015_du - sr13_du = sr13_cap_hs_with_negatives) THEN 0	--GROWTH MATCHES cap_hs
						WHEN (ludu2015_du - sr13_du > sr13_cap_hs_with_negatives) THEN 0	--GROWTH IS GREATER THAN EXPECTED cap_hs (NEGATIVE UPDATED cap_hs)
						WHEN (ludu2015_du < sr13_du) THEN sr13_cap_hs_with_negatives		--FOR NEGATIVE GROWTH KEEP cap_hs
						ELSE sr13_cap_hs_with_negatives - (ludu2015_du - sr13_du)			--CALCULATE GROWTH, SUBSTRACT FROM cap_hs
					END


-- REVISE PARCELS WHERE cap_hs CHANGED
SELECT * FROM ref.sr13capacity_ludu2015
WHERE sr13_cap_hs_with_negatives <> sr13_cap_hs_growth_adjusted
ORDER BY sr13_cap_hs_growth_adjusted DESC


--REVISE PARCELS WITH LARGE cap_hs BY CITY
WITH c AS(
	SELECT * FROM ref.sr13capacity_ludu2015 AS c
	JOIN (SELECT parcelID, mgra FROM gis.ludu2015) AS m
	ON c.ludu2015_parcel_id = m.parcelID
	WHERE sr13_cap_hs_with_negatives <> sr13_cap_hs_growth_adjusted
)
SELECT
	c.*
	,m.city
FROM c
JOIN OPENQUERY(sql2014b8, 'SELECT * FROM [lis].[gis].[MGRA13]') AS m
	ON c.mgra = m.mgra
WHERE m.city IN(6, 16)
ORDER BY c.sr13_cap_hs_with_negatives DESC


/*** ADJUST ***/
--TO ADJUST cap_hs TO 0, CREATE .csv FILE WITH parcel_id
CREATE TABLE staging.sr13_cap_adjust (
parcel_id int NOT NULL
);

--ADD parcel_id THAT ARE NOT IN ORIGINAL CAPACITY, INTO .csv

--ADJUST [sr13_cap_hs_growth_adjusted] AND TAKE NOTE [adjusted_cap_hs]
BULK INSERT staging.sr13_cap_adjust
FROM '\\nasb8\Shared\TEMP\esa\cap_hs\cap_hs_sr13_to_ludu15_adjust.csv'	--NETWORK
WITH
(
	FIRSTROW = 1,
	FIELDTERMINATOR = ',',  --CSV field delimiter
	ROWTERMINATOR = '\n',   --Use to shift the control to next row
	TABLOCK
)

--JOIN TABLES FOR ADDITIONAL parcel_id
SELECT
	ludu2015_parcel_id
	,parcel_id
	,COALESCE(ludu2015_parcel_id, parcel_id) AS parcel_id_all
	,sr13_cap_hs_growth_adjusted
	,CASE
		WHEN parcel_id IS NOT NULL THEN 0
		ELSE sr13_cap_hs_growth_adjusted
	END AS cap_hs_adjusted
FROM ref.sr13capacity_ludu2015 AS c
FULL OUTER JOIN staging.sr13_cap_adjust AS a
ON c.ludu2015_parcel_id = a.parcel_id
--WHERE a.parcel_id IS NOT NULL	--JUST FROM UPDATE TABLE
ORDER BY 2 DESC

--INSERT NEW parcel_id RECORDS
INSERT INTO ref.sr13capacity_ludu2015(
	ludu2015_parcel_id
	,sr13_cap_hs_growth_adjusted
	,revised)
SELECT parcel_id
	,0
	,'Y'
FROM staging.sr13_cap_adjust
WHERE parcel_id NOT IN (SELECT ludu2015_parcel_id FROM ref.sr13capacity_ludu2015)	--NOT ALREADY IN TABLE
ORDER BY parcel_id


--UPDATE [sr13_cap_hs_growth_adjusted]
UPDATE c
SET
	[ludu2015_parcel_id] = COALESCE(c.ludu2015_parcel_id, a.parcel_id)
	,[sr13_cap_hs_growth_adjusted] = CASE 
		WHEN parcel_id IS NOT NULL THEN 0
		ELSE sr13_cap_hs_growth_adjusted
		END
	,[revised] = 'Y'
FROM ref.sr13capacity_ludu2015 AS c
FULL OUTER JOIN staging.sr13_cap_adjust AS a
ON c.ludu2015_parcel_id = a.parcel_id
WHERE a.parcel_id IS NOT NULL	--DO FOR UPDATE TABLE


SELECT SUM([sr13_cap_hs_growth_adjusted])	--383,235
FROM [ref].[sr13capacity_ludu2015]


/* CHECKS */
SELECT 
	SUM(sr13_du) AS sr13_du
	,SUM(sr13_cap_hs_with_negatives) AS sr13_cap_hs_with_negatives
	,SUM(ludu2015_du) AS ludu2015_du
	,SUM(sr13_cap_hs_growth_adjusted) AS sr13_cap_hs_growth_adjusted
FROM ref.sr13capacity_ludu2015

SELECT * FROM ref.sr13capacity_ludu2015
WHERE sr13_du <> ludu2015_du

