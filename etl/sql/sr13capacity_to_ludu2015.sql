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
	parcel_id_ludu2015 int NOT NULL
	,du_sr13 int NULL
	,cap_hs_sr13 int NULL
	,du_ludu2015 int NULL
	,cap_hs_ludu2015 int NULL
	,update_2015 varchar(32)
) 
GO

--LOAD LCKey TO parcel FOR RECORDS FOUND IN ludu2015
INSERT INTO ref.sr13capacity_ludu2015(
	parcel_id_ludu2015
	,du_sr13
	,cap_hs_sr13
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
	WHERE ludu2015.parcelID IN (SELECT parcel_id_ludu2015 FROM ref.sr13capacity_ludu2015)
	GROUP BY ludu2015.parcelID
	ORDER BY 3 DESC
)
UPDATE sr_ludu
SET
	du_sr13 = du_sr13 + sr13.du
	,cap_hs_sr13 = cap_hs_sr13 + sr13.cap_hs
	,update_2015 = 'parcel_and_centroid_to_parcel'
FROM ref.sr13capacity_ludu2015 AS sr_ludu
JOIN sr13 ON sr_ludu.parcel_id_ludu2015 = sr13.parcelID


--LOAD LCKey TO parcel FOR RECORDS NOT FOUND IN ludu2015 AND NOT ALREADY ADDED
INSERT INTO ref.sr13capacity_ludu2015(
	parcel_id_ludu2015
	,du_sr13
	,cap_hs_sr13
	,update_2015)
SELECT
	ludu2015.parcelID
	,SUM(sr13.du)
	,SUM(sr13.cap_hs)
	,'centroid_to_parcel' 
FROM [staging].[sr13_missing_match] AS sr13
LEFT JOIN [GIS].[ludu2015] AS ludu2015
	ON sr13.subParcel = ludu2015.subParcel
WHERE ludu2015.parcelID NOT IN (SELECT parcel_id_ludu2015 FROM ref.sr13capacity_ludu2015)
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
SELECT COUNT(*), SUM(du_sr13), SUM(cap_hs_sr13) FROM ref.sr13capacity_ludu2015 


--SELECT update_2015, COUNT(*) FROM ref.sr13capacity_ludu2015 GROUP BY update_2015

--UPDATE ludu2015 du
UPDATE sr_ludu
SET
	du_ludu2015 = ludu2015.du
FROM ref.sr13capacity_ludu2015 AS sr_ludu
JOIN (SELECT parcelID, SUM(du) AS du FROM GIS.ludu2015 GROUP BY parcelID) AS ludu2015
ON sr_ludu.parcel_id_ludu2015 = ludu2015.parcelID


--CALCULATE UPDATED cap_hs TO 2015
SELECT
	parcel_id_ludu2015
	,du_sr13
	,du_ludu2015
	,(du_ludu2015 - du_sr13) AS growth
	,cap_hs_sr13
	,CASE
		WHEN (du_ludu2015 = du_sr13) THEN cap_hs_sr13		--NO CHANGE IN du, cap_hs REMAINS
		WHEN (du_ludu2015 - du_sr13 = cap_hs_sr13) THEN 0	--GROWTH MATCHES cap_hs
		WHEN (du_ludu2015 - du_sr13 > cap_hs_sr13) THEN 0	--GROWTH IS GREATER THAN EXPECTED cap_hs (NEGATIVE UPDATED cap_hs)
		WHEN (du_ludu2015 < du_sr13) THEN cap_hs_sr13		--FOR NEGATIVE GROWTH KEEP cap_hs
		ELSE cap_hs_sr13 - (du_ludu2015 - du_sr13)			--CALCULATE GROWTH, SUBSTRACT FROM cap_hs
	END AS cap_hs_to2015
	,update_2015
FROM ref.sr13capacity_ludu2015
--WHERE du_sr13 <> du_ludu2015	--NO CHANGE IN du
ORDER BY 4 DESC


--UPDATE cap_hs TO 2015
UPDATE ref.sr13capacity_ludu2015
SET
	cap_hs_ludu2015 = CASE
						WHEN (du_ludu2015 = du_sr13) THEN cap_hs_sr13		--NO CHANGE IN du, cap_hs REMAINS
						WHEN (du_ludu2015 - du_sr13 = cap_hs_sr13) THEN 0	--GROWTH MATCHES cap_hs
						WHEN (du_ludu2015 - du_sr13 > cap_hs_sr13) THEN 0	--GROWTH IS GREATER THAN EXPECTED cap_hs (NEGATIVE UPDATED cap_hs)
						WHEN (du_ludu2015 < du_sr13) THEN cap_hs_sr13		--FOR NEGATIVE GROWTH KEEP cap_hs
						ELSE cap_hs_sr13 - (du_ludu2015 - du_sr13)			--CALCULATE GROWTH, SUBSTRACT FROM cap_hs
					END



SELECT * FROM ref.sr13capacity_ludu2015
WHERE du_sr13 <> du_ludu2015
