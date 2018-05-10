USE spacecore

/*
IF OBJECT_ID('urbansim.parcels') IS NOT NULL
   DROP TABLE urbansim.parcels
GO
*/

CREATE TABLE urbansim.parcels (
    --objectid int not null --PLACEHOLDER FOR OPERATIONS BELOW. DROP AT END OF LOAD		--***
    parcel_id int NOT NULL
	,block_id nvarchar(15) --NOT NULL
	,development_type_id_2015 int
	,development_type_id_2017 int
	--,own int
	,lu_2015 int
	,lu_2017 int
	,du_2015 int
	,du_2017 int
    ,land_value float
    ,parcel_acres float
    ,region_id integer
    ,mgra_id integer
    --,zone varchar(35)
    ,luz_id smallint
    ,msa_id smallint
	,jurisdiction_id smallint
    ,proportion_undevelopable float
    ,tax_exempt_status bit
    ,distance_to_freeway float
    ,distance_to_onramp float
	,distance_to_coast float
    ,distance_to_transit float
    ,apn nvarchar(8) --This doesn't really work for condo lots
    ,shape geometry
    ,centroid geometry --Placeholder for spatial operations
)

--INSERT FROM LUDU2015: SHAPE
INSERT INTO urbansim.parcels WITH (TABLOCK) (parcel_id, own, du_2015 shape)
SELECT
    parcelID
	--,MIN(genOwnID)
	,SUM(du)
	,geometry::UnionAggregate(Shape)
FROM
    GIS.ludu2015
GROUP BY
	parcelID

--UPDATE ACREAGE
UPDATE
	usp
SET
	parcel_acres = Shape.STArea() / 43560.
FROM
	urbansim.parcels AS usp
;
--UPDATE CENTROID FROM LARGEST SUBPARCEL
WITH cent AS(
	SELECT
		ROW_NUMBER() OVER (PARTITION BY ParcelID ORDER BY lu_case, acres DESC) row_id
		,ParcelID
		,LCKey
		,centroid
		,acres
	FROM
		(SELECT
			ParcelID
			,LCKey
			,lu
			,CASE WHEN lu IN(4110, 4112, 4117, 4118) THEN 2
				ELSE 1
			END AS lu_case				 
			,centroid
			,acres
		FROM GIS.ludu2015points) x

)
UPDATE
	usp
SET
	centroid = cent.centroid
FROM urbansim.parcels AS usp
JOIN cent ON cent.parcelID  = usp.parcel_id
WHERE cent.row_id = 1
;
--UPDATE ASR, INSERT WILL THROW A WARNING ABOUT SUMMING ASR_LAND BECAUSE OF NULLS --- THAT'S OKAY
WITH asr AS(
	SELECT 
		parcelid
		,sum(ASR_LAND) as land_value
		,CASE max(NULLIF(taxstat, 'N')) WHEN 'N' THEN 1 ELSE 0 END as tax_exempt_status
	FROM
		GIS.parcels
	GROUP BY 
		parcelid
)
UPDATE
	usp
SET
    usp.land_value = asr.land_value
	,usp.tax_exempt_status = asr.tax_exempt_status 
    ,region_id = 1
FROM
	urbansim.parcels AS usp
JOIN asr ON asr.parcelid = usp.parcel_id
;

--SET THE DEVELOPMENT TYPE ID AND LU FROM PRIORITY FOR 2015
--GET PRIORITY, STORE TO TEMP TABLE
DROP TABLE IF EXISTS #priority
;
SELECT
	lc.parcelID AS parcel_id
	,dev.priority AS p
	,dev.development_type_id
	,lc.lu
	,lc.acres
	,ROW_NUMBER() OVER (PARTITION BY parcelID ORDER BY priority, acres DESC) AS rownum
INTO #priority
FROM gis.ludu2015 AS lc								--LUDU 2015
LEFT JOIN ref.development_type_lu_code xref 
	ON lc.lu = xref.lu_code
LEFT JOIN ref.development_type dev 
	ON xref.development_type_id = dev.development_type_id
ORDER BY lc.parcelID
;
--GET PRIORITY AND COPY VALUES
UPDATE
    usp
SET 
    usp.development_type_id_2015 = p.development_type_id
	,lu_2015 = lu
FROM
    urbansim.parcels usp 
LEFT JOIN
	(SELECT * FROM #priority WHERE rownum = 1) AS p
ON usp.parcel_id = p.parcel_id
;
DROP TABLE IF EXISTS #priority
;


--SET THE DEVELOPMENT TYPE ID FOR MILITARY 2015
DROP TABLE IF EXISTS #mil
;
WITH  lu AS(
	SELECT
		parcelID AS parcel_id
		,MIN(lu) AS lu							--MAY HAVE MULTIPLE MIL LU
	FROM gis.ludu2015
	WHERE lu BETWEEN 6700 AND 6709				--GQ Military Barracks
	OR lu = 1403								--Military Use
	GROUP BY parcelID
	--ORDER BY parcelID
)
, own AS(
	SELECT
		parcelId AS parcel_id
		,MIN(own) AS own
	FROM spacecore.gis.ludu2015
	WHERE own = 41
	GROUP BY parcelId
	--ORDER BY parcel_id
)
, fac AS(
	SELECT
		parcel_id
		,base_name
	FROM spacecore.GIS.military_facility_parcel_2015
	WHERE mil_base = 1
	--ORDER BY parcel_id
)
SELECT
	usp.parcel_id
	,lu.lu
	,own.own
	,fac.base_name
	,usp.lu_2015 AS lu_2015_all
	,usp.du_2015
	,usp.development_type_id_2015
	,CASE
		WHEN usp.development_type_id_2015 IN (23,29) THEN usp.development_type_id_2015 
		WHEN usp.du_2015 > 0 THEN 23
		ELSE 29
	END AS development_type_id_2015_mil
INTO #mil
FROM urbansim.parcels As usp
LEFT JOIN lu ON usp.parcel_id = lu.parcel_id
LEFT JOIN own ON usp.parcel_id = own.parcel_id
LEFT JOIN fac ON usp.parcel_id = fac.parcel_id
WHERE lu.lu IS NOT NULL
OR own.own IS NOT NULL
OR fac.base_name IS NOT NULL
ORDER BY usp.parcel_id
;
--SELECT * FROM #mil ORDER BY parcel_id

--UPDATE DEVELOPMENT TYPE ID 2015 MIL
UPDATE usp
	SET usp.development_type_id_2015 = mil.development_type_id_2015_mil
FROM urbansim.parcels AS usp
JOIN #mil AS mil ON usp.parcel_id = mil.parcel_id
;
SELECT * FROM urbansim.parcels WHERE development_type_id_2015 IN(23, 29)
;
DROP TABLE #mil
;


--SET THE DEVELOPMENT TYPE ID AND LU FROM PRIORITY FOR 2017
--DO SPATIAL JOIN, GET PRIORITY, STORE TO TEMP TABLE
DROP TABLE IF EXISTS #priority
;
SELECT
	--lc.parcelID AS parcel_id_2017					--parcel_id_2017
	usp.parcel_id									--parcel_id_2015
	,dev.priority AS p
	,dev.development_type_id
	,lc.lu
	,lc.acres
	,ROW_NUMBER() OVER (PARTITION BY usp.parcel_id ORDER BY priority, acres DESC) AS rownum		--USE 2015 parcel_id
INTO #priority
FROM gis.ludu2017 AS lc								--LUDU 2017
LEFT JOIN ref.development_type_lu_code xref 
	ON lc.lu = xref.lu_code
LEFT JOIN ref.development_type dev 
	ON xref.development_type_id = dev.development_type_id
JOIN gis.ludu2017points AS lcp
	ON lc.subParcel = lcp.subParcel
JOIN urbansim.parcels AS usp
	ON usp.shape.STIntersects(lcp.shape) = 1		--USE gis.ludu2017points
ORDER BY lc.parcelID
;

--GET PRIORITY AND COPY VALUES
UPDATE
    usp
SET 
	usp.development_type_id_2017 = p.development_type_id
	,usp.lu_2017 = p.lu
FROM
    urbansim.parcels usp 
LEFT JOIN
	(SELECT * FROM #priority WHERE rownum = 1) AS p
ON usp.parcel_id = p.parcel_id
;

DROP TABLE IF EXISTS #priority
;

--RETURN VALUES
SELECT * FROM urbansim.parcels WHERE lu_2015 <> lu_2017
;


--SET THE DEVELOPMENT TYPE ID FOR MILITARY 2017
DROP TABLE IF EXISTS #mil
;
WITH  lu AS(
	SELECT
		usp.parcel_id
		,MIN(lp.lu) AS lu							--MAY HAVE MULTIPLE MIL LU
	FROM GIS.ludu2017points AS lp
	JOIN urbansim.parcels AS usp
		ON usp.shape.STIntersects(lp.Shape) = 1
	WHERE lp.lu BETWEEN 6700 AND 6709				--GQ Military Barracks
	OR lp.lu = 1403									--Military Use
	GROUP BY usp.parcel_id
	--ORDER BY usp.parcel_id
)
, own AS(
	SELECT
		usp.parcel_id
		,MIN(lp.genOwnID) AS own
	FROM GIS.ludu2017points AS lp
	JOIN urbansim.parcels AS usp
		ON usp.shape.STIntersects(lp.Shape) = 1
	WHERE lp.genOwnID = 41
	GROUP BY usp.parcel_id
	--ORDER BY usp.parcel_id
)
, fac AS(
	SELECT
		usp.parcel_id
		,base_name
	FROM spacecore.GIS.military_facility_parcel_2017_2015 AS fac
	JOIN urbansim.parcels AS usp
		ON usp.parcel_id = fac.parcel_id
	WHERE fac.mil_base = 1
	--ORDER BY usp.parcel_id
)
SELECT
	usp.parcel_id
	,lu.lu
	,own.own
	,fac.base_name
	,usp.lu_2017 AS lu_2017_all
	,usp.du_2017
	,usp.development_type_id_2017
	,CASE
		WHEN usp.development_type_id_2017 IN (23,29) THEN usp.development_type_id_2017 
		WHEN usp.du_2017 > 0 THEN 23
		ELSE 29
	END AS development_type_id_2017_mil
INTO #mil
FROM urbansim.parcels As usp
LEFT JOIN lu ON usp.parcel_id = lu.parcel_id
LEFT JOIN own ON usp.parcel_id = own.parcel_id
LEFT JOIN fac ON usp.parcel_id = fac.parcel_id
WHERE lu.lu IS NOT NULL
OR own.own IS NOT NULL
OR fac.base_name IS NOT NULL
ORDER BY usp.parcel_id
;

--SELECT * FROM #mil ORDER BY parcel_id

--UPDATE DEVELOPMENT TYPE ID 2017 MIL
UPDATE usp
	SET usp.development_type_id_2017 = mil.development_type_id_2017_mil
FROM urbansim.parcels AS usp
JOIN #mil AS mil ON usp.parcel_id = mil.parcel_id
;
SELECT * FROM urbansim.parcels WHERE development_type_id_2017 IN(23, 29)
;
DROP TABLE #mil
;

--SET DU FROM LUDU_2017
/*
DU_2017 IS UPDATED LATER, IN SCRIPT:
capacity_2017_into_2015.sql
*/


/*
--EXCLUDE ROAD RIGHT OF WAY RECORDS
DELETE FROM urbansim.parcels
WHERE development_type_id_2015 = 24 --Transportation Right of Way
*/

--CREATE A PRIMARY KEY SO WE CAN CREATE A SPATIAL INDEX
ALTER TABLE urbansim.parcels ADD CONSTRAINT pk_urbansim_parcels_parcel_id PRIMARY KEY CLUSTERED (parcel_id) 

--SET THE SHAPES TO BE NOT NULL SO WE CAN CREATE A SPATIAL INDEX
ALTER TABLE urbansim.parcels ALTER COLUMN shape geometry NOT NULL
ALTER TABLE urbansim.parcels ALTER COLUMN centroid geometry NOT NULL

--SELECT max(x_coord), min(x_coord), max(y_coord), min(y_coord) from gis.parcels

CREATE SPATIAL INDEX [ix_spatial_urbansim_parcels_centroid] ON [urbansim].[parcels]
(
    [centroid]
) USING  GEOMETRY_GRID
    WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
    CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
;
CREATE SPATIAL INDEX [ix_spatial_urbansim_parcels_shape] ON [urbansim].[parcels]
(
    [shape]
) USING  GEOMETRY_GRID
    WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
    CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

--SPATIAL GET BLOCKID
--ALTER TABLE urbansim.parcels ALTER COLUMN block_id nvarchar(15) NOT NULL	
UPDATE
    usp 
SET
    usp.block_id = b.blockid
FROM
    urbansim.parcels usp
LEFT JOIN (SELECT BLOCKID10 blockid, Shape FROM ref.blocks) b ON b.shape.STIntersects(usp.centroid) = 1
;
--SPATIAL GET NEAREST BLOCKID, IF NULL
--SELECT * FROM urbansim.parcels WHERE block_id IS NULL
WITH near AS(
	SELECT row_id, parcel_id, block_id, dist 
	FROM (
		SELECT
			ROW_NUMBER() OVER (PARTITION BY parcels.parcel_id ORDER BY parcels.parcel_id, parcels.centroid.STDistance(blocks.shape)) row_id
			,parcels.parcel_id
			,blocks.blockid10 AS block_id
			,parcels.centroid.STDistance(blocks.shape) AS dist
		FROM urbansim.parcels 
			JOIN ref.blocks
			ON parcels.centroid.STBuffer(300).STIntersects(blocks.shape) = 1	--CHECK IF BUFFERDIST IS SUFFICIENT
		WHERE parcels.block_id IS NULL
			) x
	WHERE row_id = 1
)
UPDATE
    usp 
SET
    usp.block_id = near.block_id
FROM
    urbansim.parcels usp
	,near
WHERE
	usp.parcel_id = near.parcel_id
AND
	usp.block_id IS NULL
;
--SPATIAL CREATE MGRA FIELD
UPDATE
    usp 
SET
    usp.mgra_id = mgra.mgra
FROM
    urbansim.parcels usp
LEFT JOIN (SELECT zone as mgra, shape FROM data_cafe.ref.geography_zone z INNER JOIN data_cafe.ref.geography_type t ON z.geography_type_id = t.geography_type_id WHERE t.geography_type = 'mgra' and t.vintage = 13) mgra ON mgra.shape.STIntersects(usp.centroid) = 1
;
--USE DATA_CAFE XREF TO BUILD ZONE FIELDS
UPDATE
    usp
SET
    usp.luz_id = xref.luz_13,
    usp.msa_id = xref.msa_modeling_1
FROM
    urbansim.parcels usp
LEFT JOIN data_cafe.ref.vi_xref_geography_mgra_13 xref ON usp.mgra_id = xref.mgra_13
;
/*
--SPATIAL ZONING
UPDATE
	usp
SET
    usp.zone = usz.zone
FROM
	urbansim.parcels usp
	LEFT JOIN [spacecore].[staging].[zoning_from_postgres] usz ON usz.[geom].STIntersects(usp.centroid) = 1
;
*/
/*
--IF ZONING IS NULL, GRAB NEAREST
WITH near AS(
	SELECT row_id, parcel_id, zone, dist 
	FROM (
		SELECT
			ROW_NUMBER() OVER (PARTITION BY parcels.parcel_id ORDER BY parcels.parcel_id, parcels.centroid.STDistance(zoning.geom)) row_id
			,parcels.parcel_id
			,zoning.zone
			,parcels.centroid.STDistance(zoning.geom) AS dist
		FROM urbansim.parcels 
			JOIN [spacecore].[staging].[zoning_from_postgres] AS zoning
			ON parcels.centroid.STBuffer(300).STIntersects(zoning.geom) = 1	--CHECK IF BUFFERDIST IS SUFFICIENT
		WHERE parcels.zone IS NULL
			) x
	WHERE row_id = 1
)
UPDATE
    usp 
SET
    usp.zone = near.zone
FROM
    urbansim.parcels usp
	,near
WHERE
	usp.parcel_id = near.parcel_id
AND
	usp.zone IS NULL
*/
		
/*
--JURISDICTION ID FROM ZONE
UPDATE
	usp
SET
	usp.jurisdiction_id = LEFT(usp.zone, CHARINDEX('_', usp.zone) - 1)
FROM
	urbansim.parcels AS usp
*/

--CREATE CONSTRAINTS
UPDATE
    usp
SET
    usp.proportion_undevelopable = cons.constrained_area/usp.parcel_acres
FROM
    urbansim.parcels usp
JOIN (SELECT parcel_id, geometry::UnionAggregate(constrained_area).STArea() / 43560. constrained_area 
	FROM
		(
		SELECT
		  usp.parcel_id
		  ,cons.fid
		  ,cons.geom.STIntersection(usp.shape) constrained_area
		FROM
		  urbansim.parcels usp
		  LEFT JOIN gis.devcons cons ON cons.geom.STIntersects(usp.shape) = 1
		WHERE
		  cons.fid is not null) x
	GROUP BY parcel_id) cons
ON usp.parcel_id = cons.parcel_id

--CONTROL TO 1 FOR 100%
UPDATE
    usp
SET 
	proportion_undevelopable = 1.0
FROM
	urbansim.parcels usp
WHERE
	proportion_undevelopable > 1


--SET MILITARY LANDS TO UNDEVELOPABLE
UPDATE
	usp
SET
	usp.proportion_undevelopable = 1
FROM 
	urbansim.parcels usp
WHERE
	development_type_id_2015 = 29

--/*
--SET APN
UPDATE
	usp
SET
	usp.apn  = lcp.apn
FROM 
	urbansim.parcels usp
	LEFT JOIN (SELECT 
					APN
					,parcelid
				FROM(
					SELECT
						APN
						,parcelid
						,ROW_NUMBER() OVER(PARTITION BY parcelid ORDER BY APN) AS row_num
					FROM GIS.ludu2015
					WHERE APN > 0) x
				WHERE row_num = 1
				) lcp
	ON usp.parcel_id = lcp.PARCELID 
--*/


/*#################### CALCULATE DISTANCES ####################*/
--CALCULATE DISTANCE TO COAST
WITH coast AS(
	SELECT geometry::UnionAggregate(coast.SHAPE) AS shape
	FROM GIS.coast
)
UPDATE parcels
SET distance_to_coast = parcels.centroid.STDistance(coast.shape)
FROM urbansim.parcels
	,coast