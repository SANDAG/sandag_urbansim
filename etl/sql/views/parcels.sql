USE spacecore

--IF OBJECT_ID('urbansim.parcels') IS NOT NULL
--    DROP TABLE urbansim.parcels
--GO

CREATE TABLE urbansim.parcels (
    objectid int not null --PLACEHOLDER FOR OPERATIONS BELOW. DROP AT END OF LOAD
    ,parcel_id int NOT NULL
	,block_id nvarchar(15) --NOT NULL
	,development_type_id int
    ,land_value float
    ,parcel_acres float
    ,region_id integer
    ,mgra_id integer
    ,zoning_id varchar(35)
    ,luz_id smallint
    ,msa_id smallint
    ,proportion_undevelopable float
    ,tax_exempt_status bit
    ,apn nvarchar(10) --This doesn't really work for condo lots
    ,shape geometry
    ,centroid geometry --Placeholder for spatial operations
)

--INSERT WILL THROW A WARNING ABOUT SUMMING ASR_LAND BECAUSE OF NULLS --- THAT'S OKAY
INSERT INTO urbansim.parcels WITH (TABLOCK) (objectid, parcel_id, land_value, region_id, tax_exempt_status)
SELECT 
    min(objectid) objectid
    ,parcelid
    ,sum(ASR_LAND) as land_value
    ,1 as region_id
    ,CASE max(NULLIF(taxstat, 'N')) WHEN 'N' THEN 1 ELSE 0 END as tax_exempt_status
FROM
    GIS.parcels
GROUP BY 
    parcelid

--SET THE SHAPE, CENTROID, AND ACREAGE
UPDATE
  usp
SET
    usp.shape = tmp.shape
    ,usp.centroid = tmp.centroid
    ,usp.parcel_acres = tmp.shape.STArea() / 43560.
FROM
    urbansim.parcels usp
JOIN (SELECT OBJECTID, shape, geometry::Point(X_COORD, Y_COORD, 2230) as centroid FROM gis.parcels WHERE OBJECTID IN (SELECT objectid FROM urbansim.parcels)) tmp ON usp.OBJECTID = tmp.OBJECTID

--SET THE DEVELOPMENT TYPE ID FROM PRIORITY
UPDATE
    usp
SET 
    usp.development_type_id = dev.dev_type_id
FROM
    urbansim.parcels usp 
LEFT JOIN (
		SELECT
		  p_dev_type.parcelID
		  ,dev.development_type_id dev_type_id
		  ,dev.name dev_type
		FROM
		  (SELECT lc.parcelID, MIN(dev.priority) p FROM gis.landcore lc
				INNER JOIN ref.development_type_lu_code xref ON lc.lu = xref.lu_code
				INNER JOIN ref.development_type dev ON xref.development_type_id = dev.development_type_id
				GROUP BY lc.parcelID) p_dev_type
		  INNER JOIN ref.development_type dev ON p_dev_type.p = dev.priority
		  ) dev
ON usp.parcel_id = dev.parcelID

--EXCLUDE ROAD RIGHT OF WAY RECORDS
DELETE FROM urbansim.parcels
WHERE development_type_id = 24 --Transportation Right of Way

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

--SPATIAL CREATE MGRA FIELD
UPDATE
    usp 
SET
    usp.mgra_id = mgra.mgra
FROM
    urbansim.parcels usp
LEFT JOIN (SELECT zone as mgra, shape FROM data_cafe.ref.geography_zone z INNER JOIN data_cafe.ref.geography_type t ON z.geography_type_id = t.geography_type_id WHERE t.geography_type = 'mgra' and t.vintage = 13) mgra ON mgra.shape.STIntersects(usp.centroid) = 1

--USE DATA_CAFE XREF TO BUILD ZONE FIELDS
UPDATE
    usp
SET
    usp.luz_id = xref.luz_13,
    usp.msa_id = xref.msa_modeling_1
FROM
    urbansim.parcels usp
LEFT JOIN data_cafe.ref.vi_xref_geography_mgra_13 xref ON usp.mgra_id = xref.mgra_13


--SPATIAL Zoning
UPDATE
	usp
SET
    usp.zoning_id = usz.zoning_id
FROM
	urbansim.parcels usp
	LEFT JOIN urbansim.zoning usz ON usz.shape.STIntersects(usp.centroid) = 1


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
	development_type_id = 29

--SET APN
UPDATE
	usp
SET
	usp.apn  = gip.apn
FROM 
	urbansim.parcels usp
	LEFT JOIN (SELECT MIN(APN) apn
				,parcelid
				--,SHAPE.STArea() / 43560. ACRES
				FROM GIS.parcels
				WHERE APN IS NOT NULL
				GROUP BY parcelid) gip
	ON usp.parcel_id = gip.PARCELID 
