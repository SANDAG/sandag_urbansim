/*** CONVERT MULTIPART POLYGONS TO SINGLEPART ***/

--CHECK BUILDINGID
SELECT COUNT(*) FROM urbansim.buildings
SELECT COUNT(DISTINCT building_id) FROM urbansim.buildings

--BREAK MULTIPART POLYGONS
SELECT 
	[development_type_id]
	,b.[parcel_id]
	,b.[improvement_value]
	,b.[residential_units]
	,b.[residential_sqft]
	,b.[non_residential_sqft]
	,b.[job_spaces]
	,b.[non_residential_rent_per_sqft]
	,b.[price_per_sqft]
	,b.[stories]
	,b.[year_built]
	,b.[data_source]
	,b.[subparcel_id]
	,b.[luz_id]
	,b.[building_id]
	,b.[mgra_lc]
	,b.[mgra]
	,b.[du_lc]
	--,b.[shape]
	,b.[centroid]
	,b.shape.STGeometryN(n.numbers) shape
INTO staging.buildings_adj1
FROM urbansim.buildings b
JOIN [ref].[numbers] n
ON n.numbers <= b.shape.STNumGeometries()
ORDER BY b.building_id


--SELECT LARGER PARTS, DISCARD SMALLER
SELECT *
INTO staging.buildings_adj1
FROM (
	SELECT * 
	FROM (
		SELECT
			ROW_NUMBER() OVER (PARTITION BY building_id ORDER BY building_id, shape.STArea() DESC) row_id
				,[development_type_id]
				,[parcel_id]
				,[improvement_value]
				,[residential_units]
				,[residential_sqft]
				,[non_residential_sqft]
				,[job_spaces]
				,[non_residential_rent_per_sqft]
				,[price_per_sqft]
				,[stories]
				,[year_built]
				,[data_source]
				,[subparcel_id]
				,[luz_id]
				,[building_id]
				,[mgra_lc]
				,[mgra]
				,[du_lc]
				,[shape]
				,[centroid]
				,shape.STArea() area
		FROM staging.buildings_adj1) x
	WHERE row_id = 1
	) x

--ADD CENTROIDS
UPDATE [staging].[buildings_adj2]
SET centroid = shape.STCentroid()

--CREATE A PRIMARY KEY SO WE CAN CREATE A SPATIAL INDEX
ALTER TABLE [staging].[buildings_adj2] ADD CONSTRAINT pk_staging_buildings_adj2_building_id PRIMARY KEY CLUSTERED (building_id) 

--SET THE SHAPES TO BE NOT NULL SO WE CAN CREATE A SPATIAL INDEX
ALTER TABLE [staging].[buildings_adj2] ALTER COLUMN shape geometry NOT NULL
ALTER TABLE [staging].[buildings_adj2] ALTER COLUMN centroid geometry NOT NULL

--SELECT max(x_coord), min(x_coord), max(y_coord), min(y_coord) from gis.parcels

CREATE SPATIAL INDEX [ix_spatial_staging_buildings_adj2_shape] ON [staging].[buildings_adj2]
(
    shape
) USING  GEOMETRY_GRID
    WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
    CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

CREATE SPATIAL INDEX [ix_spatial_staging_buildings_adj2_centroid] ON [staging].[buildings_adj2]
(
    centroid
) USING  GEOMETRY_GRID
    WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
    CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]



/*** CENTROID CHECK ***/

--CENTROID CHECK, WITHIN SUBPARCEL	--110
SELECT c.subparcel_id centroid
	, b.subparcel_id subparcel
FROM	
	[staging].[buildings_adj2] c
JOIN
	[staging].[buildings_adj2] b
ON c.centroid.STWithin(b.shape) = 1
WHERE b.subparcel_id != c.subparcel_id
ORDER BY c.subparcel_id



/*** TODO:
FIX BAD CENTROIDS, REPLACE WITH POINT ON SURFACE***/

--ALLOW NULLS TO AVOID ERROR
ALTER TABLE [staging].[buildings_adj2] ALTER COLUMN centroid geometry NOT NULL

--CENTROID FIX WITH POINT ON SURFACE
UPDATE 
	usb
SET 
	usb.centroid = usb.shape.STBuffer(-10).STPointOnSurface()
FROM
	[staging].[buildings_adj2] usb
WHERE
	subparcel_id IN(
					SELECT 
						c.subparcel_id
					FROM	
						[staging].[buildings_adj2] c
					JOIN
						[staging].[buildings_adj2] b
					ON c.centroid.STWithin(b.shape) = 1
					WHERE b.subparcel_id != c.subparcel_id
					) 
;

--BUILDING CENTROID, GET SUBPARCEL, DU FROM LANDCORE
UPDATE
	usb
SET
	usb.subparcel_id = l.subParcel
	,usb.du_lc = l.du
FROM
	[staging].[buildings_adj2] usb
JOIN (SELECT subParcel, du, Shape FROM [GIS].[landcore]) l on l.Shape.STIntersects(usb.centroid) = 1

--CHECK TOTALS
SELECT SUM(du) FROM [GIS].[landcore]
SELECT SUM(du_lc) FROM [staging].[buildings_adj2]
SELECT SUM(du_lc) FROM (SELECT DISTINCT subparcel_id, du_lc FROM [staging].[buildings_adj2]) x


/*** GRAB LU FROM LANDCORE SUBPARCEL USING POINTONSURFACE FOR ALL RECORDS ***/

--ADD COLUMN TO HOLD LU FROM POINTONSURFACE
ALTER TABLE [staging].[buildings_adj2]
ADD subparcel_id_pos int
ALTER TABLE [staging].[buildings_adj2]
ADD du_lc_pos int

--BUILDING POINTONSURFACE, GET SUBPARCEL, DU FROM LANDCORE
UPDATE
	usb
SET
	usb.subparcel_id_pos = l.subParcel
	,usb.du_lc_pos = l.du
FROM
	[staging].[buildings_adj2] usb
JOIN (SELECT subParcel, du, Shape FROM [GIS].[landcore]) l on l.Shape.STIntersects(usb.shape.STBuffer(-10).STPointOnSurface()) = 1