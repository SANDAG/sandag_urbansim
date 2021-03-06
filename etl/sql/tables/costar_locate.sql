USE spacecore

IF OBJECT_ID('input.costar_locate') IS NOT NULL
	DROP TABLE input.costar_locate
GO
SELECT [property_id]
	,[building_address]
	,[building_class]
	,[building_location]
	,[building_name]
	,[building_park]
	,[building_status]
	,[city]
	,[propertytype]
	,[secondary_type]
	,[centroid]
	,[parcel_id]
INTO input.costar_locate
FROM input.costar
WHERE location = 'nearest'

--->>>
--CREATE A PRIMARY KEY SO WE CAN CREATE A SPATIAL INDEX
ALTER TABLE input.costar_locate ADD CONSTRAINT pk_input_costarlocate_id PRIMARY KEY CLUSTERED (property_id) 

--SET THE SHAPES TO BE NOT NULL SO WE CAN CREATE A SPATIAL INDEX
    ALTER TABLE input.costar_locate ALTER COLUMN centroid geometry NOT NULL

--SELECT max(x_coord), min(x_coord), max(y_coord), min(y_coord) from gis.parcels

CREATE SPATIAL INDEX [ix_spatial_input_costarlocate_centroid] ON input.costar_locate
(
    [centroid]
) USING  GEOMETRY_GRID
    WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
    CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
---<<<

--ADD FIELD TO WRITE DEV_TYPE, DISTANCE
ALTER TABLE input.costar_locate ADD dev_type_id smallint
ALTER TABLE input.costar_locate ADD dev_type nvarchar(50)
ALTER TABLE input.costar_locate ADD dist numeric(38,18)

--GET PARCEL_ID FROM NEAREST PARCEL
UPDATE
	c
SET
	c.dist = p.dist
	,c.dev_type_id = p.dev_type
	,c.dev_type = d.name
	--,c.parcel_id = p.parcel_id
FROM
	input.costar_locate c
JOIN (
	SELECT row_id, property_id, parcel_id, dev_type, dist 
	FROM (
		SELECT
			ROW_NUMBER() OVER (PARTITION BY c.property_id ORDER BY c.property_id, c.centroid.STDistance(p.shape)) row_id
			,c.property_id
			,p.parcel_id
			,p.development_type_id dev_type
			,c.centroid.STDistance(p.shape) AS dist
		FROM urbansim.parcels p
			INNER JOIN input.costar_locate c ON c.centroid.STBuffer(1000).STIntersects(p.shape) = 1) x
	WHERE row_id = 1) p
ON c.property_id = p.property_id
JOIN ref.development_type d ON p.dev_type = d.development_type_id


SELECT * FROM input.costar_locate
ORDER BY dev_type, propertytype, secondary_type

