USE spacecore

SELECT [property_id]
	,[building_address]
	,[building_class]
	,[building_location]
	,[building_name]
	,[building_park]
	,[building_status]
	,[city]
	,[latitude]
	,[longitude]
	,[centroid]
	,[parcel_id]
	,[subparcel_id]
INTO input.costar_locate
FROM input.costar
WHERE parcel_id IS NULL

--ADD ID TO USE AS OBJECT IN SPATIAL VIEWER
ALTER TABLE input.costar_locate 
ADD id  int IDENTITY(1,1)

--->>>
--CREATE A PRIMARY KEY SO WE CAN CREATE A SPATIAL INDEX
ALTER TABLE input.costar_locate ADD CONSTRAINT pk_input_costarlocate_id PRIMARY KEY CLUSTERED (id) 

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

--GET PARCEL_ID FROM NEAREST PARCEL
UPDATE
	c
SET
	c.parcel_id = p.parcel_id
FROM
	input.costar_locate c
JOIN (
	SELECT row_id, id, parcel_id, dist 
	FROM (
		SELECT
			ROW_NUMBER() OVER (PARTITION BY c.id ORDER BY c.id, c.centroid.STDistance(p.shape)) row_id
			,c.id
			,p.parcel_id
			,c.centroid.STDistance(p.shape) AS dist
		FROM urbansim.parcels p
			INNER JOIN input.costar_locate c ON c.centroid.STBuffer(1000).STIntersects(p.shape) = 1) x
	WHERE row_id = 1) p
ON c.id = p.id

