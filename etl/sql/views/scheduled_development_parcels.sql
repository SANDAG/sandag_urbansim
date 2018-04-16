/****** LOAD FROM GDB TO MSSQL ******/
--USE OGR2OGR
/*
E:\OSGeo4W64\bin\ogr2ogr.exe -f MSSQLSpatial "MSSQL:server=sql2014a8;database=spacecore;trusted_connection=yes" E:\data\urbansim_data_development\scheduled_development\Parcels\scheddev_parcel_all_modelinput.shp -nln scheduled_development_parcels -lco SCHEMA=staging -lco OVERWRITE=YES -OVERWRITE
*/

USE spacecore
;
/*
/****** FIX SRID  ******/
SELECT DISTINCT ogr_geometry.STSrid FROM staging.scheduled_development_parcels;
UPDATE staging.scheduled_development_parcels SET ogr_geometry.STSrid = 2230;		--NAD83 / California zone 6 (ftUS)
*/

/****** LOAD  ******/
DROP TABLE IF EXISTS urbansim.scheduled_development_parcels
;
SELECT
	site_id
	,CAST(parcel_id AS int) AS parcel_id
	,capacity3 AS capacity_3
	,sfu_effect AS sfu_effective_adj
	,mfu_effect AS mfu_effective_adj
	,mhu_effect AS mhu_effective_adj
	,notes
	,editor
	,ogr_geometry AS shape
INTO urbansim.scheduled_development_parcels
FROM staging.scheduled_development_parcels
;

DROP TABLE IF EXISTS staging.scheduled_development_parcels
;

--SET THE SHAPES TO BE NOT NULL SO WE CAN CREATE A SPATIAL INDEX
ALTER TABLE urbansim.scheduled_development_parcels ALTER COLUMN shape geometry NOT NULL
ALTER TABLE urbansim.scheduled_development_parcels ALTER COLUMN parcel_id int NOT NULL
;

--CREATE A PRIMARY KEY SO WE CAN CREATE A SPATIAL INDEX
ALTER TABLE urbansim.scheduled_development_parcels
ADD CONSTRAINT pk_urbansim_scheddevparcels_parcel_id
PRIMARY KEY CLUSTERED (parcel_id)

--SELECT max(x_coord), min(x_coord), max(y_coord), min(y_coord) from gis.parcels
CREATE SPATIAL INDEX ix_spatial_urbansim_scheddevparcels_shape ON urbansim.scheduled_development_parcels
(
    shape
) USING  GEOMETRY_GRID
    WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
    CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
;


