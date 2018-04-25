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


/****** FIX SITE 15016 ******/
INSERT INTO urbansim.scheduled_development_parcels
SELECT
	site_id
	,parcel_id
	,0
	,0
	,0
	,0
	,'use scheddevassign no res unit'
	,'cchu'
	,shape
FROM urbansim.parcels AS usp
WHERE site_id IN (15005, 15016)
AND parcel_id NOT IN
	(SELECT parcel_id 
	FROM urbansim.scheduled_development_parcels AS sdp 
	WHERE site_id IN (15005, 15016))
ORDER BY
	site_id
	,parcel_id
;

 
/****** INSERT GQ ******/
--LOAD FROM GDB TO MSSQL
--USE OGR2OGR
--LOAD INTO SCHEMA: GIS
/*
E:\OSGeo4W64\bin\ogr2ogr.exe -f MSSQLSpatial "MSSQL:server=sql2014a8;database=spacecore;trusted_connection=yes" E:\data\urbansim_data_development\scheduled_development\Parcels\urbansim_schedev_parcel_civGQ_add.shp -nln schedev_parcel_civGQ_add -lco SCHEMA=GIS -lco OVERWRITE=YES -OVERWRITE
*/
;
--FIX SRID
SELECT DISTINCT ogr_geometry.STSrid FROM [GIS].[schedev_parcel_civgq_add];
UPDATE [GIS].[schedev_parcel_civgq_add] SET ogr_geometry.STSrid = 2230;		--NAD83 / California zone 6 (ftUS)

--ADD COLUMN AND SET TO 0
ALTER TABLE urbansim.scheduled_development_parcels
ADD civGQ int NULL;

UPDATE urbansim.scheduled_development_parcels
SET civGQ = 0;

--CHECK FOR GQ PARCELS DUPLICATES AND DELETE
SELECT *
FROM urbansim.scheduled_development_parcels AS sdp
WHERE EXISTS
	(SELECT parcel_id 
	FROM GIS.schedev_parcel_civgq_add  AS sda
	WHERE sda.parcel_id = sdp.parcel_id)

--Msg 2627, Level 14, State 1, Line 99
--Violation of PRIMARY KEY constraint 'pk_urbansim_scheddevparcels_parcel_id'. Cannot insert duplicate key in object 'urbansim.scheduled_development_parcels'. The duplicate key value is (5209364).

DELETE
FROM urbansim.scheduled_development_parcels
WHERE parcel_id IN(5209364, 5294150)


--INSERT GQ PARCELS
INSERT INTO urbansim.scheduled_development_parcels
SELECT
	site_id
	,parcel_id
	,capacity3
	,sfu_effect
	,mfu_effect
	,mhu_effect
	,notes
	,editor
	,ogr_geometry
	,civgq
FROM GIS.schedev_parcel_civgq_add AS sda
ORDER BY
	site_id
	,parcel_id
;

--FIX GQ FOR PARCEL 5098007
SELECT *
FROM urbansim.scheduled_development_parcels
WHERE parcel_id = 5098007
;

UPDATE urbansim.scheduled_development_parcels
SET civGQ = 138
	,notes= 'Mission Cov apt and senior housing'
WHERE parcel_id = 5098007
;


--***************************************************************************
--CHECKS

--SITE LEVEL
SELECT
	usp.site_id AS site_id_usp
	,sdp.site_id AS site_id_sdp
	,SUM(capacity_2) AS capacity_2
	,SUM(capacity_3) AS capacity_3
	,SUM(COALESCE(capacity_3, capacity_2)) AS cap
FROM urbansim.urbansim.parcel AS usp
FULL OUTER JOIN urbansim.urbansim.scheduled_development_parcel AS sdp
	ON usp.parcel_id = sdp.parcel_id
WHERE usp.site_id IS NOT NULL
AND sdp.site_id IS NULL
GROUP BY
	sdp.site_id
	,usp.site_id
ORDER BY
	sdp.site_id
	,usp.site_id


--PARCEL LEVEL
SELECT
	usp.site_id AS site_id_usp
	,sdp.site_id AS site_id_sdp
	,usp.parcel_id AS parcel_id_usp
	,sdp.parcel_id AS parcel_id_sdp
	,capacity_2
	,capacity_3
	,COALESCE(capacity_3, capacity_2) AS cap
FROM urbansim.urbansim.parcel AS usp
FULL OUTER JOIN urbansim.urbansim.scheduled_development_parcel AS sdp
	ON usp.parcel_id = sdp.parcel_id
WHERE usp.site_id IS NOT NULL
--WHERE usp.site_id = 15016
ORDER BY
	usp.site_id
	,usp.parcel_id

