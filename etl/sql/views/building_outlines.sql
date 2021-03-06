USE spacecore;

/*
--IMPORT FROM GEODATABASE INTO MSSQL
--RUN OSGeo4W Shell GDAL 1.11 OR HIGHER
ogr2ogr -f MSSQLSpatial "MSSQL:server=sql2014a8;database=spacecore;trusted_connection=yes" "M:\RES\DataSolutions\GIS\Data\BuildingFootprints\Data\BuildingOutlines.gdb" "BuildingOutlines" -nln building_outlines -lco SCHEMA=staging -lco OVERWRITE=YES -OVERWRITE
*/

DROP TABLE IF EXISTS [spacecore].[GIS].[building_outlines];
SELECT --TOP 10
	[objectid]
	,CAST([outline_id] AS int) AS [outline_id]
	,CAST([bldgid] AS int) AS [bldgid]
	,[comment]
	,[ogr_geometry] AS shape
	,geometry::STGeomFromText('POINT(' + CAST(CAST(centroid_x AS numeric(18,9)) AS varchar) + ' ' + CAST(CAST(centroid_y AS numeric(18,9)) AS varchar) + ') ', 2230) AS centroid
	--[ogr_fid]
	--,[centroid_x]
	--,[centroid_y]
	--,[area]
	--,[shape_leng]
	--,[shape_length]
	--,[shape_area]
INTO [spacecore].[GIS].[building_outlines]
FROM [spacecore].[staging].[building_outlines]
--WHERE [comment] <> ''
--WHERE [ogr_geometry] IS NULL

SELECT COUNT(*)
FROM [spacecore].[GIS].[building_outlines]


SELECT --TOP 100
	*
FROM [spacecore].[GIS].[building_outlines]
WHERE [ogr_fid] = [objectid]



