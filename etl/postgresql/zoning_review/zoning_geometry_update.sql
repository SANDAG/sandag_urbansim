/* ########## 1- CONVERT FROM GDB TO SHP ########## */
--MANUAL EXPORT

/* ########## 2- LOAD FROM SHP TO SQL ########## */
--LOAD SHP TO POSTGRESQL
ogr2ogr -f PostgreSQL PG:"dbname='urbansim' host='socioeca8' port='5432' user='urbansim_user' password='urbansim'" E:\data\urbansim_data_development\zoning\zoning_update_geo.shp -nln zoning_review_geo -lco SCHEMA="staging" -s_srs EPSG:2230 -t_srs EPSG:2230 -lco OVERWRITE=YES -OVERWRITE
--IF LOAD ERROR ON MULTISHAPE OR OVERFLOW NOT FIXABLE< TRY MSSQL

--LOAD SHP TO MSSQL
E:\OSGeo4W64\bin\ogr2ogr.exe -f MSSQLSpatial "MSSQL:server=sql2014a8;database=spacecore;trusted_connection=yes" E:\data\urbansim_data_development\zoning\zoning_update_geo.shp -nln zoning_review_geo -lco SCHEMA="staging" -s_srs EPSG:2230 -t_srs EPSG:2230 -lco OVERWRITE=YES -OVERWRITE
--IF IF LOAD ERROR CONVERTING NUMERIC TO NUMERIC, 
--DELETE GEOMETRY DERIVED COLUMNS (AREA, PERIMETER...) FROM SOURCE, 
--AND/OR MODIFY COLUMNS IN NEW TABLE
--[ogr_fid]
--[ogr_geometry]
--[zoneid]
--ALTER TABLE [staging].[zoning_review_geo] ALTER COLUMN [jurisdict] numeric(38,19)
--[zonecode]
--ALTER TABLE [staging].[zoning_review_geo] ALTER COLUMN [regionid] numeric(38,19)
--ALTER TABLE [staging].[zoning_review_geo] ALTER COLUMN [orig_fid] numeric(38,19)

--TRUNCATE TABLE
TRUNCATE TABLE [staging].[zoning_review_geo]

--CHECK TABLE
SELECT * FROM [staging].[zoning_review_geo]

--RELOAD SHP TO MSSQL WITH APPEND
E:\OSGeo4W64\bin\ogr2ogr.exe -f MSSQLSpatial "MSSQL:server=sql2014a8;database=spacecore;trusted_connection=yes" E:\data\urbansim_data_development\zoning\zoning_update_geo.shp -nln zoning_review_geo -lco SCHEMA="staging" -s_srs EPSG:2230 -t_srs EPSG:2230 -append

/* ########## 3- LOAD FROM SQL TO POSTGRESQL ########## */
--USE ETL TOOL urbansim_etl.py


/* ########## 4- JOIN NEW GEOMETRIES TO UPDATED ZONING(ATTRIBUTES) ########## */
SELECT z.zoning_id AS zoning
	,r.zoneid AS geo
	,z.shape_review AS zoning_shape_review
	--,z.geometry_old				--INCLUDE GEOMETRY 1/4
	--,r.geometry_new				--INCLUDE GEOMETRY 2/4
	,CASE
		WHEN z.zoning_id IS  NULL AND r.zoneid IS NULL THEN '???'
		WHEN z.zoning_id = r.zoneid AND z.SHAPE IS NULL THEN 'READY ADDED'
		WHEN z.zoning_id = r.zoneid AND UPPER(notes) LIKE '%NOT FOUND%' THEN 'READY NOT FOUND'
		WHEN z.zoning_id = r.zoneid THEN 'READY'
		WHEN z.zoning_id IS NOT NULL AND UPPER(notes) LIKE '%NOT FOUND%' THEN 'NOT FOUND'
		WHEN z.zoning_id IS NOT NULL AND z.SHAPE IS NULL AND r.zoneid IS NULL THEN 'HAVE ATT NEED SHAPE'
		WHEN z.zoning_id IS NULL AND r.zoneid IS NOT NULL THEN 'HAVE SHAPE NEED ATT'
		ELSE '--'
	END AS review
INTO staging.zoning_review_geo_join			--LOAD INTO NEW TABLE
FROM
(
	SELECT zoning_id
		,jurisdiction_id
		,zone_code
		,shape
		--,shape AS geometry_old		--INCLUDE GEOMETRY 3/4
		,notes
		,CASE 
			WHEN UPPER(notes) LIKE '%NOT FOUND%' THEN 'NOT FOUND'
			WHEN SHAPE IS NULL THEN 'ADDED'
		END AS shape_review
	FROM staging.zoning
) AS z
FULL OUTER JOIN (SELECT 
		REPLACE(REPLACE(zoneid, '.', '_'), '/', '_') AS zoneid 
		FROM staging.zoning_review_geo 
		GROUP BY zoneid) AS r
--FULL OUTER JOIN (SELECT zoneid, ST_Union(wkb_geometry) AS geometry_new FROM staging.zoning_review_geo GROUP BY zoneid) AS r	--INCLUDE GEOMETRY 3/4
ON z.zoning_id = r.zoneid
ORDER BY 4,1,2
--ORDER BY COALESCE(z.zoning_id, r.zoneid)

--CHECK RESULTS
SELECT *  FROM staging.zoning_review_geo_join