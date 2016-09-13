/** ########## 1- CONVERT FROM GDB TO SHP ########## **/
--MANUAL EXPORT

/** ########## 2- LOAD FROM SHP TO SQL ########## **/
--LOAD SHP TO POSTGRESQL
ogr2ogr -f PostgreSQL PG:"dbname='urbansim' host='socioeca8' port='5432' user='urbansim_user' password='urbansim'" E:\data\urbansim_data_development\zoning\zoning_review_geo.shp -nln zoning_review_geo -lco SCHEMA="staging" -s_srs EPSG:2230 -t_srs EPSG:2230 -lco OVERWRITE=YES -OVERWRITE
--IF LOAD ERROR ON MULTISHAPE OR OVERFLOW NOT FIXABLE, TRY MSSQL

--LOAD SHP TO MSSQL
E:\OSGeo4W64\bin\ogr2ogr.exe -f MSSQLSpatial "MSSQL:server=sql2014a8;database=spacecore;trusted_connection=yes" E:\data\urbansim_data_development\zoning\zoning_review_geo.shp -nln zoning_review_geo -lco SCHEMA="staging" -s_srs EPSG:2230 -t_srs EPSG:2230 -lco OVERWRITE=YES -OVERWRITE

--IF LOAD ERROR CONVERTING NUMERIC TO NUMERIC, 
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
	E:\OSGeo4W64\bin\ogr2ogr.exe -f MSSQLSpatial "MSSQL:server=sql2014a8;database=spacecore;trusted_connection=yes" E:\data\urbansim_data_development\zoning\zoning_review_geo.shp -nln zoning_review_geo -lco SCHEMA="staging" -s_srs EPSG:2230 -t_srs EPSG:2230 -append

/** ########## 3- LOAD FROM SQL TO POSTGRESQL ########## **/
USE ETL TOOL urbansim_etl.py
	--IF ERROR: "sqlalchemy.exc.DataError: (psycopg2.DataError) Geometry type (MultiPolygon) does not match column type (Polygon)"
	USE QGIS TOOL: "Import into PostGIS"



/** ########## 4- JOIN NEW GEOMETRIES TO UPDATED ZONING(ATTRIBUTES) ########## **/

--COPY [zoneid] VALUES FOR LOOKUP
UPDATE staging.zoning_review_geo
SET zoneid_lookup = zoneid

--COLUMN [zoneid_lookup] CAN BE MODIFIED FOR IMPROVED JOIN
--WILL EXPORT CROSS TABLE WHEN READY

/** ##### 4A- FULL JOIN FOR TABLE STATUS (GET STATISTICS) ##### **/
SELECT z.zoning_id AS zoning
	,r.zoneid_lookup AS geo
	,z.shape_review AS zoning_shape_review
	,CASE
		WHEN z.zoning_id IS  NULL AND r.zoneid_lookup IS NULL THEN '???'
		WHEN z.zoning_id = r.zoneid_lookup AND z.SHAPE IS NULL THEN 'READY ADDED'
		WHEN z.zoning_id = r.zoneid_lookup AND UPPER(notes) LIKE '%NOT FOUND%' THEN 'READY NOT FOUND'
		WHEN z.zoning_id = r.zoneid_lookup THEN 'READY'
		WHEN z.zoning_id IS NOT NULL AND UPPER(notes) LIKE '%NOT FOUND%' THEN 'NOT FOUND'
		WHEN z.zoning_id IS NOT NULL AND z.SHAPE IS NULL AND r.zoneid_lookup IS NULL THEN 'HAVE ATT NEED SHAPE'
		WHEN z.zoning_id IS NULL AND r.zoneid_lookup IS NOT NULL THEN 'HAVE SHAPE NEED ATT'
		ELSE '--'
	END AS review
--INTO staging.zoning_review_geo_join			--LOAD INTO NEW TABLE
FROM
(
	SELECT zoning_id
		,zone_code
		,shape
		,notes
		,CASE 
			WHEN UPPER(notes) LIKE '%NOT FOUND%' THEN 'NOT FOUND'
			WHEN SHAPE IS NULL THEN 'ADDED'
		END AS shape_review
	FROM staging.zoning_base
	ORDER BY zoning_id
) AS z
FULL OUTER JOIN (	
	SELECT
		zoneid_lookup
	FROM staging.zoning_review_geo 
	GROUP BY zoneid_lookup, jurisdict
	) AS r
ON z.zoning_id = r.zoneid_lookup
--ORDER BY 4,1,2
ORDER BY COALESCE(z.zoning_id, r.zoneid_lookup)

--EXPORT CROSS REFERENCE TABLE 
SELECT zoneid
	,zoneid_lookup
--INTO staging.zoning_review_xtable_geo
FROM staging.zoning_review_geo


/** ##### 4B- JOIN ON GEOMETRY TABLE (WRITE TABLE FOR LOAD) ##### **/
SELECT COALESCE(z.zoning_id,r.zoneid_lookup) AS zoning_id
	,CASE
		WHEN z.zoning_id IS  NULL AND r.zoneid_lookup IS NULL THEN '???'
		WHEN z.zoning_id = r.zoneid_lookup AND z.SHAPE IS NULL THEN 'READY ADDED'
		WHEN z.zoning_id = r.zoneid_lookup AND UPPER(notes) LIKE '%NOT FOUND%' THEN 'READY NOT FOUND'
		WHEN z.zoning_id = r.zoneid_lookup THEN 'READY'
		WHEN z.zoning_id IS NOT NULL AND UPPER(notes) LIKE '%NOT FOUND%' THEN 'NOT FOUND'
		WHEN z.zoning_id IS NOT NULL AND z.SHAPE IS NULL AND r.zoneid_lookup IS NULL THEN 'HAVE ATT NEED SHAPE'
		WHEN z.zoning_id IS NULL AND r.zoneid_lookup IS NOT NULL THEN 'HAVE SHAPE NEED ATT'
		ELSE '--'
	END AS review
INTO staging.zoning_review_status			--LOAD INTO NEW TABLE
FROM
(
	SELECT zoning_id
		,ST_Union(shape::geometry) AS shape
		,STRING_AGG(notes, ',') AS notes		--TO CHECK FOR STATUS NOTES
	FROM staging.zoning_base
	GROUP BY zoning_id
) AS z
RIGHT JOIN (
	SELECT
		zoneid_lookup
		,CAST(COALESCE(LEFT(zoneid_lookup, STRPOS(zoneid_lookup,'_') - 1)) AS Integer) AS jurisdiction
		,ST_Union(geom) AS shape
	FROM staging.zoning_review_geo 
	GROUP BY zoneid_lookup, jurisdict
	) AS r
ON z.zoning_id = r.zoneid_lookup
--ORDER BY 4,1,2
ORDER BY COALESCE(z.zoning_id, r.zoneid_lookup)
