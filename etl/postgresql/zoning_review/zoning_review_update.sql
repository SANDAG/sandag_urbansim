-- ZONING SHAPE REVIEW
SELECT zoning_id
	,jurisdiction_id
	,zone_code
	--,shape
	,notes
	,CASE 
		WHEN UPPER(notes) LIKE '%NOT FOUND%' THEN 'NOT FOUND'
		WHEN SHAPE IS NULL THEN 'ADDED'
	END AS shape_review
FROM urbansim.zoning
--WHERE UPPER(notes) LIKE '%FIND%'
ORDER BY zoning_id

/* START ZONING REVIEW LOAD */
--copy confluence table to CSV

--CREATE TABLE TO LOAD REVIEW
CREATE TABLE staging.zoning_review (
jurisdiction_id int
,zoning_id varchar
,max_dua double precision
,avg_hiden double precision
,min_hiden double precision
,max_hiden double precision
,assigned varchar
,max_dua_rev double precision
,max_units_rev double precision
,notes text
);

--CLEAN UP TABLE
TRUNCATE staging.zoning_review;

--INSERT CSV, OPTION FROM SERVER
--PSQL
\COPY staging.zoning_review FROM '//socioeca8/E/staging/zoning_review.csv' WITH DELIMITER ',' CSV header; 

--INSERT CSV, OPTION FROM OTHER
--PSQL
\COPY staging.zoning_review FROM 'E:/apps/sandag_urbansim/etl/postgresql/zoning_review/zoning_review.csv' WITH DELIMITER ',' CSV header;

--CHECK
SELECT * FROM staging.zoning_review
;
--CHECK
SELECT DISTINCT zoning_id
FROM staging.zoning_review
;

--CHECK FOR DUPLICATES
SELECT 
	zoning_id
	,jurisdiction_id
	,max_dua
	,notes
	,ROW_NUMBER() OVER(PARTITION BY zoning_id ORDER BY jurisdiction_id) AS RowNumber
FROM staging.zoning_review
ORDER BY rownumber DESC, zoning_id 



/* START ZONING REVIEW UPDATE */
--COPY TO STAGING TABLE
SELECT * 
INTO staging.zoning
FROM urbansim.zoning

--ADD TMP COLUMNS
ALTER TABLE staging.zoning ADD COLUMN max_dua_tmp numeric
ALTER TABLE staging.zoning ADD COLUMN max_res_units_tmp integer
ALTER TABLE staging.zoning ADD COLUMN notes_tmp text

--JOIN TABLES
SELECT
	z.zoning_id
	,z.max_dua
	,z.max_dua_tmp
	,z.max_res_units
	,z.max_res_units_tmp
	,c.max_dua_rev	
	,c.max_units_rev
	,z.notes
	,z.tmp_notes
	,c.notes
	,z.notes_tmp
	,c.assigned
FROM staging.zoning z
JOIN staging.zoning_review c
ON z.zoning_id = c.zoning_id

--COPY DUA VALUES
UPDATE staging.zoning z
SET max_dua_tmp = c.max_dua_rev
	,max_res_units_tmp = c.max_units_rev
	,notes_tmp = CONCAT(z.notes , ' -ewe: ' , c.notes)
FROM staging.zoning_conf c
WHERE z.zoning_id = c.zoning_id
AND 	--ADD FILTER HERE
--RUN AGAIN, COPY TO ACTUAL COLUMNS

--DROP TMP COLUMNS
ALTER TABLE staging.zoning DROP COLUMN max_dua_tmp
ALTER TABLE staging.zoning DROP COLUMN max_res_units_tmp
ALTER TABLE staging.zoning DROP COLUMN notes_tmp

