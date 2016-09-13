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
,notes_rev text
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
	,ROW_NUMBER() OVER(PARTITION BY zoning_id ORDER BY jurisdiction_id) AS RowNumber
FROM staging.zoning_review
ORDER BY rownumber DESC, zoning_id 



/* START ZONING REVIEW UPDATE */
--COPY TO STAGING TABLE
SELECT * 
INTO staging.zoning
--FROM urbansim.zoning
FROM staging.zoning_08302016

--ADD TMP COLUMNS
ALTER TABLE staging.zoning ADD COLUMN max_dua_tmp numeric;
ALTER TABLE staging.zoning ADD COLUMN max_res_units_tmp integer;
ALTER TABLE staging.zoning ADD COLUMN notes_tmp text;

--JOIN TABLES
SELECT
	z.zoning_id
	,z.max_dua
	--,z.max_dua_tmp
	,z.max_res_units
	--,z.max_res_units_tmp
	,c.max_dua_rev	
	,c.max_units_rev
	,z.notes
	,z.tmp_notes
	,c.notes_rev
	--,z.notes_tmp
	,c.assigned
	,review_by
	,review_date
FROM staging.zoning z
JOIN staging.zoning_review c
ON z.zoning_id = c.zoning_id
--WHERE z.max_dua > c.max_dua_rev

--COPY DUA VALUES BY ASSIGNED
--CHECK ASSIGNED
SELECT assigned, COUNT(*)
FROM staging.zoning_review
GROUP BY assigned

--ASSIGNED EWE
UPDATE staging.zoning z
SET max_dua_tmp = r.max_dua_rev
	,max_res_units_tmp = r.max_units_rev
	,review_date = '2016-08-12 12:00:00.000' 
	,review_by = 'eric.wendt@sandag.org'
	,notes_tmp = CONCAT(z.notes , ' ewe: ' , r.notes_rev)
FROM staging.zoning_review r
WHERE z.zoning_id = r.zoning_id
AND assigned = 'EWE'
--RUN AGAIN, COPY TO ACTUAL COLUMNS

--ASSIGNED CDAN
UPDATE staging.zoning z
SET max_dua_tmp = r.max_dua_rev
	,max_res_units_tmp = r.max_units_rev
	,review_date = '2016-08-12 12:00:00.000'
	,review_by = 'clint.daniels@sandag.org'
	,notes_tmp = CONCAT(z.notes , ' cdan: ' , r.notes_rev)
FROM staging.zoning_review r
WHERE z.zoning_id = r.zoning_id
AND assigned = 'CDAN'
--RUN AGAIN, COPY TO ACTUAL COLUMNS

--OOPS DELETE PRECEDING 
UPDATE staging.zoning
SET notes = RIGHT(notes, LENGTH(notes)-6)
WHERE notes LIKE ' ewe: %'

--OOPS DELETE PRECEDING 
UPDATE staging.zoning
SET notes = RIGHT(notes, LENGTH(notes)-7)
WHERE notes LIKE ' cdan: %'


--FIND RECORDS THAT DID NOT JOIN
SELECT zoning_id
FROM staging.zoning_review
WHERE zoning_id NOT IN (SELECT zoning_id FROM staging.zoning)

--INSERT ADDITIONAL RECORDS BY USER
INSERT INTO staging.zoning (
	zoning_id
	,max_dua
	,max_res_units
	,notes
	,review_date
	,review_by
)
SELECT
	zoning_id
	,max_dua_rev
	,max_units_rev
	,notes_rev
	,'2016-08-12 12:00:00.000' AS review_date
	,'eric.wendt@sandag.org' AS review_by
FROM staging.zoning_review
WHERE zoning_id NOT IN (SELECT zoning_id FROM staging.zoning)
AND assigned = 'EWE'

--INSERT ADDITIONAL RECORDS BY USER
INSERT INTO staging.zoning (
	zoning_id
	,max_dua
	,max_res_units
	,notes
	,review_date
	,review_by
)
SELECT
	zoning_id
	,max_dua_rev
	,max_units_rev
	,notes_rev
	,'2016-08-12 12:00:00.000' AS review_date
	,'clint.daniels@sandag.org' AS review_by
FROM staging.zoning_review
WHERE zoning_id NOT IN (SELECT zoning_id FROM staging.zoning)
AND assigned = 'CDAN'



--LOOK AT FINAL TABLE
SELECT
	z.zoning_id
	,z.max_dua
	--,z.max_dua_tmp
	,z.max_res_units
	--,z.max_res_units_tmp
	--,c.max_dua_rev	
	--,c.max_units_rev
	,z.notes
	--,z.tmp_notes
	--,c.notes
	--,z.notes_tmp
	--,c.assigned
FROM staging.zoning z
JOIN staging.zoning_review c
ON z.zoning_id = c.zoning_id
--WHERE z.max_dua > c.max_dua_rev

--DROP TMP COLUMNS
ALTER TABLE staging.zoning DROP COLUMN max_dua_tmp;
ALTER TABLE staging.zoning DROP COLUMN max_res_units_tmp;
ALTER TABLE staging.zoning DROP COLUMN notes_tmp;

