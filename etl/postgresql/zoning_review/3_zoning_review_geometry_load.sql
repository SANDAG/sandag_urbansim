/** ########## GET GEOMETRY TABLE, JOIN ZONE DATA, LOAD ##########**/
--JOIN AND INSERT
SELECT g.zoneid AS zoning_id				--USE ORIGINAL ZONINGID FOR NAME
	,z.jurisdiction_id
	,z.zone_code
	,z.region_id
	,z.min_lot_size
	,z.min_far
	,z.max_far
	,z.min_front_setback
	,z.max_front_setback
	,z.rear_setback
	,z.side_setback
	,z.min_dua
	,z.max_dua
	,z.max_res_units
	,z.max_building_height
	,z.zone_code_link
	,z.notes
	,z.review_date
	,z.review_by
	,g.geom
	,s.review
	,g.zoneid_lookup
INTO staging.zoning_review_updated
FROM staging.zoning_review_geo AS g
LEFT JOIN staging.zoning_base AS z
ON g.zoneid_lookup = z.zoning_id			--USE LOOKUP ZONINGID FOR JOIN
LEFT JOIN staging.zoning_review_status AS s
ON g.zoneid = s.zoning_id				--USE ORIGINAL ZONINGID FOR JOIN

-- Query returned successfully: 1277 rows affected, 7171 ms execution time.
--WAS EXPECTING 1212

--TRUNCATE TABLE
TRUNCATE TABLE staging.zoning_review_updated

--INSERT FROM GEOMETRY TABLE
INSERT INTO staging.zoning_review_updated(
	zoning_id
	,jurisdiction_id
	,zone_code
	,region_id
	,geom
	,zoneid_lookup
)
SELECT zoneid
	,jurisdict
	,zonecode
	,1 AS region_id
	,geom
	,zoneid_lookup
FROM staging.zoning_review_geo;

--GET VALUES FROM ZONING TABLE
UPDATE staging.zoning_review_updated
SET
	min_lot_size = z.min_lot_size
	,min_far = z.min_far
	,max_far = z.max_far
	,min_front_setback = z.min_front_setback
	,max_front_setback = z.max_front_setback
	,rear_setback = z.rear_setback
	,side_setback = z.side_setback
	,min_dua = z.min_dua
	,max_dua = z.max_dua
	,max_res_units = z.max_res_units
	,max_building_height = z.max_building_height
	,zone_code_link = z.zone_code_link
	,notes = z.notes
	,review_date = z.review_date
	,review_by = z.review_by
FROM staging.zoning_base AS z
WHERE zoneid_lookup = z.zoning_id		--USE LOOKUP ZONINGID FOR JOIN

--GET VALUES FROM STATUS TABLE
UPDATE staging.zoning_review_updated
SET review = s.review
FROM staging.zoning_review_status s
WHERE zoneid_lookup = s.zoning_id		--USE LOOKUP ZONINGID FOR JOIN

/** ########## JURISDICTION_ID = 1 IS ALREADY PROCESSED ##########**/
--DELETE JURISDICTION=1 RECORDS
DELETE FROM staging.zoning_review_updated
WHERE jurisdiction_id = 1

--INSERT FROM ZONING TABLE
INSERT INTO staging.zoning_review_updated
SELECT zoning_id_mod AS zoning_id				--USE ORIGINAL ZONINGID FOR NAME
	,jurisdiction_id
	,zone_code
	,region_id
	,min_lot_size
	,min_far
	,max_far
	,min_front_setback
	,max_front_setback
	,rear_setback
	,side_setback
	,min_dua
	,max_dua
	,max_res_units
	,max_building_height
	,zone_code_link
	,notes
	,review_date
	,review_by
	,(shape::geometry) AS geom
	,NULL AS review
	,zoning_id AS zoneid_lookup				--USE LOOKUP ZONINGID AS LOOKUP
FROM staging.zoning_base
WHERE jurisdiction_id = 1
;

SELECT COUNT(*) FROM staging.zoning_review_updated
