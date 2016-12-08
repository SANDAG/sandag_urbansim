﻿DROP TABLE IF EXISTS urbansim.parcel_zoning_schedule;
DROP TABLE IF EXISTS urbansim.zoning_allowed_use;
DROP TABLE IF EXISTS urbansim.zoning;
DROP TABLE IF EXISTS urbansim.zoning_schedule;
DROP TABLE IF EXISTS staging.sr13_capacity;

CREATE TABLE urbansim.zoning_schedule
(
    zoning_schedule_id integer PRIMARY KEY
    ,parent_zoning_schedule_id integer REFERENCES urbansim.zoning_schedule (zoning_schedule_id)    
    ,yr smallint NOT NULL
    ,short_name character varying NOT NULL
    ,long_name text
);
ALTER TABLE urbansim.zoning_schedule
  OWNER TO urbansim_user;
GRANT ALL ON TABLE urbansim.zoning_schedule TO urbansim_user;

--add base zoning schedule entry
INSERT INTO urbansim.zoning_schedule (zoning_schedule_id, yr, short_name, long_name)
VALUES (1, 2016, '2016 Municipal Code Zoning', 'Staff-reviewed and codified zoning ordinances from jurisdictions');


CREATE TABLE urbansim.zoning
(
    zoning_id integer NOT NULL DEFAULT nextval('staging.zoning_zoning_id_seq1'::regclass) PRIMARY KEY
    ,zoning_schedule_id integer NOT NULL REFERENCES urbansim.zoning_schedule (zoning_schedule_id) 
    ,zone character varying
    ,parent_zoning_id integer
    ,parent_zone character varying
    ,jurisdiction_id integer
    ,zone_code character varying
    ,yr_effective smallint
    ,region_id integer
    ,min_lot_size integer
    ,min_far numeric
    ,max_far numeric
    ,min_front_setback numeric
    ,max_front_setback numeric
    ,rear_setback numeric
    ,side_setback numeric
    ,min_dua numeric
    ,max_dua numeric
    ,max_res_units integer
    ,max_building_height integer
    ,zone_code_link character varying
    ,notes text
    ,review_date timestamp without time zone
    ,review_by character varying
    ,shape geography(MultiPolygon,4326)
    ,review text
    ,CONSTRAINT uk_zoning UNIQUE (zoning_schedule_id, zone, yr_effective)
);

ALTER TABLE urbansim.zoning
  OWNER TO urbansim_user;
GRANT ALL ON TABLE urbansim.zoning TO urbansim_user;

CREATE INDEX idx_zoning_shape
ON urbansim.zoning
USING gist (shape);

CREATE INDEX ix_zoning_zoning_schedule_zoning_id
ON urbansim.zoning
USING btree (zoning_schedule_id, zoning_id);

CREATE INDEX ix_zoning_jurisdiction_id
ON urbansim.zoning
USING btree (jurisdiction_id);


--load zoning from staging.zoning
INSERT INTO urbansim.zoning
SELECT 
    DEFAULT
    ,1 AS zoning_schedule_id
    ,zoning_id AS zone
    ,NULL AS parent_zoning_id
    ,NULL AS parent_zone
    ,jurisdiction_id
    ,zone_code
    ,yr_effective
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
    ,shape
    ,review
FROM ref.zoning_base

CREATE TABLE urbansim.zoning_allowed_use
(
    zoning_id integer NOT NULL REFERENCES urbansim.zoning (zoning_id)
    ,zone character varying
    ,zoning_allowed_use_id SERIAL PRIMARY KEY
    ,development_type_id integer NOT NULL REFERENCES urbansim.development_type (development_type_id)
);
ALTER TABLE urbansim.zoning_allowed_use
  OWNER TO urbansim_user;
GRANT ALL ON TABLE urbansim.zoning_allowed_use TO urbansim_user;

CREATE INDEX ix_zoning_allowed_use_zoning_id
ON urbansim.zoning_allowed_use
USING btree (zoning_id);

--load zoning_allowed_use from staging.zoning_allowed_use
INSERT INTO urbansim.zoning_allowed_use
SELECT
    z.zoning_id
    ,a.zoning_id AS zone
    ,a.zoning_allowed_use_id
    ,a.development_type_id
FROM ref.zoningalloweduse_base a
    ,urbansim.zoning z
WHERE z.zone = a.zoning_id;


CREATE TABLE urbansim.parcel_zoning_schedule
(
    zoning_schedule_id integer REFERENCES urbansim.zoning_schedule
    ,parcel_id integer REFERENCES urbansim.parcels (parcel_id)
    ,zoning_id integer REFERENCES urbansim.zoning (zoning_id)
    ,zone character varying
    ,CONSTRAINT uk_parcel_zoning_schedule UNIQUE (zoning_schedule_id, parcel_id, zoning_id)
);
ALTER TABLE urbansim.parcel_zoning_schedule
  OWNER TO urbansim_user;
GRANT ALL ON TABLE urbansim.parcel_zoning_schedule TO urbansim_user;

INSERT INTO urbansim.zoning_schedule (zoning_schedule_id, parent_zoning_schedule_id, yr, short_name, long_name)
VALUES (2, 1, 2012, 'SR13 Final Capacity Based Zoning',
        'Zoning densities on SR13 parcel level GP capacities as reviewed by the jurisdictions');


CREATE TABLE staging.sr13_capacity
(
    parcel_id int PRIMARY KEY
    ,cap_hs int
    ,du int
);

COPY staging.sr13_capacity
FROM 'E:\sr13Capacity.csv' DELIMITER ',' CSV


/*** LOAD INTO ZONING ***/
WITH t AS (
SELECT
    zone
    ,max_res_units
    ,cap_hs
    ,COUNT(*) as num_parcels
    ,COUNT(zone) OVER (PARTITION BY zone) as total_cases
FROM
    (    SELECT
        parcels.parcel_id
        ,zoning.zone
        ,zoning.min_dua
        ,zoning.max_dua
        ,zoning.max_res_units
        ,CASE
             WHEN sr13_capacity.cap_hs IS NULL OR sr13_capacity.cap_hs < 0 THEN 0
             ELSE sr13_capacity.cap_hs
         END as cap_hs
        ,SUM(buildings.residential_units) as residential_units
    FROM
        ref.parcelzoning_base AS parcels
            LEFT JOIN urbansim.buildings
            ON buildings.parcel_id = parcels.parcel_id
                LEFT JOIN ref.zoning_base AS zoning
                ON zoning.zone = parcels.zone
                    LEFT JOIN staging.sr13_capacity
                        ON parcels.parcel_id = sr13_capacity.parcel_id
    GROUP BY
        parcels.parcel_id
        ,parcels.zone
        ,zoning.zone
        ,zoning.min_dua
        ,zoning.max_dua
        ,zoning.max_res_units
        ,sr13_capacity.cap_hs) parcel_zoning_sr13_comparison  
GROUP BY
    zone
    ,max_res_units
    ,cap_hs    
ORDER BY
    zone
    ,num_parcels DESC)
--INSERT INTO urbansim.zoning
SELECT
  2 as zoning_schedule_id
  ,zoning.zone || ' cap_hs ' || t.cap_hs
  ,zoning.zone
  ,jurisdiction_id
  ,zone_code
  ,yr_effective
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
  ,t.cap_hs as max_res_units
  ,max_building_height
  ,zone_code_link
  ,notes
  ,'9/1/16'
  ,'DFL'
  ,NULL
  ,'Override from SR13 capacity where cap_hs <> base zoning max_res_units'
FROM
    ref.zoning_base AS zoning
        INNER JOIN t
        ON t.zone = zoning.zone
WHERE zoning.max_res_units <> t.cap_hs
OR zoning.max_res_units IS NULL


/*** LOAD INTO PARCEL ZONING SCHEDULE ***/
WITH t AS (
SELECT
    zone
    ,max_res_units
    ,cap_hs
    ,COUNT(*) as num_parcels
    ,COUNT(zone) OVER (PARTITION BY zone) as total_cases
FROM
    (    SELECT
        parcels.parcel_id
        ,zoning.zoning_id
        ,zoning.zone
        ,zoning.min_dua
        ,zoning.max_dua
        ,zoning.max_res_units
        ,CASE
             WHEN sr13_capacity.cap_hs IS NULL OR sr13_capacity.cap_hs < 0 THEN 0
             ELSE sr13_capacity.cap_hs
         END as cap_hs
        ,SUM(buildings.residential_units) as residential_units
    FROM
        ref.parcelzoning_base AS parcels
            INNER JOIN urbansim.buildings
            ON buildings.parcel_id = parcels.parcel_id
                LEFT JOIN urbansim.zoning
                ON zoning.zone = parcels.zone
                    LEFT JOIN staging.sr13_capacity
                        ON parcels.parcel_id = sr13_capacity.parcel_id
    GROUP BY
        parcels.parcel_id
        ,parcels.zone
        ,zoning.zoning_id
        ,zoning.zone
        ,zoning.min_dua
        ,zoning.max_dua
        ,zoning.max_res_units
        ,sr13_capacity.cap_hs) parcel_zoning_sr13_comparison  
GROUP BY
    zone
    ,max_res_units
    ,cap_hs    
ORDER BY
    zone
    ,num_parcels DESC)
--INSERT INTO urbansim.parcel_zoning_schedule
SELECT
    2 as zoning_schedule_id
    ,parcels.parcel_id
    ,zoning.zoning_id
    ,zoning.zone
FROM
    ref.parcelzoning_base AS parcels
        INNER JOIN staging.sr13_capacity
        ON sr13_capacity.parcel_id = parcels.parcel_id
            INNER JOIN t
            ON t.zone = parcels.zone
                INNER JOIN urbansim.zoning
                ON zoning.zoning_schedule_id = 2
                AND zoning.zone = t.zone || ' cap_hs ' || t.cap_hs
                AND zoning.max_res_units = sr13_capacity.cap_hs

ORDER BY parcels.parcel_id

/***########## INSERT POLYGONS FOR SCHEDULE 2 START ##########***/	--FIX FOR PARCELS DATASET, NO ZONING DATA!!!!!
/*** 1- INSERT ZONING POLIGONS FROM PARCELS ****/
UPDATE urbansim.zoning z
SET shape = p.shape::geography						--BACK TO GEOGRAPHY
FROM
(
	SELECT  pzs.zoning_id, pzs.zoning_schedule_id
		--pzs.parcel_id
		,ST_Multi(ST_Union((p.shape)::geometry)) AS shape	--TO MULTIPART GEOMETRY
		--, zoning_id, zoning_schedule_id
	FROM urbansim.parcel_zoning_schedule AS pzs
	JOIN (SELECT parcel_id, shape FROM urbansim.parcels) AS p
	ON pzs.parcel_id = p.parcel_id
	GROUP BY pzs.zoning_id, pzs.zoning_schedule_id
)p
WHERE z.zoning_id = p.zoning_id
AND z.zoning_schedule_id = 2

/*** 2- INSERT ADDITIONAL ZONING POLYGONS FROM PARENT;
PARCELS NOT IN ZONING SCHEDULE 2, YES IN ZONING POLYGONS AFFECTED BY IT ****/
INSERT INTO urbansim.zoning
SELECT z2.zoning_schedule_id
	,pz.zoning_id AS zoning_id
	,pz.zoning_id AS parent_zoning_id
	,z1.jurisdiction_id
	,z1.zone_code
	,z1.yr_effective
	,z1.region_id
	,z1.min_lot_size
	,z1.min_far
	,z1.max_far
	,z1.min_front_setback
	,z1.max_front_setback
	,z1.rear_setback
	,z1.side_setback
	,z1.min_dua
	,z1.max_dua
	,z1.max_res_units
	,z1.max_building_height
	,z1.zone_code_link
	,z1.notes
	,z1.review_date
	,z1.review_by
	,pz.shape
	,z1.review
FROM (SELECT zoning_schedule_id, parent_zoning_id
	FROM urbansim.zoning 
	WHERE zoning_schedule_id = 2
	GROUP BY zoning_schedule_id, parent_zoning_id) AS z2
JOIN (SELECT p.zoning_id
		,ST_Multi(ST_Union((p.shape)::geometry)) AS shape
	FROM (SELECT parcel_id, zoning_id, shape FROM urbansim.parcels WHERE parcel_id NOT IN (SELECT parcel_id FROM urbansim.parcel_zoning_schedule WHERE zoning_schedule_id = 2)) p
	JOIN (SELECT parent_zoning_id FROM urbansim.zoning WHERE zoning_schedule_id = 2 GROUP BY parent_zoning_id) AS z
	ON p.zoning_id = z.parent_zoning_id
	GROUP BY p.zoning_id
	) AS pz
ON z2.parent_zoning_id = pz.zoning_id
JOIN (SELECT *
	FROM urbansim.zoning 
	WHERE zoning_schedule_id = 1) AS z1
ON z2.parent_zoning_id = z1.zoning_id
UNION ALL
/*** 3- INSERT ADDITIONAL ZONING POLYGONS FROM SCHEDULE 1 NOT CHANGED IN SCHEDULE 2 ****/
SELECT 2 AS zoning_schedule_id
	,zoning_id
	,zoning_id AS parent_zoning_id
	,jurisdiction_id
	,zone_code
	,yr_effective
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
	,shape
	,'Override from SR13 capacity where cap_hs <> base zoning max_res_units'
FROM urbansim.zoning
WHERE zoning_schedule_id = 1
AND zoning_id NOT IN (SELECT DISTINCT parent_zoning_id FROM urbansim.zoning WHERE zoning_schedule_id = 2)
;





