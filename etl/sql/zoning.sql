DROP TABLE IF EXISTS urbansim.parcel_zoning_schedule;
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
    zoning_schedule_id integer NOT NULL REFERENCES urbansim.zoning_schedule (zoning_schedule_id)    
    ,zoning_id character varying PRIMARY KEY
    ,parent_zoning_id character varying
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
    ,CONSTRAINT uk_zoning UNIQUE (zoning_schedule_id, zoning_id, yr_effective)
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
    1 as zoning_schedule_id
    ,zoning_id
    ,NULL as parent_zoning_id
    ,jurisdiction_id
    ,zone_code
    ,2015 AS yr_effective
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
    ,geography AS shape
    ,review
FROM staging.zoning;


CREATE TABLE urbansim.zoning_allowed_use
(
    zoning_id character varying NOT NULL REFERENCES urbansim.zoning (zoning_id)
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
    a.zoning_id
    ,a.zoning_allowed_use_id
    ,a.development_type_id
FROM staging.zoning_allowed_use a
    ,urbansim.zoning z
WHERE z.zoning_id = a.zoning_id;


CREATE TABLE urbansim.parcel_zoning_schedule
(
    zoning_schedule_id integer REFERENCES urbansim.zoning_schedule
    ,parcel_id integer REFERENCES urbansim.parcels (parcel_id)
    ,zoning_id character varying REFERENCES urbansim.zoning (zoning_id)
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



WITH t AS (
SELECT
    zoning_id
    ,max_res_units
    ,cap_hs
    ,COUNT(*) as num_parcels
    ,COUNT(zoning_id) OVER (PARTITION BY zoning_id) as total_cases
FROM
    (    SELECT
        parcels.parcel_id
        ,parcels.parcel_acres
        ,zoning.zoning_id
        ,zoning.min_dua
        ,zoning.max_dua
        ,zoning.max_res_units
        ,CASE
             WHEN sr13_capacity.cap_hs IS NULL OR sr13_capacity.cap_hs < 0 THEN 0
             ELSE sr13_capacity.cap_hs
         END as cap_hs
        ,SUM(buildings.residential_units) as residential_units
    FROM
        urbansim.parcels
            INNER JOIN urbansim.buildings
            ON buildings.parcel_id = parcels.parcel_id
                LEFT JOIN staging.zoning
                ON zoning.zoning_id = parcels.zoning_id
                    LEFT JOIN staging.sr13_capacity
                        ON parcels.parcel_id = sr13_capacity.parcel_id
    GROUP BY
        parcels.parcel_id
        ,parcels.parcel_acres
        ,parcels.zoning_id
        ,zoning.zoning_id
        ,zoning.min_dua
        ,zoning.max_dua
        ,zoning.max_res_units
        ,sr13_capacity.cap_hs) parcel_zoning_sr13_comparison  
GROUP BY
    zoning_id
    ,max_res_units
    ,cap_hs    
ORDER BY
    zoning_id
    ,num_parcels DESC)
INSERT INTO urbansim.zoning
SELECT
  2 as zoning_schedule_id
  ,zoning.zoning_id || ' cap_hs ' || t.cap_hs
  ,zoning.zoning_id
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
  ,shape
  ,'Override from SR13 capacity where cap_hs <> base zoning max_res_units'
FROM
    urbansim.zoning
        INNER JOIN t
        ON t.zoning_id = zoning.zoning_id
WHERE zoning.max_res_units <> t.cap_hs


WITH t AS (
SELECT
    zoning_id
    ,max_res_units
    ,cap_hs
    ,COUNT(*) as num_parcels
    ,COUNT(zoning_id) OVER (PARTITION BY zoning_id) as total_cases
FROM
    (    SELECT
        parcels.parcel_id
        ,parcels.parcel_acres
        ,zoning.zoning_id
        ,zoning.min_dua
        ,zoning.max_dua
        ,zoning.max_res_units
        ,CASE
             WHEN sr13_capacity.cap_hs IS NULL OR sr13_capacity.cap_hs < 0 THEN 0
             ELSE sr13_capacity.cap_hs
         END as cap_hs
        ,SUM(buildings.residential_units) as residential_units
    FROM
        urbansim.parcels
            INNER JOIN urbansim.buildings
            ON buildings.parcel_id = parcels.parcel_id
                LEFT JOIN staging.zoning
                ON zoning.zoning_id = parcels.zoning_id
                    LEFT JOIN staging.sr13_capacity
                        ON parcels.parcel_id = sr13_capacity.parcel_id
    GROUP BY
        parcels.parcel_id
        ,parcels.parcel_acres
        ,parcels.zoning_id
        ,zoning.zoning_id
        ,zoning.min_dua
        ,zoning.max_dua
        ,zoning.max_res_units
        ,sr13_capacity.cap_hs) parcel_zoning_sr13_comparison  
GROUP BY
    zoning_id
    ,max_res_units
    ,cap_hs    
ORDER BY
    zoning_id
    ,num_parcels DESC)
INSERT INTO urbansim.parcel_zoning_schedule
SELECT
    2 as zoning_schedule_id
    ,parcels.parcel_id
    ,zoning.zoning_id
FROM
    urbansim.parcels
        INNER JOIN staging.sr13_capacity
        ON sr13_capacity.parcel_id = parcels.parcel_id
            INNER JOIN t
            ON t.zoning_id = parcels.zoning_id
                INNER JOIN urbansim.zoning
                ON zoning.zoning_schedule_id = 2
                AND zoning.zoning_id = t.zoning_id || ' cap_hs ' || t.cap_hs
                AND zoning.max_res_units = sr13_capacity.cap_hs
--ORDER BY parcels.parcel_id
--LIMIT 1000
/*
SELECT

    zoning_id
    ,max_dua
    ,max_res_units

    ,cap_hs

    ,COUNT(*) as num_parcels

    ,COUNT(zoning_id) OVER (PARTITION BY zoning_id) as total_cases

FROM

    (SELECT

        parcels.parcel_id

        ,parcels.parcel_acres

        ,zoning.zoning_id

        ,zoning.min_dua

        ,zoning.max_dua

        ,zoning.max_res_units

        ,CASE

             WHEN sr13_capacity.cap_hs IS NULL OR sr13_capacity.cap_hs < 0 THEN 0

             ELSE sr13_capacity.cap_hs

         END as cap_hs

        ,SUM(buildings.residential_units) as residential_units

    FROM

        urbansim.parcels

            INNER JOIN urbansim.buildings

            ON buildings.parcel_id = parcels.parcel_id

                LEFT JOIN staging.zoning

                ON zoning.zoning_id = parcels.zoning_id

                    LEFT JOIN staging.sr13_capacity

                        ON parcels.parcel_id = sr13_capacity.parcel_id

    GROUP BY

        parcels.parcel_id

        ,parcels.parcel_acres

        ,parcels.zoning_id

        ,zoning.zoning_id

        ,zoning.min_dua

        ,zoning.max_dua

        ,zoning.max_res_units

        ,sr13_capacity.cap_hs) t   

GROUP BY

    zoning_id
    ,max_dua
    ,max_res_units

    ,cap_hs    

ORDER BY

    zoning_id

    ,num_parcels DESC */