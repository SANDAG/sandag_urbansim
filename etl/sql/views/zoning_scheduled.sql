/* ORIGINALLY WRITTEN IN POSTGRESQL */

DROP TABLE IF EXISTS urbansim.parcel_zoning_schedule2;
DROP TABLE IF EXISTS urbansim.zoning_allowed_use;
DROP TABLE IF EXISTS urbansim.zoning;
DROP TABLE IF EXISTS urbansim.zoning_schedule;
DROP TABLE IF EXISTS urbansim.zoning_parcels;

CREATE TABLE urbansim.zoning_schedule
(
    zoning_schedule_id int PRIMARY KEY
    ,parent_zoning_schedule_id int REFERENCES urbansim.zoning_schedule (zoning_schedule_id)    
    ,yr smallint NOT NULL
    ,short_name varchar(MAX) NOT NULL
    ,long_name varchar(MAX)
);

--ADD BASE ZONING SCHEDULE ENTRY
INSERT INTO urbansim.zoning_schedule (zoning_schedule_id, yr, short_name, long_name)
VALUES (1, 2016, '2016 Municipal Code Zoning', 'Staff-reviewed and codified zoning ordinances from jurisdictions');


CREATE TABLE urbansim.zoning
(
    zoning_id int IDENTITY NOT NULL
    ,zoning_schedule_id int NOT NULL REFERENCES urbansim.zoning_schedule (zoning_schedule_id) 
    ,zone nvarchar(MAX)
    ,parent_zoning_id int
    ,parent_zone nvarchar(MAX)
    ,jurisdiction_id int
    ,zone_code nvarchar(MAX)
    ,yr_effective smallint
    ,region_id int
    ,min_lot_size int
    ,min_far numeric
    ,max_far numeric
    ,min_front_setback numeric
    ,max_front_setback numeric
    ,rear_setback numeric
    ,side_setback numeric
    ,min_dua numeric
    ,max_dua numeric
    ,max_res_units int
    ,cap_hs int
    ,max_building_height int
    ,zone_code_link nvarchar(MAX)
    ,notes text
    ,review_date datetime
    ,review_by nvarchar(MAX)
    ,shape geometry
    ,review text
	,PRIMARY KEY (zoning_id)
    ,CONSTRAINT uk_zoning UNIQUE (zoning_schedule_id, zoning_id, yr_effective)
);

CREATE SPATIAL INDEX [idx_zoning_geom] ON urbansim.zoning(shape)
USING  GEOMETRY_GRID
    WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
    CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

CREATE INDEX ix_zoning_zoning_schedule_zoning_id
ON urbansim.zoning (zoning_schedule_id, zoning_id);

CREATE INDEX ix_zoning_jurisdiction_id
ON urbansim.zoning (jurisdiction_id);

--LOAD ZONING
INSERT INTO urbansim.zoning(
    zoning_schedule_id
    ,zone
    ,parent_zoning_id
    ,parent_zone
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
    ,review)
SELECT 
    1 AS zoning_schedule_id
    ,zone
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
;


CREATE TABLE urbansim.zoning_allowed_use
(
    zoning_id int NOT NULL REFERENCES urbansim.zoning (zoning_id)
    ,zone nvarchar(MAX)
    ,zoning_allowed_use_id int IDENTITY PRIMARY KEY
    ,development_type_id smallint NOT NULL REFERENCES ref.development_type (development_type_id)
);

CREATE INDEX ix_zoning_allowed_use_zoning_id
ON urbansim.zoning_allowed_use(zoning_id);

--LOAD ZONING ALLOWED USE FROM staging.zoning_allowed_use
SET IDENTITY_INSERT urbansim.zoning_allowed_use ON
INSERT INTO urbansim.zoning_allowed_use(
	zoning_id
	,zone
	,zoning_allowed_use_id
	,development_type_id
)
SELECT
    z.zoning_id
    ,a.zone
    ,a.zoning_allowed_use_id
    ,a.development_type_id
FROM ref.zoningalloweduse_base a
    ,urbansim.zoning z
WHERE z.zone = a.zone
SET IDENTITY_INSERT urbansim.zoning_allowed_use OFF;


CREATE TABLE urbansim.parcel_zoning_schedule2
(
    zoning_schedule_id int REFERENCES urbansim.zoning_schedule
    ,parcel_id int REFERENCES urbansim.parcels (parcel_id)
    ,zoning_id int REFERENCES urbansim.zoning (zoning_id)
    ,zone nvarchar(MAX)
    ,CONSTRAINT uk_parcel_zoning_schedule2 UNIQUE (zoning_schedule_id, parcel_id, zoning_id)
);

INSERT INTO urbansim.zoning_schedule (zoning_schedule_id, parent_zoning_schedule_id, yr, short_name, long_name)
VALUES (2, 1, 2012, 'SR13 Final Capacity Based Zoning',
        'Zoning densities on SR13 parcel level GP capacities as reviewed by the jurisdictions');


/*** LOAD INTO ZONING ***/
WITH t AS (
	SELECT
		zone
		,max_res_units
		,cap_hs
		,COUNT(*) as num_parcels
		,COUNT(zone) OVER (PARTITION BY zone) as total_cases
	FROM
		(SELECT
			parcels.parcel_id
			,zoning.zone
			,zoning.min_dua
			,zoning.max_dua
			,zoning.max_res_units
			,CASE
				 WHEN sr13_capacity.sr13_cap_hs_growth_adjusted < 0 THEN 0
				 ELSE sr13_capacity.sr13_cap_hs_growth_adjusted
			 END as cap_hs
		FROM
			ref.sr13_capacity
				JOIN ref.parcelzoning_base AS parcels
				ON parcels.parcel_id = sr13_capacity.ludu2015_parcel_id
					JOIN ref.zoning_base AS zoning
					ON zoning.zone = parcels.zone
		GROUP BY
			parcels.parcel_id
			,zoning.zone
			,zoning.min_dua
			,zoning.max_dua
			,zoning.max_res_units
			,sr13_capacity.sr13_cap_hs_growth_adjusted
		) parcel_zoning_sr13_comparison  
	GROUP BY
		zone
		,max_res_units
		,cap_hs    
	--ORDER BY
	--	zone
	--	,num_parcels DESC
)
INSERT INTO urbansim.zoning(
    zoning_schedule_id
    ,zone
    ,parent_zoning_id
    ,parent_zone
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
    ,cap_hs
    ,max_building_height
    ,zone_code_link
    ,notes
    ,review_date
    ,review_by
    ,shape
    ,review)
SELECT
    2 AS zoning_schedule_id
    ,CONCAT(zoning.zone, ' cap_hs ', t.cap_hs) AS zone
    ,zoning.zoning_id AS parent_zoning_id
    ,zoning.zone AS parent_zone
    ,jurisdiction_id
    ,CONCAT(zone_code, ' cap_hs ', t.cap_hs) AS zone_code
    ,yr_effective
    ,region_id
    ,min_lot_size
    ,min_far
    ,max_far
    ,min_front_setback
    ,max_front_setback
    ,rear_setback
    ,side_setback
    ,NULL AS min_dua
    ,NULL AS max_dua
    ,NULL AS max_res_units
    ,t.cap_hs
    ,max_building_height
    ,zone_code_link
    ,notes
    ,'9/1/16'
    ,'DFL'
    ,NULL
    ,'Override from SR13 capacity where cap_hs <> base zoning max_res_units'
FROM
    (SELECT * FROM urbansim.zoning WHERE zoning_schedule_id = 1) AS zoning
        JOIN t
        ON t.zone = zoning.zone
;

/*** LOAD INTO PARCEL ZONING SCHEDULE ***/
WITH t AS (
    SELECT
        parcels.parcel_id
        ,zoning.zone
        ,zoning.min_dua
        ,zoning.max_dua
        ,zoning.max_res_units
        ,CASE
             WHEN sr13_capacity.sr13_cap_hs_growth_adjusted < 0 THEN 0
             ELSE sr13_capacity.sr13_cap_hs_growth_adjusted
         END as cap_hs
    FROM
        ref.sr13_capacity
            JOIN ref.parcelzoning_base AS parcels
            ON parcels.parcel_id = sr13_capacity.ludu2015_parcel_id
                LEFT JOIN urbansim.zoning
                ON zoning.zone = parcels.zone
    GROUP BY
        parcels.parcel_id
        ,zoning.zoning_id
        ,zoning.zone
        ,zoning.min_dua
        ,zoning.max_dua
        ,zoning.max_res_units
        ,sr13_capacity.sr13_cap_hs_growth_adjusted
)
INSERT INTO urbansim.parcel_zoning_schedule2
SELECT
    2 as zoning_schedule_id
    ,parcel_id
    ,zoning.zoning_id
    ,zoning.zone
FROM t
JOIN urbansim.zoning
ON zoning.zone = CONCAT(t.zone, ' cap_hs ', t.cap_hs)
ORDER BY 2
;

/*** ZONING PARCELS TABLE ***/
CREATE TABLE urbansim.zoning_parcels
(
    zoning_parcels_id int IDENTITY NOT NULL,
    parcel_id int NOT NULL,
    zoning_id int NOT NULL,
    zone nvarchar(35) NOT NULL,
    zoning_schedule_id int NOT NULL,
    CONSTRAINT uk_zoning_parcels UNIQUE (parcel_id, zoning_id, zoning_schedule_id)
);

CREATE INDEX ix_zoning_parcel_id
    ON urbansim.zoning_parcels(parcel_id, zoning_id, zoning_schedule_id);


--LOAD ZONING SCHEDULE 1
INSERT INTO urbansim.zoning_parcels (parcel_id, zoning_id, zone, zoning_schedule_id)
SELECT 
    parcel_id
    ,z.zoning_id
    ,p.zone
    ,1 AS zoning_schedule_id
FROM ref.parcelzoning_base  AS p
JOIN (SELECT zoning_id, zone FROM urbansim.zoning WHERE zoning_schedule_id = 1) AS z
    ON p.zone = z.zone
;
--LOAD ZONING SCHEDULE 2
INSERT INTO urbansim.zoning_parcels (parcel_id, zoning_id, zone, zoning_schedule_id)
SELECT
    p.parcel_id
    ,COALESCE (pzs.zoning_id, p.zoning_id)
    ,COALESCE (pzs.zone, p.zone)
    ,2 AS zoning_schedule_id
FROM urbansim.parcel_zoning_schedule2 AS pzs
RIGHT JOIN (SELECT p.parcel_id, z.zoning_id, p.zone
            FROM ref.parcelzoning_base AS p
            LEFT JOIN (SELECT zoning_id, zone FROM urbansim.zoning WHERE zoning_schedule_id = 1) AS z ON p.zone = z.zone ) AS p
    ON pzs.parcel_id = p.parcel_id
ORDER BY parcel_id
;


/***########## INSERT POLYGONS FOR SCHEDULE 2 START ##########***/	--FIX FOR PARCELS DATASET, NO ZONING DATA!!!!!
/*** 1- INSERT ZONING POLYGONS FROM PARCELS ****/
UPDATE z
SET shape= p.shape						--BACK TO GEOGRAPHY
FROM urbansim.zoning AS z
JOIN(
    SELECT  pzs.zoning_id, pzs.zoning_schedule_id
        ,geometry::UnionAggregate(shape) AS shape		--TO MULTIPART GEOMETRY
    FROM urbansim.parcel_zoning_schedule2 AS pzs
    JOIN urbansim.parcels AS p
    ON pzs.parcel_id = p.parcel_id
    GROUP BY pzs.zoning_id, pzs.zoning_schedule_id
    --ORDER BY pzs.zoning_id
)p
ON z.zoning_id = p.zoning_id
WHERE z.zoning_schedule_id = 2
;

/*** 2- INSERT ADDITIONAL ZONING POLYGONS FROM PARENT;
PARCELS NOT IN ZONING SCHEDULE 2, YES IN ZONING POLYGONS AFFECTED BY IT ****/
INSERT INTO urbansim.zoning(
    zoning_schedule_id
    ,zone
    ,parent_zoning_id
    ,parent_zone
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
    ,review)
SELECT
    2 AS zoning_schedule_id
    ,zp.zone
    ,zp.zoning_id AS parent_zoning_id
    ,zp.zone AS parent_zone
    ,z.jurisdiction_id
    ,z.zone_code
    ,z.yr_effective
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
    ,zp.shape
    ,z.review
FROM urbansim.zoning AS z
JOIN (SELECT
        zp.zoning_id
        ,zp.zone
        ,geometry::UnionAggregate(shape) AS shape
    FROM urbansim.zoning_parcels AS zp
    JOIN urbansim.parcels AS p ON zp.parcel_id = p.parcel_id
    WHERE zp.zoning_schedule_id = 2
    AND zp.parcel_id NOT IN (SELECT parcel_id FROM urbansim.parcel_zoning_schedule2)
    GROUP BY zp.zoning_id, zp.zone) AS zp
ON z.zoning_id = zp.zoning_id
WHERE z.zoning_schedule_id = 1
;

/*** 3- INSERT ADDITIONAL ZONING POLYGONS FROM SCHEDULE 1 NOT CHANGED IN SCHEDULE 2 ****/
INSERT INTO urbansim.zoning(
    zoning_schedule_id
    ,zone
    ,parent_zoning_id
    ,parent_zone
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
    ,review)
SELECT
    2 AS zoning_schedule_id
    ,zone
    ,zoning_id AS parent_zoning_id
    ,zone AS parent_zone
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
FROM urbansim.zoning
WHERE zoning_schedule_id = 1
AND zone NOT IN (SELECT DISTINCT parent_zone FROM urbansim.zoning WHERE zoning_schedule_id = 2)
;

SELECT COUNT(*) FROM urbansim.zoning;




