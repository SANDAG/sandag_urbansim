CREATE TABLE urbansim.zoning_parcels
(
    zoning_parcels_id serial NOT NULL,
    parcel_id integer NOT NULL,
    zoning_id character varying NOT NULL,
    zoning_schedule_id integer NOT NULL,
    CONSTRAINT uk_zoning_parcels UNIQUE (parcel_id, zoning_id, zoning_schedule_id)
)
;
ALTER TABLE urbansim.zoning_parcels
    OWNER TO urbansim_user;
GRANT ALL ON TABLE urbansim.zoning TO urbansim_user;


CREATE INDEX ix_zoning_parcel_id
    ON urbansim.zoning_parcels
    USING btree
    (parcel_id, zoning_id, zoning_schedule_id);

SELECT COUNT(*) FROM urbansim.zoning_parcels


--LOAD ZONING SCHEDULE 1
INSERT INTO urbansim.zoning_parcels (parcel_id, zoning_id, zoning_schedule_id)
SELECT 
    parcel_id
    ,zoning_id
    ,1 AS zoning_schedule_id
FROM ref.parcelzoning_base  AS p
JOIN (SELECT zoning_id, zone FROM urbansim.zoning WHERE zoning_schedule_id = 1) AS z
    ON p.zone = z.zone

--LOAD ZONING SCHEDULE 2
INSERT INTO urbansim.zoning_parcels (parcel_id, zoning_id, zoning_schedule_id)
SELECT
    p.parcel_id
    ,COALESCE (pzs.zoning_id, p.zoning_id)
    ,2 AS zoning_schedule_id
FROM urbansim.parcel_zoning_schedule AS pzs
RIGHT JOIN (SELECT p.parcel_id, z.zoning_id 
            FROM ref.parcelzoning_base AS p
            LEFT JOIN (SELECT zoning_id, zone FROM urbansim.zoning WHERE zoning_schedule_id = 2) AS z ON p.zone = z.zone ) AS p
    ON pzs.parcel_id = p.parcel_id
ORDER BY parcel_id
;

