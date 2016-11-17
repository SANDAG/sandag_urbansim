CREATE TABLE urbansim.zoning_parcels
(
    parcel_id character varying NOT NULL,
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

SELECT * FROM urbansim.zoning_parcels


--LOAD ZONING SCHEDULE 1
INSERT INTO urbansim.zoning_parcels
SELECT parcel_id
    ,zoning_id
    ,1 AS zoning_schedule_id
FROM urbansim.parcels


--LOAD ZONING SCHEDULE 2
INSERT INTO urbansim.zoning_parcels
SELECT p.parcel_id
    ,COALESCE (pzs.zoning_id, p.zoning_id)
    ,2 AS zoning_schedule_id
FROM urbansim.parcel_zoning_schedule AS pzs
RIGHT JOIN (SELECT parcel_id, zoning_id FROM urbansim.parcels) AS p
    ON pzs.parcel_id = p.parcel_id
;
/*
SELECT parcel_id
    ,zoning_id
    ,zoning_schedule_id
FROM urbansim.parcel_zoning_schedule;
*/

