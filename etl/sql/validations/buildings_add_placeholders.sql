USE spacecore

/*** RES PLACEHOLDERS ***/
--LOOK FOR LUDU RECORDS WITH DU>0 AND NO BUILDING. BUFFER AND INSERT INTO NEW TABLE
SELECT parcel_id, centroid.STBuffer(1) shape
--INTO staging.buildings_add_res
FROM urbansim.parcels p
WHERE p.parcel_id IN (SELECT DISTINCT l.parcelID
						FROM GIS.landcore l
						WHERE l.parcelID NOT IN (SELECT parcel_id FROM urbansim.buildings)
						AND l.du > 0)
AND p.parcel_id NOT IN staging.parcel_centroid_bad

--FOR BAD CENTROID, WITHIN SHAPE
SELECT parcel_id, p.shape.STBuffer(-10).STPointOnSurface().STBuffer(1) shape
--INTO staging.buildings_add_res
FROM urbansim.parcels p
WHERE p.parcel_id IN (SELECT DISTINCT l.parcelID
						FROM GIS.landcore l
						WHERE l.parcelID NOT IN (SELECT parcel_id FROM urbansim.buildings)
						AND l.du > 0)
AND p.parcel_id IN staging.parcel_centroid_bad


/*** EMP PLACEHOLDERS ***/
--LOOK FOR LUDU RECORDS WITH EMP AND NO BUILDING. BUFFER AND INSERT INTO NEW TABLE
SELECT parcel_id, centroid.STBuffer(1) shape
--INTO staging.buildings_add_emp
FROM urbansim.parcels p
WHERE p.parcel_id IN 
		(
		SELECT parcelID FROM [socioec_data].[ca_edd].[emp_2013]
		EXCEPT
		SELECT parcel_id FROM spacecore.gis.buildings
		)
AND p.parcel_id NOT IN staging.parcel_centroid_bad

--FOR BAD CENTROID, WITHIN SHAPE
SELECT parcel_id, p.shape.STBuffer(-10).STPointOnSurface().STBuffer(1) shape
--INTO staging.buildings_add_emp
FROM urbansim.parcels p
WHERE p.parcel_id IN 
		(
		SELECT parcelID FROM [socioec_data].[ca_edd].[emp_2013]
		EXCEPT
		SELECT parcel_id FROM spacecore.gis.buildings
		)
AND p.parcel_id IN staging.parcel_centroid_bad

--DELETE PREVIOUS PLACEHOLDERS FROM BUILDINGS TABLE
DELETE FROM GIS.buildings
WHERE dataSource = 'place_holders'

--ADD RES PLACEHOLDERS
INSERT INTO GIS.buildings (shape, dataSource)
SELECT shape, 'place_holders'
FROM staging.buildings_add_res

--ADD EMP PLACEHOLDERS
INSERT INTO GIS.buildings (shape, dataSource)
SELECT shape, 'place_holders'
FROM staging.buildings_add_emp

--CHECK
SELECT dataSource, COUNT(*) 
FROM [GIS].[buildings]
GROUP BY dataSource

--NEXT: DROP URBANSIM.BUILDINGS, RUN BUILDINGS.SQL TO REGENERATE

--CHECK
SELECT COUNT(*) FROM [GIS].[buildings]
SELECT COUNT(*) FROM [urbansim].[buildings]

--CHECK UNITS
SELECT SUM(du)
FROM gis.landcore

SELECT *
FROM urbansim.buildings
WHERE development_type_id = 19
AND residential_units > 1
ORDER BY residential_units DESC