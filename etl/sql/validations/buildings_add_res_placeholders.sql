USE spacecore

--LOOK FOR LUDU RECORDS WITH DU>0 AND NO BUILDING. BUFFER AND INSERT INTO NEW TABLE
SELECT parcel_id, centroid.STBuffer(1) shape
INTO GIS.buildings_add_res
FROM urbansim.parcels p
WHERE p.parcel_id IN (SELECT DISTINCT l.parcelID
						FROM GIS.landcore l
						WHERE l.parcelID NOT IN (SELECT parcel_id FROM urbansim.buildings)
						AND l.du > 0) 

--ADD RES PLACEHOLDERS
INSERT INTO GIS.buildings (shape, dataSource)
SELECT shape, 'place_holders'
FROM GIS.buildings_add_res

--CHECK
SELECT * FROM [GIS].[buildings]

--NEXT: DROP URBANSIM.BUILDINGS, RUN BUILDINGS.SQL TO REGENERATE

--CHECK
SELECT COUNT(*) FROM [GIS].[buildings]
SELECT COUNT(*) FROM [urbansim].[buildings]

SELECT SUM(du)
FROM gis.landcore

SELECT *
FROM urbansim.buildings
WHERE development_type_id = 19
AND residential_units > 1
ORDER BY residential_units DESC