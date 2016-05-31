USE spacecore

--LOOK FOR LUDU RECORDS WITH DU>0 AND NO BUILDING. BUFFER AND INSERT INTO NEW TABLE
SELECT parcel_id, centroid.STBuffer(1) shape
INTO GIS.buildings_add_emp
FROM urbansim.parcels p
WHERE p.parcel_id IN 
		(
		SELECT parcelID FROM [socioec_data].[ca_edd].[emp_2013]
		EXCEPT
		SELECT parcel_id FROM spacecore.urbansim.buildings
		)

--ADD RES PLACEHOLDERS
INSERT INTO GIS.buildings (shape, dataSource)
SELECT shape, 'place_holders'
FROM GIS.buildings_add_emp

--CHECK
SELECT * FROM [GIS].[buildings]

--NEXT: DROP URBANSIM.BUILDINGS, RUN BUILDINGS.SQL TO REGENERATE

--CHECK
SELECT COUNT(*) FROM [GIS].[buildings]
SELECT COUNT(*) FROM [urbansim].[buildings]

