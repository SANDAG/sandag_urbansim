/***########## JURISDICTION ID OVERRIDES ##########***/
--CREATE TEMP TABLE WITH OVERRIDES
CREATE TABLE #update
	([parcel_id] int, [jur_old] int, [jur_new] int)
;
INSERT INTO #update VALUES
	(240931, 19, 7),
	(394693, 19, 18),
	(647045, 19, 5),
	(701058, 19, 7),
	(712885, 19, 5),
	(818952, 1, 6),
	(5038426, 15, 19),
	(5040257, 15, 19),
	(5224326, 18, 19)
;

SELECT * FROM #update

--CHECK CHANGES
SELECT 
	u.parcel_id
	,p.parcel_id
	,p.jurisdiction_id
	,jur_old
	,jur_new
FROM #update AS u
JOIN urbansim.parcels AS p ON u.parcel_id = p.parcel_id
ORDER BY p.parcel_id

SELECT 
	u.parcel_id
	,b.parcel_id
	,b.jurisdiction_id
	,jur_old
	,jur_new
FROM #update AS u
JOIN urbansim.buildings AS b ON u.parcel_id = b.parcel_id
ORDER BY b.parcel_id


--EXECUTE CHANGES TO PARCELS
UPDATE
	usp
SET 
	usp.jurisdiction_id = jur_new
FROM 
	urbansim.parcels AS usp
	JOIN #update AS u ON usp.parcel_id = u.parcel_id

--EXECUTE CHANGES TO BUILDINGS
UPDATE
	usb
SET 
	usb.jurisdiction_id = jur_new
FROM 
	urbansim.buildings AS usb
	JOIN #update AS u ON usb.parcel_id = u.parcel_id


/***########## RESIDENTIAL UNITS ADJUSTMENT ##########***/
--CREATE TEMP TABLE WITH ADJUSTMENTS
CREATE TABLE #adjust
	([parcel_id] int, [units] int, [jurisdiction_id] nvarchar(35), [comment] nvarchar(35))
;
INSERT INTO #adjust VALUES
	(14156,7,'San Diego','MCRD'),
	(340020,13,'San Diego','NMCDC'),
	(523182,41,'San Diego','not residential'),
	(523183,8,'San Diego','not residential'),
	(4102141,29,'San Diego','not residential'),
	(634988,596,'San Marcos','dorms'),
	(5301892,302,'San Marcos','dorms'),
	(321373,1,'San Marcos','not residential'),
	(286322,75,'El Cajon','hotel'),
	(5035372,1,'Oceanside','not residential'),
	(329481,1,'Unincorporated','not residential'),
	(5024624, NULL, '', ''),
	(748817, NULL, '', ''),
	(5024624, NULL, '', ''),
	(30400, NULL, '', ''),
	(4946, NULL, '', '')
;

SELECT * FROM #adjust ORDER BY parcel_id

--CHECK CHANGES TO BUILDINGS
SELECT
	a.parcel_id
	,b.parcel_id
	,b.building_id
	,a.units AS parcel_units
	,b.residential_units AS bldg_units
	,a.jurisdiction_id
	,b.jurisdiction_id
	,comment
FROM #adjust AS a
JOIN urbansim.buildings AS b ON a.parcel_id = b.parcel_id
ORDER BY b.parcel_id

--EXECUTE CHANGES TO BUILDINGS
UPDATE
	usb
SET 
	usb.residential_units = 0
FROM 
	urbansim.buildings AS usb
	JOIN #adjust AS a ON usb.parcel_id = a.parcel_id