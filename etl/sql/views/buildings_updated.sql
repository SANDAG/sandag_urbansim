USE spacecore
--IF OBJECT_ID('urbansim.buildings_updated') IS NOT NULL
--    DROP TABLE urbansim.buildings_updated
--GO
CREATE TABLE urbansim.buildings_updated(
	building_id int NOT NULL
	,development_type_id smallint
	,subparcel_id int NULL
	,parcel_id int NOT NULL
	,block_id bigint
	,mgra_id int
	,luz_id int
	,improvement_value float
	,residential_units smallint
	,residential_sqft int
	,non_residential_sqft int
    ,job_spaces smallint
    ,non_residential_rent_per_sqft float
	,price_per_sqft float
	,stories int
	,year_built smallint
	,shape geometry
	,centroid geometry
	,data_source nvarchar(50)
	,subparcel_assignment nvarchar(50)
	,floorspace_source nvarchar(50)
)
INSERT INTO urbansim.buildings_updated WITH (TABLOCK) (
	building_id
	,subparcel_id
	,parcel_id
	,shape
	,centroid
	,data_source
	)
SELECT 
	bldgID
	,subparcel_					--subparcelID
	,parcel_id					--parcelID
	,ogr_geometry				--shape
	,ogr_geometry.STCentroid()	--centroid
	,data_sourc					--dataSource
FROM gis.buildings_updated

--CREATE A PRIMARY KEY SO WE CAN CREATE A SPATIAL INDEX
ALTER TABLE urbansim.buildings_updated ADD CONSTRAINT pk_urbansim_buildings_updated_building_id PRIMARY KEY CLUSTERED (building_id) 

--SET THE SHAPES TO BE NOT NULL SO WE CAN CREATE A SPATIAL INDEX
ALTER TABLE urbansim.buildings_updated ALTER COLUMN shape geometry NOT NULL
ALTER TABLE urbansim.buildings_updated ALTER COLUMN centroid geometry NOT NULL

--SELECT max(x_coord), min(x_coord), max(y_coord), min(y_coord) from gis.parcels

CREATE SPATIAL INDEX [ix_spatial_urbansim_buildings_updated_shape] ON urbansim.buildings_updated
(
    shape
) USING  GEOMETRY_GRID
    WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
    CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

CREATE SPATIAL INDEX [ix_spatial_urbansim_buildings_updated_centroid] ON urbansim.buildings_updated
(
    centroid
) USING  GEOMETRY_GRID
    WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
    CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

--GET BLOCK_ID
UPDATE
	usb
SET
	usb.block_id = b.BLOCKID10
FROM
	urbansim.buildings_updated usb
JOIN ref.blocks b
ON b.Shape.STContains(usb.shape.STCentroid()) = 1


/** LANDCORE DATA **/
--GET LANDCORE DATA: PARCEL_ID, DEV_TYPE
UPDATE
	usb
SET
	usb.mgra_id = lc.mgra
	,usb.development_type_id = dev.development_type_id
FROM
	urbansim.buildings_updated usb
JOIN gis.landcore lc
ON usb.parcel_ID = lc.parcelID
JOIN ref.development_type_lu_code dev 
ON lc.lu = dev.lu_code


/** ASSESSOR AND LANDCORE DATA **/
--GET ASSESSOR DATA: RESIDENTIAL UNITS, NON-RES SQFT, PRICE/SQFT, YEAR BUILT
UPDATE
	usb
SET
	usb.improvement_value = a.imps/b.bldgs
	,usb.residential_sqft = CASE WHEN usb.development_type_id BETWEEN 19 AND 22
								THEN a.sqft/b.bldgs
								ELSE 0
							END
	,usb.non_residential_sqft = CASE WHEN usb.development_type_id NOT BETWEEN 19 AND 22
								THEN a.sqft/b.bldgs
								ELSE 0
							END
	,usb.year_built = a.year_built

FROM
	urbansim.buildings_updated usb
	LEFT JOIN 
		(SELECT parcel_id,  COUNT(parcel_id) bldgs						--NUMBER OF BUILDINGS
		FROM urbansim.buildings_updated
		GROUP BY parcel_id) b
	 ON usb.parcel_id = b.parcel_id
	LEFT JOIN
		(SELECT l.parcelID
			,LEFT(par.apn,8) apn8
			,SUM([CURRENT_IMPS]) imps
			,SUM([TOTAL_LVG_AREA]+[ADDITION_AREA]) sqft
			,AVG(CASE WHEN [YEAR_EFFECTIVE] <= 16
					THEN 2000 + CAST([YEAR_EFFECTIVE] AS smallint)
					ELSE 1900 + CAST([YEAR_EFFECTIVE] AS smallint)
				END ) year_built
			,COUNT(*) num
		FROM spacecore.input.assessor_par par
		JOIN
			(SELECT parcelID
				,MIN(apn) apn											--GRAB LOWEST APN
			FROM spacecore.gis.landcore
			GROUP BY parcelID) l
		ON l.apn = LEFT(par.apn,8)										--ONE TO MANY, SELECT MIN APN
		GROUP BY l.parcelID, LEFT(par.apn,8)
		) a
	ON usb.parcel_id = a.parcelID

/** COSTAR DATA **/
--GET COSTAR DATA: RESIDENTIAL UNITS, NON-RES SQFT, PRICE/SQFT, YEAR BUILT
UPDATE
	usb
SET
	usb.residential_sqft = CASE 
								WHEN usb.development_type_id BETWEEN 19 AND 22 THEN 
									CASE 
										WHEN c.rentable_building_area > 0 THEN (c.rentable_building_area/b.bldgs)
										ELSE usb.residential_sqft
									END
								ELSE usb.residential_sqft
							END
	,usb.non_residential_sqft = CASE 
									WHEN usb.development_type_id NOT BETWEEN 19 AND 22 THEN 
										CASE 
											WHEN c.rentable_building_area > 0 THEN (c.rentable_building_area/b.bldgs)
											ELSE usb.non_residential_sqft
										END
									ELSE usb.non_residential_sqft
								END
	,usb.year_built = CASE 
						WHEN c.year_built > 0 THEN c.year_built
						ELSE
							CASE 
								WHEN usb.year_built  IS NULL THEN 2000
								ELSE usb.year_built
							END								
						END
	,usb.stories = CASE 

						WHEN c.stories > 0 THEN c.stories
						ELSE 1
					END	
FROM
	urbansim.buildings_updated usb
	LEFT JOIN 
		(SELECT parcel_id,  COUNT(parcel_id) bldgs						--NUMBER OF BUILDINGS
		FROM urbansim.buildings_updated
		GROUP BY parcel_id) b
	 ON usb.parcel_id = b.parcel_id
	LEFT JOIN
		(SELECT parcel_id
			, SUM([rentable_building_area]) rentable_building_area
			, AVG([year_built]) year_built
			, AVG([number_of_stories]) stories
		FROM input.costar
		GROUP BY parcel_id) c
	ON usb.parcel_id = c.parcel_id


/*#################### START RES AND EMP  SPACE ####################*/

/*################## STEP 6: FOR SUB-PARCELS WITH NO BUILDINGS STILL AND DU, ASSIGN FAKE BUILDING POINT  ###################*/
INSERT INTO urbansim.buildings_updated (building_id, subparcel_id, parcel_id, shape, data_source, subparcel_assignment)
SELECT 
	lc.subParcel + 2000000 AS building_id		--INSERTED BUILDING_ID > 2,000,000
	,lc.subParcel
	,parcelID
	,lc.Shape.STPointOnSurface().STBuffer(1)
	,'PLACEHOLDER'
	,'PLACEHOLDER'
FROM 
	spacecore.gis.landcore lc
	LEFT JOIN urbansim.buildings_updated wsb ON lc.subParcel = wsb.subparcel_id
WHERE 
	du > 0
	AND wsb.subparcel_id is null

/*################## STEP 7: VERIFY THAT ALL SUB-PARCEL WITH DU HAVE A BUILDING  ###################*/
SELECT COUNT(*) missing_res_building FROM 
	spacecore.gis.landcore lc
	LEFT JOIN urbansim.buildings_updated wsb ON lc.subParcel = wsb.subparcel_id
WHERE 
	du > 0
	AND wsb.subparcel_id is null

/*###### STEP 8: SET BUILDING'S RESIDENTIAL UNITS ON SUBPARCELS WITH ONLY ONE BUILDING #####*/
UPDATE wsb
  SET wsb.residential_units = lc.du
FROM
  urbansim.buildings_updated wsb
  INNER JOIN spacecore.gis.landcore lc ON lc.subParcel = wsb.subparcel_id
WHERE
  wsb.subparcel_id IN (SELECT subparcel_id FROM urbansim.buildings_updated wsb GROUP BY subparcel_id HAVING count(*) = 1)

/*###### STEP 9: SET BUILDINGS W/ SUB-PARCELS WITH NO RESIDENTIAL UNITS TO ZERO #####*/
UPDATE wsb
  SET wsb.residential_units = lc.du
FROM
  urbansim.buildings_updated wsb
  INNER JOIN spacecore.gis.landcore lc ON lc.subParcel = wsb.subparcel_id
WHERE
  wsb.subparcel_id IN (SELECT subparcel_id FROM urbansim.buildings_updated wsb GROUP BY subparcel_id HAVING count(*) > 1)
  AND lc.du = 0

/*########### STEP 10: EVENLY DISTRIBUTE RESIDENTIAL UNITS ON BLDG SIZE WHERE BLDG COUNT > 1 ON SUBPARCEL ############*/
-----MAY WANT TO THINK ABOUT EXCLUDING REALLY SMALL BUILDINGS FROM THIS QUERY
WITH bldgs AS (
  SELECT
    wsb.subparcel_id
   ,wsb.building_id
   ,lc.du/ COUNT(*) OVER (PARTITION BY subparcel_id) +
     CASE 
       WHEN ROW_NUMBER() OVER (PARTITION BY subparcel_id ORDER BY wsb.shape.STArea()) <= (lc.du % COUNT(*) OVER (PARTITION BY subparcel_id)) THEN 1 
       ELSE 0 
	 END units
  FROM
    urbansim.buildings_updated wsb
    INNER JOIN spacecore.gis.landcore lc ON wsb.subparcel_id = lc.subParcel
  WHERE
    subparcel_id IN (SELECT subparcel_id FROM urbansim.buildings_updated wsb WHERE wsb.residential_units is null GROUP BY subparcel_id HAVING count(*) > 1))


UPDATE wsb
  SET wsb.residential_units = bldgs.units
FROM
	urbansim.buildings_updated wsb
	INNER JOIN bldgs ON wsb.building_id = bldgs.building_id

/*################ STEP 11: SOME FINAL CHECKS TO ENSURE MGRA AND REGIONAL UNIT CONSISTENCY ######################*/
SELECT SUM(residential_units) bldg_units FROM urbansim.buildings_updated
SELECT SUM(du) lc_units FROM spacecore.gis.landcore

SELECT 
  wsb.subparcel_id
  ,lc.du lc_units
  ,SUM(residential_units) bldg_units
FROM 
  urbansim.buildings_updated wsb
  INNER JOIN spacecore.gis.landcore lc ON wsb.subparcel_id = lc.subParcel
GROUP BY
  wsb.subparcel_id
  ,lc.du
HAVING lc.du <> SUM(residential_units)


/*############ STEP 12: UPDATE RESIDENTIAL SQ FT WHERE DATA AVAILABLE ###### */
UPDATE wsb
  SET wsb.residential_sqft = wsb.residential_units * asr_units.avg_unit_size
FROM
  urbansim.buildings_updated wsb
  INNER JOIN spacecore.gis.landcore lc ON wsb.subparcel_id = lc.subParcel
  INNER JOIN
    (SELECT
      p.PARCELID
	  ,CAST(TOTAL_LVG_AREA as float) / bldgs.res_units avg_unit_size
    FROM
      (SELECT PARCELID, SUM(TOTAL_LVG_AREA) TOTAL_LVG_AREA FROM spacecore.gis.parcels GROUP BY PARCELID) p
       INNER JOIN (SELECT lc.parcelID, SUM(residential_units) res_units FROM urbansim.buildings_updated wsb INNER JOIN spacecore.gis.landcore lc ON wsb.subparcel_id = lc.subParcel GROUP BY lc.parcelID HAVING SUM(residential_units) > 0) bldgs
         ON p.PARCELID = bldgs.parcelID
	   WHERE TOTAL_LVG_AREA IS NOT NULL AND TOTAL_LVG_AREA > 0) asr_units ON lc.parcelID = asr_units.PARCELID


/*################ STEP 13: SOME MORE FINAL CHECKS TO ENSURE MGRA AND REGIONAL UNIT CONSISTENCY ######################*/
SELECT building_id, residential_units, residential_sqft FROM urbansim.buildings_updated wsb WHERE residential_units > 0 AND residential_sqft <= 0

SELECT * 
FROM
  (SELECT MGRA, SUM(residential_units) housing_units FROM urbansim.buildings_updated wsb INNER JOIN spacecore.gis.landcore lc ON wsb.subparcel_id = lc.subParcel GROUP BY MGRA) urbansim
  INNER JOIN (SELECT mgra_id % 1300000 mgra, SUM(units) housing_units, SUM(occupied) housholds FROM demographic_warehouse.fact.housing WHERE datasource_id = 19 GROUP BY mgra_id) estimates
    ON urbansim.MGRA = estimates.mgra
WHERE
  urbansim.housing_units <> estimates.housing_units

/*################ STEP 14: LOAD HOUSEHOLD TABLE ######################*/
/*
INSERT INTO urbansim.households_updated (
	scenario_id, 
	mgra, 
	tenure, 
	persons, 
	workers, 
	age_of_head, 
	income, 
	children, 
	race_id, 
	cars
)
SELECT
	scenario_id, 
	mgra, 
	tenure, 
	persons, 
	workers, 
	age_of_head, 
	income, 
	children,
	race_id, 
	cars 
FROM spacecore.input.vi_households
*/
SELECT *
INTO urbansim.households_updated
FROM spacecore.input.vi_households

/*################ STEP 15: THIS IS A 2010 HH FILE WITH 2015 BUILDINGS, SHAVE OFF HH WHERE BUILDINGS WERE DEMOLISHED SINCE 2010 ######################*/
WITH hh AS (
SELECT
  hh.mgra
  ,household_id
  ,ROW_NUMBER() OVER (PARTITION BY hh.mgra ORDER BY household_id) idx
  ,bldgs.housing_units
FROM
  urbansim.households_updated hh
  INNER JOIN (SELECT MGRA, SUM(residential_units) housing_units FROM urbansim.buildings_updated wsb INNER JOIN spacecore.gis.landcore lc ON wsb.subparcel_id = lc.subParcel GROUP BY MGRA) bldgs
    ON hh.mgra = bldgs.MGRA
)

SELECT * FROM urbansim.households_updated WHERE household_id IN (SELECT household_id FROM hh WHERE idx > housing_units)
DELETE FROM urbansim.households_updated WHERE household_id IN (SELECT household_id FROM hh WHERE idx > housing_units)


/*################ STEP 16: EVENLY DISTRIBUTE HOUSEHOLDS ONTO BUILDINGS BY MGRA ######################*/
WITH bldg as (
SELECT
  ROW_NUMBER() OVER (PARTITION BY mgra ORDER BY building_id) idx
  ,building_id
  ,mgra
FROM
(SELECT
  building_id
  ,subparcel_id
  ,residential_units
FROM
urbansim.buildings_updated
,ref.numbers n
WHERE n.numbers <= residential_units) bldgs
INNER JOIN spacecore.gis.landcore lc ON lc.subParcel = bldgs.subparcel_id),
hh AS (
SELECT 
  ROW_NUMBER() OVER (PARTITION BY mgra ORDER BY household_id) idx
  ,household_id
  ,mgra
FROM
  urbansim.households_updated
)

UPDATE h
 SET h.building_id = bldg.building_id

FROM
  urbansim.households_updated h
  INNER JOIN hh ON h.household_id = hh.household_id
  INNER JOIN bldg ON bldg.MGRA = hh.mgra AND bldg.idx = hh.idx

/*################ STEP 17: SOME MORE FINAL CHECKS TO ENSURE MGRA AND REGIONAL UNIT CONSISTENCY ######################*/
SELECT COUNT(*) FROM urbansim.households_updated WHERE building_id = 0

SELECT * FROM
(SELECT COUNT(*) hh, mgra FROM urbansim.households_updated GROUP BY mgra) as hh
INNER JOIN (SELECT SUM(residential_units) units, mgra FROM urbansim.buildings_updated INNER JOIN spacecore.gis.landcore ON subParcel = subparcel_id GROUP BY mgra) bldg ON hh.mgra = bldg.mgra
WHERE hh > units
