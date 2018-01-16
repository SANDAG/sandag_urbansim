/*
OUTPUT:
[urbansim].[households]
[urbansim].[jobs]
[urbansim].[buildings]
*/

USE spacecore

IF OBJECT_ID('urbansim.buildings') IS NOT NULL
    DROP TABLE urbansim.buildings
GO

CREATE TABLE urbansim.buildings(
	building_id int NOT NULL
	,development_type_id smallint
	,subparcel_id int NULL
	,parcel_id int NOT NULL
	,block_id bigint
	,mgra_id int
	,luz_id int
	,jurisdiction_id smallint
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
	,sqft_source nvarchar(50)
	,assign_jobs bit
)
/***** INITIAL LOAD *****/
--LOAD FROM GIS.BUILDINGS
INSERT INTO urbansim.buildings WITH (TABLOCK) (
	building_id
	,subparcel_id
	,parcel_id
	,shape
	,centroid
	,data_source
	,subparcel_assignment
	,assign_jobs
	)
SELECT
	bldgid						--bldgID
	,subparcel_					--subparcelID
	,parcel_id					--parcelID
	,ogr_geometry				--shape
	,ogr_geometry.STCentroid()	--centroid
	,data_sourc					--dataSource
	,'BUILDING'
	,1							--assign_jobs
FROM gis.buildings

--INSERT ADDITIONAL BUILDINGS: PUBLIC FACILITIES AND OTHER
--REMOVE OVERLAPPING BUILDINGS
DELETE
FROM urbansim.buildings
WHERE parcel_id IN (SELECT parcelid FROM [GIS].[buildings_camp_pendleton])			--16 DELETED
--OR parcel_id IN (SELECT parcelid FROM [GIS].[buildings_public_facilities])		--REMOVE PUBLIC FACILITIES DUPLICATES ???	LOOK AT DEV_TYPE???

--SELECT data_source, COUNT(*)
--FROM urbansim.buildings
--WHERE parcel_id IN (SELECT parcelid FROM [GIS].[buildings_public_facilities])	--1,021		--REMOVE PUBLIC FACILITIES DUPLICATES ???
--GROUP BY data_source

--LOAD FROM PUBLIC FACILITIES BUILDINGS	--2,534
INSERT INTO urbansim.buildings WITH (TABLOCK) (
	building_id
	,subparcel_id
	,parcel_id
	,development_type_id
	,mgra_id
	--,job_spaces
	,shape
	,centroid
	,data_source
	,subparcel_assignment
	,assign_jobs
	)
SELECT
	800000 + id					--bldgID
	,lc.subParcel				--subparcelID
	,bpf.parcelid				--parcelID
	,dev.development_type_id	--development_type_id
	,mgra13						--mgra_id
	--,emp						--job_spaces
	,ogr_geometry				--shape
	,ogr_geometry.STCentroid()	--centroid
	,datasource					--dataSource	--"SANDAG Public Facility 2016 Geocoding 042617"
	,'BUILDING'
	,0							--assign_jobs
FROM gis.buildings_public_facilities AS bpf
JOIN gis.ludu2015 AS lc ON bpf.ogr_geometry.STCentroid().STWithin(lc.shape) = 1
LEFT JOIN ref.development_type_lu_code dev											--_XX WILL NOT JOIN TO LANDCORE WHEN NO MATCH IN LU
ON bpf.lu = dev.lu_code


--LOAD FROM CAMP PENDLETON BUILDINGS	--5,762
INSERT INTO urbansim.buildings WITH (TABLOCK) (
	building_id
	,subparcel_id
	,parcel_id
	,development_type_id
	,mgra_id
	,shape
	,centroid
	,data_source
	,subparcel_assignment
	,assign_jobs
	)
SELECT
	900000 + bldgid				--bldgID
	,lc.subParcel				--subparcelID
	,bcp.parcelid				--parcelID
	,dev.development_type_id	--development_type_id
	,mgra13						--mgra_id
	,ogr_geometry				--shape
	,ogr_geometry.STCentroid()	--centroid
	,datasource					--dataSource
	,'BUILDING'
	,0							--assign_jobs
FROM gis.buildings_camp_pendleton AS bcp
JOIN gis.ludu2015 AS lc ON bcp.ogr_geometry.STCentroid().STWithin(lc.shape) = 1
LEFT JOIN ref.development_type_lu_code dev											--_XX WILL NOT JOIN TO LANDCORE WHEN NO MATCH IN LU
ON bcp.lu = dev.lu_code

--CHECK FOR NULL SHAPES AND REMOVE
SELECT * FROM [spacecore].[urbansim].[buildings] WHERE shape IS NULL
DELETE FROM [spacecore].[urbansim].[buildings] WHERE shape IS NULL

--CREATE A PRIMARY KEY SO WE CAN CREATE A SPATIAL INDEX
ALTER TABLE urbansim.buildings ADD CONSTRAINT pk_urbansim_buildings_building_id PRIMARY KEY CLUSTERED (building_id) 

--SET THE SHAPES TO BE NOT NULL SO WE CAN CREATE A SPATIAL INDEX
ALTER TABLE urbansim.buildings ALTER COLUMN shape geometry NOT NULL
ALTER TABLE urbansim.buildings ALTER COLUMN centroid geometry NOT NULL

--SELECT max(x_coord), min(x_coord), max(y_coord), min(y_coord) from gis.parcels

CREATE SPATIAL INDEX [ix_spatial_urbansim_buildings_shape] ON urbansim.buildings
(
    shape
) USING  GEOMETRY_GRID
    WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
    CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

CREATE SPATIAL INDEX [ix_spatial_urbansim_buildings_centroid] ON urbansim.buildings
(
    centroid
) USING  GEOMETRY_GRID
    WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
    CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

/** LANDCORE DATA **/
--GET LANDCORE DATA: MGRA, DEV_TYPE
UPDATE
	usb
SET
	usb.mgra_id = lc.mgra
	,usb.development_type_id = dev.development_type_id
FROM
	(SELECT * FROM urbansim.buildings WHERE assign_jobs = 1 AND mgra_id IS NULL AND development_type_id IS NULL) usb		--DO NOT USE MIL/PF BUILDINGS
JOIN gis.ludu2015 lc
ON usb.subparcel_id = lc.subParcel
JOIN ref.development_type_lu_code dev											--_XX WILL NOT JOIN TO LANDCORE WHEN NO MATCH IN LU
ON lc.lu = dev.lu_code

--SELECT * FROM urbansim.buildings WHERE development_type_id IS NULL

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
	,sqft_source = 'par'

FROM
	(SELECT * FROM urbansim.buildings WHERE assign_jobs = 1) usb		--DO NOT USE MIL/PF BUILDINGS
	LEFT JOIN 
		(SELECT parcel_id,  COUNT(parcel_id) bldgs						--NUMBER OF BUILDINGS
		FROM urbansim.buildings
		GROUP BY parcel_id) b
	 ON usb.parcel_id = b.parcel_id
	LEFT JOIN
		(SELECT lc.parcelID
			--,LEFT(par.apn,8) apn8
			,SUM([CURRENT_IMPS]) imps
			,SUM([TOTAL_LVG_AREA]+[ADDITION_AREA]) sqft
			,AVG(CASE WHEN [YEAR_EFFECTIVE] <= 16
					THEN 2000 + CAST([YEAR_EFFECTIVE] AS smallint)
					ELSE 1900 + CAST([YEAR_EFFECTIVE] AS smallint)
				END ) year_built
			,COUNT(*) num
		FROM spacecore.input.assessor_par par
		JOIN GIS.ludu2015 AS lc											--APN TO PARCELID FROM LUDU
		ON lc.APN = LEFT(par.APN, 8)
		GROUP BY lc.parcelID
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
	,sqft_source = 'costar'
FROM
	(SELECT * FROM urbansim.buildings WHERE assign_jobs = 1) usb		--DO NOT USE MIL/PF BUILDINGS
	LEFT JOIN 
		(SELECT parcel_id,  COUNT(parcel_id) bldgs						--NUMBER OF BUILDINGS
		FROM urbansim.buildings
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

--CHECK PAR AND COSTAR SQFT RESULTS
SELECT *
FROM urbansim.buildings
WHERE ISNULL([residential_sqft], 0) + ISNULL([non_residential_sqft], 0) <= 0
AND sqft_source IS NOT NULL


/*#################### START RES AND EMP SPACE ####################*/

/*#################### RES PROCESSING ####################*/

/*################## STEP 6: FOR SUB-PARCELS WITH NO BUILDINGS STILL AND DU, ASSIGN FAKE BUILDING POINT  ###################*/
--INSERT PLACEHOLDER CENTROIDS
INSERT INTO urbansim.buildings (building_id, development_type_id, subparcel_id, parcel_id, mgra_id, shape, centroid, data_source, subparcel_assignment)
SELECT 
	lc.LCKey + 2000000 AS building_id		--INSERTED BUILDING_ID > 2,000,000
	,dev.development_type_id
	,lc.LCKey
	,parcelID
	,MGRA
	,lc.centroid.STBuffer(1) AS shape
	,lc.centroid
	,'PLACEHOLDER' AS data_source
	,'PLACEHOLDER_RES' AS subparcel_assignment
FROM 
	gis.ludu2015points lc
	LEFT JOIN urbansim.buildings usb ON lc.LCKey = usb.subparcel_id
	LEFT JOIN ref.development_type_lu_code dev ON lc.lu = dev.lu_code
WHERE
	lc.du > 0
	AND usb.subparcel_id is null

/*################## STEP 7: VERIFY THAT ALL SUB-PARCEL WITH DU HAVE A BUILDING  ###################*/
SELECT COUNT(*) missing_res_building FROM 
	gis.ludu2015 lc
	LEFT JOIN urbansim.buildings usb ON lc.subParcel = usb.subparcel_id
WHERE 
	du > 0
	AND usb.subparcel_id is null

/*###### STEP 8: SET BUILDING'S RESIDENTIAL UNITS ON SUBPARCELS WITH ONLY ONE BUILDING #####*/
UPDATE usb
  SET usb.residential_units = lc.du
FROM
  urbansim.buildings usb
  INNER JOIN gis.ludu2015 lc ON lc.subParcel = usb.subparcel_id
WHERE
  usb.subparcel_id IN (SELECT subparcel_id FROM urbansim.buildings usb GROUP BY subparcel_id HAVING count(*) = 1)

/*###### STEP 9: SET BUILDINGS W/ SUB-PARCELS WITH NO RESIDENTIAL UNITS TO ZERO #####*/
UPDATE usb
  SET usb.residential_units = lc.du
FROM
  urbansim.buildings usb
  INNER JOIN gis.ludu2015 lc ON lc.subParcel = usb.subparcel_id
WHERE
  usb.subparcel_id IN (SELECT subparcel_id FROM urbansim.buildings usb GROUP BY subparcel_id HAVING count(*) > 1)
  AND lc.du = 0
;

/*########### STEP 10: EVENLY DISTRIBUTE RESIDENTIAL UNITS ON BLDG SIZE WHERE BLDG COUNT > 1 ON SUBPARCEL ############*/
-----MAY WANT TO THINK ABOUT EXCLUDING REALLY SMALL BUILDINGS FROM THIS QUERY
WITH bldgs AS (
  SELECT
    usb.subparcel_id
   ,usb.building_id
   ,lc.du/ COUNT(*) OVER (PARTITION BY subparcel_id) +
     CASE 
       WHEN ROW_NUMBER() OVER (PARTITION BY subparcel_id ORDER BY usb.shape.STArea()) <= (lc.du % COUNT(*) OVER (PARTITION BY subparcel_id)) THEN 1 
       ELSE 0 
	 END units
  FROM
    urbansim.buildings usb
    INNER JOIN gis.ludu2015 lc ON usb.subparcel_id = lc.subParcel
  WHERE
    subparcel_id IN (SELECT subparcel_id FROM urbansim.buildings usb WHERE usb.residential_units is null GROUP BY subparcel_id HAVING count(*) > 1)
)
UPDATE usb
  SET usb.residential_units = bldgs.units
FROM
	urbansim.buildings usb
	INNER JOIN bldgs ON usb.building_id = bldgs.building_id
;

/*################ STEP 11: SOME FINAL CHECKS TO ENSURE MGRA AND REGIONAL UNIT CONSISTENCY ######################*/
SELECT SUM(residential_units) bldg_units FROM urbansim.buildings
SELECT SUM(du) lc_units FROM gis.ludu2015
;
SELECT 
  usb.subparcel_id
  ,lc.du lc_units
  ,SUM(residential_units) bldg_units
FROM 
  urbansim.buildings usb
  INNER JOIN gis.ludu2015 lc ON usb.subparcel_id = lc.subParcel
GROUP BY
  usb.subparcel_id
  ,lc.du
HAVING lc.du <> SUM(residential_units)
;

/*############ STEP 12: UPDATE RESIDENTIAL SQ FT WHERE DATA AVAILABLE ###### */
UPDATE usb
	SET usb.residential_sqft = usb.residential_units * asr_units.avg_unit_size
	,sqft_source = 'avg_asr_unit_size'
FROM
	urbansim.buildings usb
	INNER JOIN gis.ludu2015 lc ON usb.subparcel_id = lc.subParcel
	INNER JOIN
		(SELECT
			p.PARCELID
			,CAST(TOTAL_LVG_AREA as float) / bldgs.res_units avg_unit_size
		FROM (SELECT PARCELID, SUM(TOTAL_LVG_AREA) TOTAL_LVG_AREA FROM gis.parcels GROUP BY PARCELID) p
		INNER JOIN (SELECT lc.parcelID, SUM(residential_units) res_units 
					FROM urbansim.buildings usb 
					INNER JOIN gis.ludu2015 lc ON usb.subparcel_id = lc.subParcel 
					GROUP BY lc.parcelID HAVING SUM(residential_units) > 0) bldgs
		ON p.PARCELID = bldgs.parcelID
		WHERE TOTAL_LVG_AREA IS NOT NULL AND TOTAL_LVG_AREA > 0) asr_units 
	ON lc.parcelID = asr_units.PARCELID
WHERE sqft_source IS NULL
;

/*################ STEP 13: SOME MORE FINAL CHECKS TO ENSURE MGRA AND REGIONAL UNIT CONSISTENCY ######################*/
SELECT building_id, residential_units, residential_sqft FROM urbansim.buildings usb WHERE residential_units > 0 AND residential_sqft <= 0;
--UPDATE FOR BUILDINGS WITH UNITS AND NO SQFT
WITH avgres AS(
	SELECT
		AVG(CAST(TOTAL_LVG_AREA as float) / bldgs.res_units) avg_unit_size
	FROM (SELECT PARCELID, SUM(TOTAL_LVG_AREA) TOTAL_LVG_AREA FROM gis.parcels GROUP BY PARCELID) p
	INNER JOIN (SELECT lc.parcelID, SUM(residential_units) res_units 
				FROM urbansim.buildings usb 
				INNER JOIN gis.ludu2015 lc ON usb.subparcel_id = lc.subParcel 
				GROUP BY lc.parcelID HAVING SUM(residential_units) > 0) bldgs
	ON p.PARCELID = bldgs.parcelID
	WHERE TOTAL_LVG_AREA IS NOT NULL AND TOTAL_LVG_AREA > 0
)
UPDATE
	usb
SET
	residential_sqft = residential_units * avgres.avg_unit_size
	,sqft_source = 'avg_res_unit_size'
FROM urbansim.buildings AS usb
	,avgres
WHERE residential_units > 0 AND residential_sqft <= 0
AND sqft_source IS NULL
;

SELECT * 
FROM
	(SELECT MGRA, SUM(residential_units) housing_units FROM urbansim.buildings usb INNER JOIN gis.ludu2015 lc ON usb.subparcel_id = lc.subParcel GROUP BY MGRA) urbansim
	INNER JOIN (SELECT mgra_id % 1300000 mgra, SUM(units) housing_units, SUM(occupied) housholds FROM demographic_warehouse.fact.housing WHERE datasource_id = 19 GROUP BY mgra_id) estimates
		ON urbansim.MGRA = estimates.mgra
WHERE
	urbansim.housing_units <> estimates.housing_units
;

/*################ STEP 14: LOAD HOUSEHOLD TABLE ######################*/
/*
INSERT INTO urbansim.households (
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

IF OBJECT_ID('urbansim.households') IS NOT NULL
    DROP TABLE urbansim.households
GO
;

SELECT *
INTO urbansim.households
FROM spacecore.input.vi_households
;
/*################ STEP 15: THIS IS A 2010 HH FILE WITH 2015 BUILDINGS, SHAVE OFF HH WHERE BUILDINGS WERE DEMOLISHED SINCE 2010 ######################*/
WITH hh AS (
SELECT
  hh.mgra
  ,household_id
  ,ROW_NUMBER() OVER (PARTITION BY hh.mgra ORDER BY household_id) idx
  ,bldgs.housing_units
FROM
  urbansim.households hh
  INNER JOIN (SELECT MGRA, SUM(residential_units) housing_units FROM urbansim.buildings usb INNER JOIN gis.ludu2015 lc ON usb.subparcel_id = lc.subParcel GROUP BY MGRA) bldgs
    ON hh.mgra = bldgs.MGRA
)
--SELECT * FROM urbansim.households WHERE household_id IN (SELECT household_id FROM hh WHERE idx > housing_units)
DELETE FROM urbansim.households WHERE household_id IN (SELECT household_id FROM hh WHERE idx > housing_units)
;

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
		urbansim.buildings
		,ref.numbers n
		WHERE n.numbers <= residential_units) bldgs
	INNER JOIN gis.ludu2015 lc ON lc.subParcel = bldgs.subparcel_id),
	hh AS (
		SELECT 
		  ROW_NUMBER() OVER (PARTITION BY mgra ORDER BY household_id) idx
		  ,household_id
		  ,mgra
		FROM
		  urbansim.households
)
UPDATE h
SET h.building_id = bldg.building_id
FROM
  urbansim.households h
  INNER JOIN hh ON h.household_id = hh.household_id
  INNER JOIN bldg ON bldg.MGRA = hh.mgra AND bldg.idx = hh.idx
;

/*################ STEP 17: SOME MORE FINAL CHECKS TO ENSURE MGRA AND REGIONAL UNIT CONSISTENCY ######################*/
SELECT COUNT(*) FROM urbansim.households WHERE building_id = 0

SELECT * FROM
(SELECT COUNT(*) hh, mgra FROM urbansim.households GROUP BY mgra) as hh
INNER JOIN (SELECT SUM(residential_units) units, mgra FROM urbansim.buildings INNER JOIN gis.ludu2015 ON subParcel = subparcel_id GROUP BY mgra) bldg ON hh.mgra = bldg.mgra
WHERE hh > units

/*######################################## EMP SPACE PROCESSING ########################################*/
--DECLARE @employment_vacancy float = 0.3;

/*** INSERT PLACEHOLDER FOR MILITARY EMP AT MGRA LEVEL ***/
--INSERT BUILDING IN MGRA WHERE MIL EMP AND CURRENTLY NO BUILDING	--6
INSERT INTO urbansim.buildings WITH (TABLOCK) (
	building_id
	,subparcel_id
	,parcel_id
	,development_type_id
	,mgra_id
	,shape
	,centroid
	,data_source
	,subparcel_assignment
	,assign_jobs
	)
SELECT
	1000000 + job_id			--bldgID
	,lc.subParcel				--subparcelID
	,lc.parcelid				--parcelID
	,dev.development_type_id	--development_type_id
	,bme.mgra					--mgra_id
	,m.Shape.STCentroid().STBuffer(1)				--shape
	,m.Shape.STCentroid()	--centroid
	,'PLACEHOLDER_MilitaryEmp_MGRAs'		--dataSource
	,'PLACEHOLDER_MIL'			--subparcel_assignment
	,0							--assign_jobs
FROM (SELECT mgra, MIN(job_id) AS job_id FROM input.jobs_military_2012_2016 WHERE yr = 2015 GROUP BY mgra)AS bme
JOIN ref.mgra13 AS m ON bme.mgra = m.MGRA
JOIN gis.ludu2015 AS lc ON m.Shape.STCentroid().STWithin(lc.shape) = 1
LEFT JOIN ref.development_type_lu_code dev ON lc.lu = dev.lu_code
WHERE bme.mgra NOT IN 									--CURRENTLY NO BUILDING
					(SELECT DISTINCT mgra_id
					FROM urbansim.buildings
					WHERE data_source IN ('Sampled footprint', 'SANDAG BLDG FOOTPRINT', 'SANDAG Camp Pendleton Digitized Bldg')
					AND development_type_id NOT IN (19, 20, 21)
					AND COALESCE(residential_units, 0) = 0
					AND COALESCE(residential_sqft, 0) = 0
					)
;
--SELECT BUILDING FROM MGRA, SORTED BY DEVELOPMENT TYPE AND FROM LARGEST, AND OVERWRITE ASSIGNMENT TO MIL
WITH mgra_b AS (
	SELECT building_id, mgra_id, data_source, assign_jobs,development_type_id
	FROM (
		SELECT 
			ROW_NUMBER() OVER(PARTITION BY mgra_id ORDER BY dev_case, area DESC) row_num
			,building_id
			,mgra_id
			,data_source
			,assign_jobs
			,development_type_id
			,dev_case
			,area
		FROM
			(SELECT
				building_id
				,mgra_id
				,data_source
				,assign_jobs
				,development_type_id
				,shape
				,CAST(shape.STArea() AS int) AS area
				,CASE development_type_id
					WHEN 29 THEN 1			--Military Reservation
					WHEN 23 THEN 2			--Military Residential (Non GQ)
					WHEN 16 THEN 3			--Government Operations
					WHEN 3 THEN 4			--Heavy Industry
					WHEN 2 THEN 5			--Light Industrial
					WHEN 4 THEN 6			--Office
					WHEN 6 THEN 7			--Depot
					WHEN 5 THEN 8			--Retail
					WHEN 32 THEN 7			--GQ - Non-Institutional - Other
					ELSE 10
				END AS dev_case
			FROM urbansim.buildings
			WHERE data_source IN ('Sampled footprint', 'SANDAG BLDG FOOTPRINT', 'SANDAG Camp Pendleton Digitized Bldg', 'PLACEHOLDER_MilitaryEmp_MGRAs')		--CURRENTLY NO BUILDING	--INCLUDE ADDED MIL
			AND development_type_id NOT IN (19, 20, 21)		--NON RES>>
			AND COALESCE(residential_units, 0) = 0
			AND COALESCE(residential_sqft, 0) = 0
			AND mgra_id IN (SELECT DISTINCT mgra FROM input.jobs_military_2012_2016 WHERE yr = 2015)
			) AS usb
		JOIN (SELECT DISTINCT mgra FROM input.jobs_military_2012_2016 WHERE yr = 2015) AS mil
			ON usb.mgra_id = mil.mgra
		) x
	WHERE row_num = 1
)
UPDATE
	usb
SET
	subparcel_assignment = 'PLACEHOLDER_MIL'
	,assign_jobs = 0
FROM
	urbansim.buildings AS usb
JOIN
	mgra_b ON usb.building_id = mgra_b.building_id

/* ##### ASSIGN BLOCK ID ##### */
UPDATE
	usb
SET
	usb.block_id = b.blockid10
FROM
	(SELECT * FROM urbansim.buildings) usb
JOIN ref.blocks b
ON b.Shape.STContains(usb.shape.STCentroid()) = 1
WHERE usb.block_id IS NULL 
;

/*#################### ASSIGN JOB_SPACES FROM EDD ####################*/
/*
JOB SPACES FOR PRIVATE ('WAC') AND SELFEMPLOYED ('SEM') ARE PROCESSED IN: job_spaces.sql
JOB SPACES FOR 'GOV' AND 'MIL' ARE ADDED TO BUILDINGS LATER, IN THEIR ALLOCATION SCRIPTS.
*/

;
/*** PARCELS DATA ***/
--GET PARCEL DATA: JURISDICTION ID
UPDATE
	usb
SET
	usb.jurisdiction_id = p.jurisdiction_id
FROM
	urbansim.buildings usb
JOIN urbansim.parcels AS p
ON usb.parcel_id = p.parcel_id
--PROCEED TO PARCEL LEVEL ADJUSTMENTS SCRIPT

/***#################### CHECKS ####################***/
--SELECT SUM(emp_adj)
--FROM socioec_data.ca_edd.emp_2013

--SELECT COUNT(*)
--FROM spacecore.input.jobs_wac_2013

--SELECT SUM(job_spaces)
--FROM urbansim.buildings

--SELECT COUNT(*)
--FROM urbansim.jobs


SELECT SUM(residential_units)
FROM urbansim.buildings

SELECT COUNT(*)
FROM urbansim.households

SELECT jurisdiction_id, SUM(residential_units) residential_units
FROM urbansim.buildings
GROUP BY jurisdiction_id
ORDER BY jurisdiction_id


/*** CHECK AT MGRA LEVEL ***/
SELECT b.mgra, bldg_units, lc_units, bldg_units - lc_units AS units_diff
FROM (SELECT mgra_id AS mgra, SUM(residential_units) AS bldg_units FROM urbansim.buildings GROUP BY mgra_id) AS b
JOIN (SELECT mgra, SUM(du) AS lc_units FROM gis.ludu2015 group by mgra) AS l
	ON b.mgra = l.mgra
--WHERE bldg_units <> lc_units
WHERE bldg_units - lc_units <> 0
ORDER BY bldg_units - lc_units DESC