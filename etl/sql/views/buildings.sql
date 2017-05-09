/*
OUTPUT:
[urbansim].[households]
[urbansim].[jobs]
[urbansim].[buildings]
*/
*
USE spacecore

IF OBJECT_ID('urbansim.buildings') IS NOT NULL
    DROP TABLE urbansim.buildings
GO

CREATE TABLE urbansim.buildings(
	building_id bigint NOT NULL
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
	,assign_jobs
	)
SELECT
	bldgid						--bldgID
	,subparcel_					--subparcelID
	,parcel_id					--parcelID
	,ogr_geometry				--shape
	,ogr_geometry.STCentroid()	--centroid
	,data_sourc					--dataSource
	,1							--assign_jobs
FROM gis.buildings

--INSERT ADDITIONAL BUILDINGS: PUBLIC FACILITIES AND OTHER
--REMOVE OVERLAPPING BUILDINGS
DELETE
FROM urbansim.buildings
WHERE parcel_id IN (SELECT parcelid FROM [GIS].[buildings_public_facilities])
OR parcel_id IN (SELECT parcelid FROM [GIS].[buildings_camp_pendleton])

--LOAD FROM PUBLIC FACILITIES BUILDINGS
INSERT INTO urbansim.buildings WITH (TABLOCK) (
	building_id
	,subparcel_id
	,parcel_id
	,development_type_id
	,mgra_id
	,job_spaces
	,shape
	,centroid
	,data_source
	,assign_jobs
	)
SELECT
	800000 + id					--bldgID
	,lc.subParcel				--subparcelID
	,bpf.parcelid				--parcelID
	,dev.development_type_id	--development_type_id
	,mgra13						--mgra_id
	,emp						--job_spaces
	,ogr_geometry				--shape
	,ogr_geometry.STCentroid()	--centroid
	,datasource					--dataSource
	,0						--assign_jobs
FROM gis.buildings_public_facilities AS bpf
JOIN gis.ludu2015 AS lc ON bpf.ogr_geometry.STCentroid().STWithin(lc.shape) = 1
LEFT JOIN ref.development_type_lu_code dev											--_XX WILL NOT JOIN TO LANDCORE WHEN NO MATCH IN LU
ON bpf.lu = dev.lu_code


--LOAD FROM CAMP PENDLETON BUILDINGS
INSERT INTO urbansim.buildings WITH (TABLOCK) (
	building_id
	,subparcel_id
	,parcel_id
	,development_type_id
	,mgra_id
	,shape
	,centroid
	,data_source
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
	(SELECT * FROM urbansim.buildings WHERE assign_jobs = 1) usb		--DO NOT USE MIL/PF BUILDINGS
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
		(SELECT p.parcelID
			--,LEFT(par.apn,8) apn8
			,SUM([CURRENT_IMPS]) imps
			,SUM([TOTAL_LVG_AREA]+[ADDITION_AREA]) sqft
			,AVG(CASE WHEN [YEAR_EFFECTIVE] <= 16
					THEN 2000 + CAST([YEAR_EFFECTIVE] AS smallint)
					ELSE 1900 + CAST([YEAR_EFFECTIVE] AS smallint)
				END ) year_built
			,COUNT(*) num
		FROM spacecore.input.assessor_par par
		JOIN
			(SELECT par.apn
				,parcelID
			FROM GIS.parcels AS p				--ASSESOR PARCELS
			JOIN input.assessor_par AS par
				ON p.APN = par.APN) p
		ON p.apn = par.apn
		GROUP BY p.parcelID
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
DECLARE @employment_vacancy float = 0.1;

/*** INSERT PLACEHOLDER FOR MILITARY EMP AT MGRA LEVEL ***/
--INSERT BUILDING IN MGRA WHERE MIL EMP AND CURRENTLY NO BUILDING
INSERT INTO urbansim.buildings WITH (TABLOCK) (
	building_id
	,subparcel_id
	,parcel_id
	,development_type_id
	,mgra_id
	,job_spaces
	,shape
	,centroid
	,data_source
	,subparcel_assignment
	,assign_jobs
	)
SELECT
	1000000 + ogr_fid			--bldgID
	,lc.subParcel				--subparcelID
	,lc.parcelid				--parcelID
	,dev.development_type_id	--development_type_id
	,mgra13						--mgra_id
	,employment					--job_spaces
	,ogr_geometry.STCentroid().STBuffer(1)				--shape
	,ogr_geometry.STCentroid()	--centroid
	,'MilitaryEmp_MGRAs'		--dataSource
	,'PLACEHOLDER_MIL'			--subparcel_assignment
	,0							--assign_jobs
FROM gis.buildings_military_emp_mgra AS bme
JOIN gis.ludu2015 AS lc ON bme.ogr_geometry.STCentroid().STWithin(lc.shape) = 1
LEFT JOIN ref.development_type_lu_code dev ON lc.lu = dev.lu_code
WHERE mgra13 NOT IN 									--CURRENTLY NO BUILDING
					(SELECT DISTINCT mgra_id
					FROM urbansim.buildings
					WHERE data_source IN ('Sampled footprint', 'SANDAG BLDG FOOTPRINT', 'SANDAG Camp Pendleton Digitized Bldg')
					AND development_type_id NOT IN (19, 20, 21)
					AND COALESCE(residential_units, 0) = 0
					AND COALESCE(residential_sqft, 0) = 0
					)

--SELECT LARGEST BUILDING FROM MGRA AND OVERWRITE DATA SOURCE TO MIL
WITH mgra_b AS(
	SELECT building_id, mgra_id, data_source, assign_jobs
	FROM (
		SELECT ROW_NUMBER() OVER (PARTITION BY mgra_id ORDER BY shape.STArea() DESC) AS row_num
			,building_id
			,mgra_id
			,data_source
			,assign_jobs
			,residential_units
		FROM urbansim.buildings
		WHERE data_source IN ('Sampled footprint', 'SANDAG BLDG FOOTPRINT', 'SANDAG Camp Pendleton Digitized Bldg', 'MilitaryEmp_MGRAs')		--CURRENTLY NO BUILDING	--INCLUDE ADDED MIL
			AND development_type_id NOT IN (19, 20, 21)		--NON RES>>
			AND COALESCE(residential_units, 0) = 0
			AND COALESCE(residential_sqft, 0) = 0
			AND mgra_id IN (SELECT mgra13 FROM gis.buildings_military_emp_mgra)
		) x
	WHERE row_num = 1
)
/*
SELECT mgra_b.building_id, mgra_b.mgra_id, mgra_b.data_source, mgra_b.assign_jobs, residential_units
FROM urbansim.buildings usb
JOIN mgra_b ON usb.building_id = mgra_b.building_id
*/
UPDATE
	usb
SET
	data_source = 'MilitaryEmp_MGRAs'
	,subparcel_assignment = 'PLACEHOLDER_MIL'
	,assign_jobs = 0
FROM
	urbansim.buildings AS usb
JOIN
	mgra_b ON usb.building_id = mgra_b.building_id

/* ##### INSERT PLACEHOLDER BUILDINGS FOR EMP FOR BLOCKS WITH EMP BUT NO BUILDING ##### */
-- ASSIGN BLOCK ID
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

--INSERT PLACEHOLDER CENTROIDS
WITH wac AS(
	SELECT DISTINCT block_id
	FROM input.jobs_wac_2013
	WHERE block_id NOT IN (SELECT DISTINCT block_id FROM urbansim.buildings WHERE assign_jobs = 1)	--DO NOT USE MIL/PF BUILDINGS
)
INSERT INTO urbansim.buildings(
	building_id
	,parcel_id
	,block_id
	,job_spaces
	,shape
	,centroid
	,data_source
	,subparcel_assignment
	,assign_jobs
	)
SELECT
	CAST(wac.block_id AS bigint) + 500000000000000 AS building_id
	,1100000 + ROW_NUMBER() OVER(ORDER BY (SELECT NULL))		--PLACEHOLDER, DOES NOT ALLOW NULL
	,wac.block_id
	,0									--job_spaces			--TO AVOID NULL IN FOLLOWING AGGREGATIONS
	,bk.shape.STCentroid().STBuffer(1)							--PLACEHOLDER, DOES NOT ALLOW NULL
	,CASE wac.block_id 
		WHEN bkk.BLOCKID10 THEN bk.shape.STCentroid()
		ELSE bk.shape.STBuffer(-1).STPointOnSurface()
		END AS centroid
	,'PLACEHOLDER_BLOCK'				--data_source
	,'PLACEHOLDER_EMP'					--subparcel_assignment
	,1									--assign_jobs
FROM wac
JOIN ref.blocks AS bk ON wac.block_id = bk.BLOCKID10 
JOIN ref.blocks AS bkk ON bk.shape.STCentroid().STWithin(bkk.shape) = 1

--UPDATE parcel_id AND OTHER COLUMNS
UPDATE usb
SET
	development_type_id = dev.development_type_id
	,parcel_id = lc.parcelid
	,mgra_id = lc.mgra
	,shape = usb.centroid.STBuffer(1)
	,subparcel_id = lc.subParcel
	--,NULL AS luz_id
FROM urbansim.buildings AS usb
JOIN gis.ludu2015 AS lc ON usb.centroid.STWithin(lc.shape) = 1
JOIN ref.development_type_lu_code dev ON lc.lu = dev.lu_code
WHERE data_source = 'PLACEHOLDER_BLOCK'


/*#################### ASSIGN JOB_SPACES FROM EMP ####################*/
DECLARE @employment_vacancy float = 0.1;
/* ##### ASSIGN JOB_SPACES TO SINGLE BUILDING SUBPARCELS ##### */
WITH single_bldg_jobs AS (
	SELECT lc.subParcel AS subparcel_id
		,SUM(CAST(CEILING(emp_adj*(1+@employment_vacancy))AS int)) emp
	FROM gis.ludu2015 lc
	INNER JOIN socioec_data.ca_edd.emp_2013 emp 
	ON lc.Shape.STContains(emp.shape) = 1
	INNER JOIN (SELECT subparcel_id 
				FROM urbansim.buildings 
				WHERE assign_jobs = 1		--DO NOT USE MIL/PF BUILDINGS
				GROUP BY subparcel_id 
				HAVING COUNT(*) = 1) single_bldg	
	ON lc.subParcel = single_bldg.subparcel_id
	WHERE emp.emp_adj IS NOT NULL
	GROUP BY lc.subParcel
	)
UPDATE 
	usb
SET 
	usb.job_spaces = sb.emp
FROM 
	(SELECT * FROM urbansim.buildings WHERE assign_jobs = 1) usb		--DO NOT USE MIL/PF BUILDINGS
JOIN single_bldg_jobs sb
ON usb.subparcel_id = sb.subparcel_id
;

/* ##### ASSIGN JOB_SPACES TO MULTIPLE BUILDING SUBPARCELS ##### */
DECLARE @employment_vacancy float = 0.1;
-----MAY WANT TO THINK ABOUT EXCLUDING REALLY SMALL BUILDINGS FROM THIS QUERY
WITH bldgs AS (
  SELECT
    usb.subparcel_id
   ,usb.building_id
   ,lc.emp/ COUNT(*) OVER (PARTITION BY usb.subparcel_id) +
     CASE 
       WHEN ROW_NUMBER() OVER (PARTITION BY usb.subparcel_id ORDER BY usb.shape.STArea()) <= (lc.emp % COUNT(*) OVER (PARTITION BY usb.subparcel_id)) THEN 1 
       ELSE 0 
	 END jobs
  FROM
    (SELECT * FROM urbansim.buildings WHERE assign_jobs = 1) usb		--DO NOT USE MIL/PF BUILDINGS
    INNER JOIN (SELECT subParcel, SUM(CAST(CEILING(emp_adj*(1+@employment_vacancy))AS int)) emp
		FROM gis.ludu2015 lc
		INNER JOIN socioec_data.ca_edd.emp_2013 emp 
		ON lc.Shape.STContains(emp.shape) = 1
		WHERE emp.emp_adj IS NOT NULL
		GROUP BY lc.subParcel) lc
	ON usb.subparcel_id = lc.subParcel
  WHERE
    usb.subparcel_id IN (SELECT subparcel_id FROM urbansim.buildings usb GROUP BY subparcel_id HAVING count(*) > 1)
	)
UPDATE
	usb
SET
	usb.job_spaces = bldgs.jobs
FROM
	(SELECT * FROM urbansim.buildings WHERE assign_jobs = 1) usb		--DO NOT USE MIL/PF BUILDINGS
	INNER JOIN bldgs ON usb.building_id = bldgs.building_id
;

/* #################### ASSIGN JOB_SPACES FROM WAC #################### */
--FIND A BUILDING PER BLOCK WHERE MORE JOB SPACES ARE NEEDED TO ACCOMMODATE WAC
SELECT wac.block_id
	,wac.job_spaces AS wj
	,usb.job_spaces AS jj
	,wac.job_spaces - usb.job_spaces AS deficit
FROM (SELECT block_id, SUM(COALESCE(job_spaces, 0)) AS job_spaces FROM urbansim.buildings WHERE assign_jobs = 1 GROUP BY block_id) AS usb
JOIN (SELECT block_id, COUNT(*) AS job_spaces FROM input.jobs_wac_2013 GROUP BY block_id) AS wac
	ON usb.block_id = wac.block_id
WHERE wac.job_spaces > usb.job_spaces

--UPDATE JOB SPACES TO ACCOMMODATE WAC
WITH b AS(
	SELECT building_id, block_id, job_sp, data_source
	FROM (
		SELECT ROW_NUMBER() OVER (PARTITION BY block_id ORDER BY shape.STArea() DESC) AS row_id
			,building_id
			,block_id
			,job_spaces AS job_sp
			,data_source
		FROM urbansim.buildings WHERE assign_jobs = 1) x
	WHERE row_id = 1
)
,sp AS(
	SELECT wac.block_id
		,wac.job_spaces AS wac_js
		,usb.job_spaces AS blk_js
		,wac.job_spaces - usb.job_spaces AS deficit
	FROM (SELECT block_id, SUM(COALESCE(job_spaces, 0)) AS job_spaces FROM urbansim.buildings WHERE assign_jobs = 1 GROUP BY block_id) AS usb
	JOIN (SELECT block_id, COUNT(*) AS job_spaces FROM spacecore.input.jobs_wac_2013 GROUP BY block_id) AS wac
		ON usb.block_id = wac.block_id
	WHERE wac.job_spaces > usb.job_spaces
)
/*
SELECT
	b.building_id
	,b.block_id
	,b.job_sp
	,sp.wac_js
	,sp.blk_js
	,COALESCE(b.job_sp, 0) + (sp.wac_js - COALESCE(sp.blk_js, 0)) AS job_sp_updt
*/
UPDATE b
SET b.job_sp = COALESCE(b.job_sp, 0) + (sp.wac_js - COALESCE(sp.blk_js, 0))
FROM b
JOIN sp ON b.block_id = sp.block_id

/*
--CHECK FOR BUILDINGS ASSIGNED TO NON-DEVELOPABLE PARCELS; ROW^^
SELECT *
FROM urbansim.buildings
WHERE subparcel_assignment = 'PLACEHOLDER_EMP'
AND development_type_id = 24					--ROAD RIGHT OF WAY
AND assign_jobs = 1
;
--QUICK-FIX BUILDINGS ASSIGNED TO NON-DEVELOPABLE PARCELS
WITH near AS(
	SELECT row_id, building_id, parcel_id, development_type_id, dist 
	FROM (
		SELECT
			ROW_NUMBER() OVER (PARTITION BY usb.building_id ORDER BY usb.building_id, usb.centroid.STDistance(usp.shape)) row_id
			,usb.building_id
			,usp.parcel_id
			,usp.development_type_id
			,usb.centroid.STDistance(usp.shape) AS dist
		FROM urbansim.buildings AS usb
			JOIN (SELECT * FROM urbansim.parcels WHERE development_type_id <> 24) AS usp
			ON usb.centroid.STBuffer(1000).STIntersects(usp.shape) = 1	--CHECK IF BUFFERDIST IS SUFFICIENT
		WHERE usb.subparcel_assignment = 'PLACEHOLDER_EMP'
		AND usb.development_type_id = 24
			) x
	WHERE row_id = 1
)
UPDATE
    usb
SET
    usb.parcel_id = near.parcel_id
	,usb.subparcel_id = NULL
	,development_type_id = near.development_type_id
FROM
    urbansim.buildings usb
	,near
WHERE
	usb.building_id = near.building_id
	AND usb.subparcel_assignment = 'PLACEHOLDER_EMP'
	AND usb.development_type_id = 24
;
*/
/*
/*################### ARBITRARILY ADD MORE SPACE FOR LEHD  ###########################*/
WITH bldg AS (
	SELECT
	  usb.building_id
	  ,deficit.block_id
	  ,deficit.deficit
	  ,deficit.deficit/ COUNT(*) OVER (PARTITION BY deficit.block_id) +
		 CASE 
		   WHEN ROW_NUMBER() OVER (PARTITION BY deficit.block_id ORDER BY usb.job_spaces desc) <= (deficit.deficit % COUNT(*) OVER (PARTITION BY deficit.block_id)) THEN 1 
		   ELSE 0 
		 END jobs
	FROM 
		urbansim.buildings usb 
		INNER JOIN (SELECT bldg.block_id, jobs, spaces,  CAST(ROUND((jobs.jobs - bldg.spaces) ,0) as INT) deficit, CAST(ROUND((jobs.jobs - bldg.spaces) * 1.3 ,0) as INT) deficit15
					FROM (SELECT block_id, COUNT(*) jobs FROM spacecore.input.jobs_wac_2013 GROUP BY block_id) jobs												--13,695 blocks	--1,377,100 jobs
					JOIN (SELECT block_id, SUM(job_spaces) spaces FROM urbansim.buildings GROUP BY block_id) bldg ON jobs.block_id = bldg.block_id				--30,307 blocks	--1,929,835 spaces
					WHERE jobs.jobs > bldg.spaces
					) deficit																																	--146 blocks	--2,197 deficit*1.5	--1,434 deficit
		ON usb.block_id = deficit.block_id
)
UPDATE usb
  SET usb.job_spaces = ISNULL(usb.job_spaces, 0) + jobs
FROM
  urbansim.buildings usb 
INNER JOIN
  bldg ON usb.building_id = bldg.building_id
WHERE ISNULL(usb.job_spaces, 0) + jobs > 0
;
*/

/*################### ALLOCATE JOBS INTO JOB_SPACES  ###########################*/
/*##### GENERATE JOBS TABLE #####*/
TRUNCATE TABLE urbansim.jobs;
--WRITE TO TABLE
WITH bldg as (
SELECT 
	ROW_NUMBER() OVER (PARTITION BY parcel_id, block_id ORDER BY row_space, job_spaces ) AS idx	--row_block
	,block_id
	,building_id
FROM(
	SELECT
		ROW_NUMBER() OVER (PARTITION BY building_id ORDER BY job_spaces)*100/job_spaces AS row_space
		,block_id
		,parcel_id
		,building_id
		,job_spaces
	FROM urbansim.buildings
	JOIN ref.numbers AS n ON n.numbers <= job_spaces
) x
),
jobs AS (
	SELECT 
	  ROW_NUMBER() OVER (PARTITION BY block_id ORDER BY job_id) idx
	  ,job_id
	  ,block_id
	  ,sector_id
	FROM
	  spacecore.input.jobs_wac_2013
)
INSERT INTO urbansim.jobs (job_id, sector_id, building_id)
SELECT jobs.job_id
	,jobs.sector_id
	,bldg.building_id
FROM bldg
RIGHT JOIN jobs
ON bldg.block_id = jobs.block_id
AND bldg.idx = jobs.idx


/***#################### WHERE SQFT IS NULL, DERIVE FROM UNITS, JOB_SPACES ####################***/
SELECT * FROM urbansim.buildings 
WHERE assign_jobs = 1
	AND ISNULL([residential_sqft], 0) + ISNULL([non_residential_sqft], 0) = 0
	AND ISNULL([residential_units], 0) + ISNULL([job_spaces], 0) > 0

--UPDATE
UPDATE usb
SET [floorspace_source] = 'units_jobs_derived'
	,[residential_sqft] = [residential_units] * 400
	,[non_residential_sqft] = [job_spaces] * 400
FROM (SELECT * FROM urbansim.buildings WHERE assign_jobs = 1) usb		--DO NOT USE MIL/PF BUILDINGS
WHERE ISNULL([residential_sqft], 0) + ISNULL([non_residential_sqft], 0) = 0
	AND ISNULL([residential_units], 0) + ISNULL([job_spaces], 0) > 0
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
SELECT SUM(emp_adj)
FROM socioec_data.ca_edd.emp_2013

SELECT COUNT(*)
FROM spacecore.input.jobs_wac_2013

SELECT SUM(job_spaces)
FROM urbansim.buildings

SELECT COUNT(*)
FROM urbansim.jobs


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