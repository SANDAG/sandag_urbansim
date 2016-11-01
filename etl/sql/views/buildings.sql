USE spacecore
--IF OBJECT_ID('urbansim.buildings') IS NOT NULL
--    DROP TABLE urbansim.buildings
--GO
CREATE TABLE urbansim.buildings(
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
INSERT INTO urbansim.buildings WITH (TABLOCK) (
	building_id
	,subparcel_id
	,parcel_id
	,shape
	,centroid
	,data_source
	)
SELECT
	bldgid						--bldgID
	,subparcel_					--subparcelID
	,parcel_id					--parcelID
	,ogr_geometry				--shape
	,ogr_geometry.STCentroid()	--centroid
	,data_sourc					--dataSource
FROM gis.buildings

--CHECK FOR NULL SHAPES AND REMOVE
DELETE FROM [spacecore].[urbansim].[buildings]
WHERE shape IS NULL

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
	urbansim.buildings usb
JOIN gis.landcore lc
ON usb.subparcel_id = lc.subParcel
JOIN ref.development_type_lu_code dev											--_XX WILL NOT JOIN TO LANDCORE WHEN NO MATCH IN LU
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
	urbansim.buildings usb
	LEFT JOIN 
		(SELECT parcel_id,  COUNT(parcel_id) bldgs						--NUMBER OF BUILDINGS
		FROM urbansim.buildings
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
	urbansim.buildings usb
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


/*#################### START RES AND EMP SPACE ####################*/

/*#################### RES PROCESSING ####################*/

/*################## STEP 6: FOR SUB-PARCELS WITH NO BUILDINGS STILL AND DU, ASSIGN FAKE BUILDING POINT  ###################*/
--INSERT PLACEHOLDER CENTROIDS
INSERT INTO urbansim.buildings (building_id, development_type_id, subparcel_id, parcel_id, mgra_id, shape, centroid, data_source, subparcel_assignment)
SELECT 
	lc.subParcel + 2000000 AS building_id		--INSERTED BUILDING_ID > 2,000,000
	,dev.development_type_id
	,lc.subParcel
	,parcelID
	,MGRA
	,lc.Shape.STPointOnSurface().STBuffer(1) AS shape		--TEMP
	,lc.Shape.STBuffer(-2).STPointOnSurface() AS centroid
	,'PLACEHOLDER' AS data_source
	,'PLACEHOLDER_RES' AS subparcel_assignment
FROM 
	spacecore.gis.landcore lc
	LEFT JOIN urbansim.buildings usb ON lc.subParcel = usb.subparcel_id
	JOIN ref.development_type_lu_code dev ON lc.lu = dev.lu_code
WHERE 
	du > 0
	AND usb.subparcel_id is null

--UPDATE PLACEHOLDER SHAPES TO MATCH CENTROIDS
UPDATE urbansim.buildings
SET shape = centroid.STBuffer(1)
WHERE subparcel_assignment = 'PLACEHOLDER_RES'

/*################## STEP 7: VERIFY THAT ALL SUB-PARCEL WITH DU HAVE A BUILDING  ###################*/
SELECT COUNT(*) missing_res_building FROM 
	spacecore.gis.landcore lc
	LEFT JOIN urbansim.buildings usb ON lc.subParcel = usb.subparcel_id
WHERE 
	du > 0
	AND usb.subparcel_id is null

/*###### STEP 8: SET BUILDING'S RESIDENTIAL UNITS ON SUBPARCELS WITH ONLY ONE BUILDING #####*/
UPDATE usb
  SET usb.residential_units = lc.du
FROM
  urbansim.buildings usb
  INNER JOIN spacecore.gis.landcore lc ON lc.subParcel = usb.subparcel_id
WHERE
  usb.subparcel_id IN (SELECT subparcel_id FROM urbansim.buildings usb GROUP BY subparcel_id HAVING count(*) = 1)

/*###### STEP 9: SET BUILDINGS W/ SUB-PARCELS WITH NO RESIDENTIAL UNITS TO ZERO #####*/
UPDATE usb
  SET usb.residential_units = lc.du
FROM
  urbansim.buildings usb
  INNER JOIN spacecore.gis.landcore lc ON lc.subParcel = usb.subparcel_id
WHERE
  usb.subparcel_id IN (SELECT subparcel_id FROM urbansim.buildings usb GROUP BY subparcel_id HAVING count(*) > 1)
  AND lc.du = 0

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
    INNER JOIN spacecore.gis.landcore lc ON usb.subparcel_id = lc.subParcel
  WHERE
    subparcel_id IN (SELECT subparcel_id FROM urbansim.buildings usb WHERE usb.residential_units is null GROUP BY subparcel_id HAVING count(*) > 1))


UPDATE usb
  SET usb.residential_units = bldgs.units
FROM
	urbansim.buildings usb
	INNER JOIN bldgs ON usb.building_id = bldgs.building_id

/*################ STEP 11: SOME FINAL CHECKS TO ENSURE MGRA AND REGIONAL UNIT CONSISTENCY ######################*/
SELECT SUM(residential_units) bldg_units FROM urbansim.buildings
SELECT SUM(du) lc_units FROM spacecore.gis.landcore

SELECT 
  usb.subparcel_id
  ,lc.du lc_units
  ,SUM(residential_units) bldg_units
FROM 
  urbansim.buildings usb
  INNER JOIN spacecore.gis.landcore lc ON usb.subparcel_id = lc.subParcel
GROUP BY
  usb.subparcel_id
  ,lc.du
HAVING lc.du <> SUM(residential_units)


/*############ STEP 12: UPDATE RESIDENTIAL SQ FT WHERE DATA AVAILABLE ###### */
UPDATE usb
  SET usb.residential_sqft = usb.residential_units * asr_units.avg_unit_size
FROM
  urbansim.buildings usb
  INNER JOIN spacecore.gis.landcore lc ON usb.subparcel_id = lc.subParcel
  INNER JOIN
    (SELECT
      p.PARCELID
	  ,CAST(TOTAL_LVG_AREA as float) / bldgs.res_units avg_unit_size
    FROM
      (SELECT PARCELID, SUM(TOTAL_LVG_AREA) TOTAL_LVG_AREA FROM spacecore.gis.parcels GROUP BY PARCELID) p
       INNER JOIN (SELECT lc.parcelID, SUM(residential_units) res_units FROM urbansim.buildings usb INNER JOIN spacecore.gis.landcore lc ON usb.subparcel_id = lc.subParcel GROUP BY lc.parcelID HAVING SUM(residential_units) > 0) bldgs
         ON p.PARCELID = bldgs.parcelID
	   WHERE TOTAL_LVG_AREA IS NOT NULL AND TOTAL_LVG_AREA > 0) asr_units ON lc.parcelID = asr_units.PARCELID


/*################ STEP 13: SOME MORE FINAL CHECKS TO ENSURE MGRA AND REGIONAL UNIT CONSISTENCY ######################*/
SELECT building_id, residential_units, residential_sqft FROM urbansim.buildings usb WHERE residential_units > 0 AND residential_sqft <= 0

SELECT * 
FROM
  (SELECT MGRA, SUM(residential_units) housing_units FROM urbansim.buildings usb INNER JOIN spacecore.gis.landcore lc ON usb.subparcel_id = lc.subParcel GROUP BY MGRA) urbansim
  INNER JOIN (SELECT mgra_id % 1300000 mgra, SUM(units) housing_units, SUM(occupied) housholds FROM demographic_warehouse.fact.housing WHERE datasource_id = 19 GROUP BY mgra_id) estimates
    ON urbansim.MGRA = estimates.mgra
WHERE
  urbansim.housing_units <> estimates.housing_units

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
SELECT *
INTO urbansim.households
FROM spacecore.input.vi_households

/*################ STEP 15: THIS IS A 2010 HH FILE WITH 2015 BUILDINGS, SHAVE OFF HH WHERE BUILDINGS WERE DEMOLISHED SINCE 2010 ######################*/
WITH hh AS (
SELECT
  hh.mgra
  ,household_id
  ,ROW_NUMBER() OVER (PARTITION BY hh.mgra ORDER BY household_id) idx
  ,bldgs.housing_units
FROM
  urbansim.households hh
  INNER JOIN (SELECT MGRA, SUM(residential_units) housing_units FROM urbansim.buildings usb INNER JOIN spacecore.gis.landcore lc ON usb.subparcel_id = lc.subParcel GROUP BY MGRA) bldgs
    ON hh.mgra = bldgs.MGRA
)

--SELECT * FROM urbansim.households WHERE household_id IN (SELECT household_id FROM hh WHERE idx > housing_units)
DELETE FROM urbansim.households WHERE household_id IN (SELECT household_id FROM hh WHERE idx > housing_units)


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
INNER JOIN spacecore.gis.landcore lc ON lc.subParcel = bldgs.subparcel_id),
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

/*################ STEP 17: SOME MORE FINAL CHECKS TO ENSURE MGRA AND REGIONAL UNIT CONSISTENCY ######################*/
SELECT COUNT(*) FROM urbansim.households WHERE building_id = 0

SELECT * FROM
(SELECT COUNT(*) hh, mgra FROM urbansim.households GROUP BY mgra) as hh
INNER JOIN (SELECT SUM(residential_units) units, mgra FROM urbansim.buildings INNER JOIN spacecore.gis.landcore ON subParcel = subparcel_id GROUP BY mgra) bldg ON hh.mgra = bldg.mgra
WHERE hh > units

/*#################### EMP SPACE PROCESSING ####################*/
DECLARE @employment_vacancy float = 0.3;

/* ##### INSERT PLACEHOLDER BUILDINGS FOR EMP ##### */
INSERT INTO urbansim.buildings(
	development_type_id
	,building_id
	,parcel_id
	,mgra_id
	,improvement_value
	,residential_units
	,residential_sqft
	,non_residential_sqft
	,job_spaces
	,non_residential_rent_per_sqft
	,price_per_sqft
	,stories
	,year_built
	,shape
	,centroid
	,data_source
	,subparcel_assignment
	,subparcel_id
	,luz_id
	)
SELECT
	dev.development_type_id
	,lc.subParcel + 4000000 AS building_id
	,lc.parcelID
	,lc.mgra as mgra
	,NULL AS improvement_value
	,0 AS residential_units
	,0 AS residential_sqft
	,NULL AS non_residential_sqft
	,emp.emp_adj AS job_spaces
	,NULL AS non_residential_rent_per_sqft
	,NULL AS price_per_sqft
	,NULL AS stories
	,NULL AS year_built
	,lc.Shape.STPointOnSurface().STBuffer(1) AS shape		--TEMP
	,lc.Shape.STBuffer(-2).STPointOnSurface() AS centroid
	,'PLACEHOLDER' AS data_source
	,'PLACEHOLDER_EMP' AS subparcel_assignment
	,lc.subParcel
	,NULL AS luz_id
FROM gis.landcore lc
INNER JOIN (SELECT lc.subparcel, SUM(emp.emp_adj) emp_adj		--GROUP BY SUBPARCEL FOR UNIQUE BUILDING_ID
			FROM socioec_data.ca_edd.emp_2013 emp
			JOIN gis.landcore lc 
			ON lc.Shape.STContains(emp.shape) = 1
			GROUP BY lc.subparcel) emp
ON lc.subparcel = emp.subparcel
LEFT JOIN (SELECT subparcel_id FROM urbansim.buildings GROUP BY subparcel_id) usb
ON lc.subParcel = usb.subparcel_id
JOIN ref.development_type_lu_code dev
ON lc.lu = dev.lu_code
WHERE emp.emp_adj IS NOT NULL
AND usb.subparcel_id IS NULL

--UPDATE PLACEHOLDER SHAPES TO MATCH CENTROIDS
UPDATE urbansim.buildings
SET shape = centroid.STBuffer(1)
WHERE subparcel_assignment = 'PLACEHOLDER_EMP'

/*#################### ASSIGN JOBS ####################*/
--DECLARE @employment_vacancy float = 0.3;
/* ##### ASSIGN JOBS TO SINGLE BUILDING SUBPARCELS ##### */
WITH single_bldg_jobs AS (
	SELECT lc.subParcel AS subparcel_id
		,SUM(CAST(ROUND(emp_adj*(1+@employment_vacancy),0)AS int)) emp
	FROM gis.landcore lc
	INNER JOIN socioec_data.ca_edd.emp_2013 emp 
	ON lc.Shape.STContains(emp.shape) = 1
	INNER JOIN (SELECT subparcel_id FROM urbansim.buildings GROUP BY subparcel_id HAVING COUNT(*) = 1) single_bldg
	ON lc.subParcel = single_bldg.subparcel_id
	WHERE emp.emp_adj IS NOT NULL
	GROUP BY lc.subParcel
	)
UPDATE 
	usb
SET 
	usb.job_spaces = sb.emp
FROM 
urbansim.buildings usb
JOIN single_bldg_jobs sb
ON usb.subparcel_id = sb.subparcel_id
;

/* ##### ASSIGN JOBS TO MULTIPLE BUILDING SUBPARCELS ##### */
--DECLARE @employment_vacancy float = 0.3;
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
    urbansim.buildings usb
    INNER JOIN (SELECT subParcel, SUM(CAST(ROUND(emp_adj*(1+@employment_vacancy),0)AS int)) emp
		FROM gis.landcore lc
		INNER JOIN socioec_data.ca_edd.emp_2013 emp 
		ON lc.Shape.STContains(emp.shape) = 1
		WHERE emp.emp_adj IS NOT NULL
		GROUP BY lc.subParcel) lc
	ON usb.subparcel_id = lc.subParcel
  WHERE
    usb.subparcel_id IN (SELECT subparcel_id FROM urbansim.buildings usb GROUP BY subparcel_id HAVING count(*) > 1)
	)

UPDATE usb
  SET usb.job_spaces = bldgs.jobs
FROM
	urbansim.buildings usb
	INNER JOIN bldgs ON usb.building_id = bldgs.building_id
;

/* ##### ASSIGN BLOCK ID ##### */
UPDATE
	usb
SET
	usb.block_id = b.blockid10
FROM
	urbansim.buildings usb
JOIN ref.blocks b
ON b.Shape.STContains(usb.shape.STCentroid()) = 1
WHERE usb.block_id IS NULL 

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
  INNER JOIN (SELECT bldg.block_id,  CAST(ROUND((jobs.jobs - bldg.spaces) * 1.5,0) as INT) deficit FROM
				(SELECT block_id, COUNT(*) jobs FROM spacecore.input.jobs_wac_2013 GROUP BY block_id) jobs
				FULL OUTER JOIN (SELECT block_id, SUM(job_spaces) spaces FROM urbansim.buildings GROUP BY block_id) bldg ON jobs.block_id = bldg.block_id
				WHERE jobs.jobs > bldg.spaces) deficit ON usb.block_id = deficit.block_id
WHERE
  usb.job_spaces > 0
)

UPDATE usb
  SET usb.job_spaces = usb.job_spaces + jobs
FROM
  urbansim.buildings usb INNER JOIN
  bldg ON usb.building_id = bldg.building_id


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
  INNER JOIN (SELECT bldg.block_id,  CAST(ROUND((jobs.jobs - bldg.spaces) * 1.5,0) as INT) deficit FROM
				(SELECT block_id, COUNT(*) jobs FROM spacecore.input.jobs_wac_2013 GROUP BY block_id) jobs
				FULL OUTER JOIN (SELECT block_id, SUM(job_spaces) spaces FROM urbansim.buildings GROUP BY block_id) bldg ON jobs.block_id = bldg.block_id
				WHERE jobs.jobs > bldg.spaces) deficit ON usb.block_id = deficit.block_id
)

UPDATE usb
  SET usb.job_spaces = usb.job_spaces + jobs
FROM
  urbansim.buildings usb INNER JOIN
  bldg ON usb.building_id = bldg.building_id

/*##### JOBS TABLE #####*/
TRUNCATE TABLE spacecore.urbansim.jobs;
WITH bldg as (
SELECT
  ROW_NUMBER() OVER (PARTITION BY block_id ORDER BY building_id) idx
  ,building_id
  ,block_id
FROM
(SELECT
  building_id
  ,subparcel_id
  ,job_spaces
  ,block_id
FROM
urbansim.buildings
,spacecore.ref.numbers n
WHERE n.numbers <= job_spaces) bldgs
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

INSERT INTO spacecore.urbansim.jobs (job_id, sector_id, building_id)
SELECT jobs.job_id
	,jobs.sector_id
	,bldg.building_id
FROM bldg
INNER JOIN jobs
ON bldg.block_id = jobs.block_id
AND bldg.idx = jobs.idx


/***#################### WHERE SQFT IS NULL, DERIVE FROM UNITS, JOB_SPACES ####################***/
UPDATE urbansim.buildings
SET [floorspace_source] = 'units_jobs_derived'
	,[residential_sqft] = [residential_units] * 300
	,[non_residential_sqft] = [job_spaces] * 300
WHERE ISNULL([residential_sqft], 0) + ISNULL([non_residential_sqft], 0) = 0
	AND ISNULL([residential_units], 0) + ISNULL([job_spaces], 0) > 0




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


/*** CHECK AT MGRA LEVEL ***/
SELECT b.mgra, bldg_units, lc_units, bldg_units - lc_units AS units_diff
FROM (SELECT mgra_id AS mgra, SUM(residential_units) AS bldg_units FROM urbansim.buildings GROUP BY mgra_id) AS b
JOIN (SELECT mgra, SUM(du) AS lc_units FROM spacecore.gis.landcore group by mgra) AS l
	ON b.mgra = l.mgra
--WHERE bldg_units <> lc_units
WHERE bldg_units - lc_units <> 0
ORDER BY bldg_units - lc_units DESC