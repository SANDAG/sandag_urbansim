USE spacecore
IF OBJECT_ID('urbansim.buildings') IS NOT NULL
    DROP TABLE urbansim.buildings
GO
CREATE TABLE urbansim.buildings(
	building_id int IDENTITY(1,1) NOT NULL
	,development_type_id smallint
	,parcel_id int
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
	,data_source nvarchar(50)
)
INSERT INTO urbansim.buildings WITH (TABLOCK) (shape, data_source)
SELECT 
	shape
	,dataSource
    --bldgID	--NOT UNIQUE ID
FROM gis.buildings

--CREATE A PRIMARY KEY SO WE CAN CREATE A SPATIAL INDEX
ALTER TABLE urbansim.buildings ADD CONSTRAINT pk_urbansim_buildings_building_id PRIMARY KEY CLUSTERED (building_id) 

--SET THE SHAPES TO BE NOT NULL SO WE CAN CREATE A SPATIAL INDEX
ALTER TABLE urbansim.buildings ALTER COLUMN shape geometry NOT NULL

--SELECT max(x_coord), min(x_coord), max(y_coord), min(y_coord) from gis.parcels

CREATE SPATIAL INDEX [ix_spatial_urbansim_buildings] ON urbansim.buildings
(
    shape
) USING  GEOMETRY_GRID
    WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
    CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

/** LANDCORE DATA **/
--GET LANDCORE DATA: PARCEL_ID, DEV_TYPE
UPDATE
	usb
SET
	usb.parcel_ID = lc.parcelID
	,usb.development_type_id = dev.development_type_id
FROM
	urbansim.buildings usb
	,gis.landcore lc
JOIN ref.development_type_lu_code dev 
ON lc.lu = dev.lu_code
WHERE usb.Shape.STCentroid().STWithin(lc.Shape) = 1

--VALIDATE PARCEL_ID TO EXISTING PARCELS, ASSIGN NEAREST PARCEL_ID
UPDATE
	usb
SET
	usb.parcel_id = usp.parcel_id
FROM
	urbansim.buildings usb
LEFT JOIN (
	SELECT row_id, building_id, parcel_id, dist 
	FROM (
		SELECT
			ROW_NUMBER() OVER (PARTITION BY usb.building_id ORDER BY usb.building_id, usb.shape.STCentroid().STDistance(usp.shape)) row_id
			,usb.building_id
			,usp.parcel_id
			,usb.shape.STCentroid().STDistance(usp.shape) AS dist
		FROM urbansim.parcels usp
			INNER JOIN (SELECT b.building_id
							--,b.residential_units
							--,b.parcel_id
							,p.parcel_id
							--,p.mgra_id
							,b.shape 
						FROM urbansim.buildings b 
						LEFT JOIN urbansim.parcels p 
						ON b.parcel_id = p.parcel_id 
						WHERE p.parcel_id IS NULL		--NO PARCEL_ID FOUND
						) usb 
			ON usb.shape.STCentroid().STBuffer(100).STIntersects(usp.shape) = 1
			) x
	WHERE row_id = 1
	) usp
ON usb.parcel_id = usp.parcel_id


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


/*
<<<<<<< HEAD




=======
/** Use the COSTAR data to set the values for job_spaces **/
/**
TODO: There are missing records on both inner joins. 
      parcel and sqft_per_job_by_devtype both need to
      be updated.
**/
UPDATE
    usb
SET
    --If the data is ***BAD(???)*** replace with 40 as the minimum
    usb.job_spaces = CEILING(usb.non_residential_sqft / CAST(CASE WHEN ISNULL(sq.sqft_per_emp,0) < 40 THEN 40 ELSE sq.sqft_per_emp END as FLOAT))
    ,usb.non_residential_rent_per_sqft = 0
FROM
    urbansim.buildings usb
    INNER JOIN urbansim.parcels p ON usb.parcel_id = p.parcel_id
    INNER JOIN urbansim.building_sqft_per_job sq ON usb.development_type_id = sq.development_type_id AND p.luz_id = sq.luz_id
>>>>>>> ea548a1ef74e00dea6b16b8fadca53d50c152d20
*/