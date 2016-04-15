USE spacecore
IF OBJECT_ID('urbansim.buildings') IS NOT NULL
    DROP TABLE urbansim.buildings
GO
CREATE TABLE urbansim.buildings (
    id  int IDENTITY(1,1)
	,building_id int NOT NULL
	,development_type_id int
	,parcel_id int
	,improvement_value float
	,residential_units int
	,residential_sqft int
	,non_residential_sqft int
    ,job_spaces smallint
    ,non_residential_rent_per_sqft float
	,price_per_sqft float
	,stories int
	,year_built smallint
	,shape geometry
)
INSERT INTO urbansim.buildings WITH (TABLOCK) (building_id, shape)
SELECT 
    bldgID
	,shape
FROM gis.buildings

--CREATE A PRIMARY KEY SO WE CAN CREATE A SPATIAL INDEX
ALTER TABLE urbansim.buildings ADD CONSTRAINT pk_urbansim_buildings_id PRIMARY KEY CLUSTERED (id) 

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
	,core.LANDCORE lc
JOIN ref.development_type_lu_code dev 
ON lc.lu = dev.lu_code
WHERE usb.Shape.STCentroid().STWithin(lc.Shape) = 1


/** PARCEL DATA **/
--GET PARCEL DATA: RESIDENTIAL UNITS, IMPROVEMENT VALUE,
UPDATE
	usb
SET
	usb.improvement_value = (i.impr/b.bldgs)
	--,usb.residential_units = (u.unitqty/b.bldgs)
FROM
	urbansim.buildings usb
	LEFT JOIN 
		(SELECT parcel_id,  COUNT(parcel_id) bldgs	--NUMBER OF BUILDINGS
		FROM urbansim.buildings
		--WHERE parcel_id = 219874		--TEST
		GROUP BY parcel_id) b
	 ON usb.parcel_id = b.parcel_id
	LEFT JOIN 
		(SELECT parcelid, SUM(asr_impr) impr		--CHECK FIELD NAME
		FROM GIS.parcels
		--WHERE PARCELID = 722555		--TEST
		GROUP BY parcelid) i
	ON usb.parcel_id = i.parcelid 

/** ASSESSOR DATA **/
--GET ASSESSOR DATA: RESIDENTIAL UNITS, NON-RES SQFT, PRICE/SQFT, YEAR BUILT
UPDATE
	usb
SET
	usb.residential_units = (ROUND((CAST (a.units AS float) / CAST (b.bldgs AS int)),0))
	,usb.improvement_value = (a.imps/b.bldgs)
FROM
	urbansim.buildings usb
	LEFT JOIN 
		(SELECT parcel_id,  COUNT(parcel_id) bldgs						--NUMBER OF BUILDINGS
		FROM urbansim.buildings
		GROUP BY parcel_id) b
	 ON usb.parcel_id = b.parcel_id
	LEFT JOIN
		(SELECT l.parcelID
			, par.[CURRENT_IMPS] imps
			,l.units													--CHECK FIELD NAME
		FROM spacecore.input.assessor_par par
		JOIN
			(SELECT parcelID
				,MIN(apn) apn											--GRAB LOWEST APN
				,SUM(du) units
			FROM spacecore.core.LANDCORE
			GROUP BY parcelID) l
		ON l.apn = LEFT(par.apn,8)										--ONE TO MANY, SELECT MIN APN
		) a
	ON usb.parcel_id = a.parcelID

/** COSTAR DATA **/
--GET COSTAR DATA: RESIDENTIAL UNITS, NON-RES SQFT, PRICE/SQFT, YEAR BUILT
UPDATE
	usb
SET
	--usb.residential_units = (c.number_of_units/b.bldgs)
	usb.non_residential_sqft = (c.total_available_space/b.bldgs)
	,usb.price_per_sqft = c.avg_asking_sf
	,usb.year_built = c.year_built										--CHECK FIELD TYPE
FROM
	urbansim.buildings usb
	LEFT JOIN 
		(SELECT parcel_id,  COUNT(parcel_id) bldgs						--NUMBER OF BUILDINGS
		FROM urbansim.buildings
		GROUP BY parcel_id) b
	 ON usb.parcel_id = b.parcel_id
	LEFT JOIN
		(SELECT parcel_id
			--, SUM(number_of_units) number_of_units					--CHECK NULL VALUES
			, SUM([total_available_space_(sf)]) total_available_space	--CHECK FIELD NAME
			, MAX([avg_asking/sf]) avg_asking_sf						--CHECK FIELD NAME
			, MIN(year_built) year_built								--CHECK FIELD NAME
		FROM input.costar
		GROUP BY parcel_id) c
	ON usb.parcel_id = c.parcel_id

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