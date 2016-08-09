USE spacecore
IF OBJECT_ID('urbansim.scheduled_development') IS NOT NULL
	DROP TABLE urbansim.scheduled_development
GO
CREATE TABLE urbansim.scheduled_development(
	OBJECTID int NOT NULL
	,parcel_ID int
	,development_type_id int
	,siteID int NOT NULL
	,siteName text
	,totalSqft int
	,empDen	float
	,civEmp int
	,milEmp int
	,sfu int
	,mfu int
	,mhu int
	,civGq int
	,milGq int
	,source	Text
	,infoDate date
	,spaceSqFt int
	,startDate date
	,compDate date
	,resSqft int
	,nResSqft int
	,created_user text
	,created_date date
	,last_edited_user text
	,last_edited_date date
	,devTypeID int
	,SHAPE_Length float
	,SHAPE_Area	float
	,SHAPE Geometry
)
INSERT INTO urbansim.scheduled_development WITH (TABLOCK) (
	OBJECTID
	,siteID
	--,parcel_ID				--FROM SPATIAL JOIN
	--,development_type_id		--FROM SPATIAL JOIN
	,siteName
	,totalSqft
	,empDen
	,civEmp
	,milEmp
	,sfu
	,mfu
	,mhu
	,civGq
	,milGq
	,source
	,infoDate
	,spaceSqFt
	,startDate
	,compDate
	,resSqft
	,nResSqft
	,created_user
	,created_date
	,last_edited_user
	,last_edited_date
	,devTypeID
	,SHAPE_Length
	,SHAPE_Area
	,SHAPE
)
SELECT
	ogr_fid
	,siteid
	,sitename
	,totalsqft
	,empden
	,civemp
	,milemp
	,sfu
	,mfu
	,mhu
	,civgq
	,milgq
	,source
	,infodate
	,spacesqft
	,startdate
	,compdate
	,ressqft
	,nressqft
	,created_us
	,created_da
	,last_edite
	,last_edi_1
	,devtypeid
	,shape_leng
	,shape_area
	,ogr_geometry
FROM
	input.scheduled_development

--CREATE A PRIMARY KEY SO WE CAN CREATE A SPATIAL INDEX
ALTER TABLE urbansim.scheduled_development ALTER COLUMN siteID int NOT NULL
ALTER TABLE urbansim.scheduled_development ADD CONSTRAINT pk_input_scheddev_siteid PRIMARY KEY CLUSTERED (siteid) 

--SET THE SHAPES TO BE NOT NULL SO WE CAN CREATE A SPATIAL INDEX
ALTER TABLE urbansim.scheduled_development ALTER COLUMN shape geometry NOT NULL

--SELECT max(x_coord), min(x_coord), max(y_coord), min(y_coord) from gis.parcels
CREATE SPATIAL INDEX [ix_spatial_urbansim_scheddev] ON urbansim.scheduled_development
(
    shape
) USING  GEOMETRY_GRID
    WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
    CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

/** LANDCORE DATA **/
--GET LANDCORE DATA: PARCEL_ID, DEV_TYPE
UPDATE
	usd
SET
	usd.parcel_ID = lc.parcelID
	,usd.development_type_id = dev.development_type_id
FROM
	urbansim.scheduled_development usd
	,gis.landcore lc
JOIN ref.development_type_lu_code dev 
ON lc.lu = dev.lu_code
WHERE usd.Shape.STCentroid().STWithin(lc.Shape) = 1
