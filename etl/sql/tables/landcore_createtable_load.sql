USE spacecore
IF OBJECT_ID('gis.landcore', 'u') IS NOT NULL
	DROP TABLE gis.landcore
GO
CREATE TABLE gis.landcore(
	OBJECTID int IDENTITY(1,1) NOT NULL,
	Shape geometry NOT NULL,
	subParcel int NOT NULL,
	lu int NOT NULL,
	plu int NOT NULL,
	planID numeric(38, 8) NULL,
	siteID smallint NULL,
	genOwnID int NOT NULL,
	apn nvarchar(8) NULL,
	parcelID int NOT NULL,
	editFlag nvarchar(50) NULL,
	du smallint NOT NULL,
	MGRA smallint NOT NULL,
	regionID smallint NOT NULL,
	area numeric(38, 8) NOT NULL,
	CONSTRAINT OBJECTID PRIMARY KEY (OBJECTID)
)
GO

INSERT INTO gis.landcore WITH(TABLOCK) (
	Shape
	,subParcel
	,lu
	,plu
    ,planID
    ,siteID
	,genOwnID
    ,apn
	,parcelID
    ,editFlag
	,du
	,mgra
	,regionID
	,area
	)
SELECT 
    shape as Shape
	,LCKey as subParcel
	,lu
	,plu
    ,null as planID
    ,null as siteID
	,own
    ,apn8 as apn
	,parcelid
    ,null as editFlag
	,du
	,mgra
	,1 as regionID
	,shape.STArea() as area
	FROM  OPENQUERY([sql2014b8], 'SELECT * FROM lis.gis.LUDU2015') WHERE parcelid > 0

CREATE SPATIAL INDEX [ix_spatial_gis_landcore_parcelid] ON [gis].[landcore]
(
    [shape]
) USING  GEOMETRY_GRID
    WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
    CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
