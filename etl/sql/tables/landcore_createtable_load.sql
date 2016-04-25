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
	FROM  OPENQUERY([pila\sdgIntDb], 'SELECT * FROM lis.gis.LUDU2015') WHERE parcelid > 0

CREATE NONCLUSTERED INDEX ix_landcore_parcelid ON gis.landcore (parcelID) INCLUDE (du)