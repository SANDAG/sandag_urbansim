USE spacecore
IF OBJECT_ID('urbansim.zoning') IS NOT NULL
    DROP TABLE urbansim.zoning
GO

CREATE TABLE urbansim.zoning (
	zoning_id varchar(35) NOT NULL
	,jurisdiction_id int
	,zone_code nvarchar(22)
	,region_id int
	,min_far float
	,max_far float
	,min_front_setback float
	,max_front_setback float
	,rear_setback float
	,side_setback float
	,min_dua float
	,max_dua float
	,max_building_height int
	,allowed_uses nvarchar(max)
	,shape geometry
)
INSERT INTO urbansim.zoning WITH (TABLOCK) (zoning_id, jurisdiction_id, zone_code, region_id, shape)
SELECT 
    CONCAT(CONVERT(varchar(20),(jurisdict)),'_',zonecode) zoningid		--NEW ZONINGID
	,jurisdict
	,zonecode
	,MIN(regionid) regionid
	,geometry::UnionAggregate(shape) shape
FROM gis.zoning
GROUP BY jurisdict,zonecode


--ZONING RULES
UPDATE
  zoning
SET
	zoning.min_far = zrul.min_far
	,zoning.max_far = zrul.max_far
	,zoning.min_front_setback = zrul.min_front_setback 
	,zoning.max_front_setback = zrul.max_front_setback
	,zoning.rear_setback = zrul.rear_setback
	,zoning.side_setback = zrul.side_setback
	,zoning.min_dua = zrul.min_dua
	,zoning.max_dua = zrul.max_dua
	,zoning.max_building_height = zrul.max_building_height
FROM 
	urbansim.zoning zoning
	LEFT JOIN (SELECT id
	,name
	,min_far
	,max_far
	,min_front_setback
	,max_front_setback
	,min_dua
	,max_dua
	,rear_setback
	,side_setback
	,max_building_height
		FROM input.zoning_rules) zrul	
	ON zoning.zoning_id = zrul.name

--LIST ALLOWED USES
UPDATE 
	zoning
SET
	zoning.allowed_uses = zlu.allowed_uses
FROM
	urbansim.zoning zoning
	 LEFT JOIN(SELECT zoning_rules_code_name
		,(SELECT CONCAT(development_type_id, ',')
			FROM input.zoning_allowed_uses zlu
			WHERE zlu.zoning_rules_code_name = zcode.zoning_rules_code_name
			ORDER BY zlu.development_type_id
			FOR XML PATH(''), TYPE).value('/','NVARCHAR(MAX)') allowed_uses
		FROM(SELECT DISTINCT zoning_rules_code_name
			FROM input.zoning_allowed_uses) zcode) zlu 
	ON zoning.zoning_id = zlu.zoning_rules_code_name


--CREATE A PRIMARY KEY SO WE CAN CREATE A SPATIAL INDEX
ALTER TABLE urbansim.zoning ADD CONSTRAINT pk_urbansim_zoning_zoning_id PRIMARY KEY CLUSTERED (zoning_id) 
--SET THE SHAPES TO BE NOT NULL SO WE CAN CREATE A SPATIAL INDEX
ALTER TABLE urbansim.zoning ALTER COLUMN shape geometry NOT NULL
--SELECT max(x_coord), min(x_coord), max(y_coord), min(y_coord) from gis.parcels
CREATE SPATIAL INDEX [ix_spatial_urbansim_zoning_shape] ON urbansim.zoning
(
    shape
) USING  GEOMETRY_GRID
    WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
    CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

    
IF OBJECT_ID('urbansim.zoning_allowed_use') IS NOT NULL
  DROP TABLE urbansim.zoning_allowed_use
GO

CREATE TABLE urbansim.zoning_allowed_use (
  zoning_allowed_use_id int IDENTITY(1,1) NOT NULL PRIMARY KEY 
  ,zoning_id varchar(35) NOT NULL
  ,development_type_id int NOT NULL
)
GO

WITH tmp(zoning_id, allowed_use_id, data) as (
select zoning_id, LEFT(allowed_uses, CHARINDEX(',',allowed_uses+',')-1),
    STUFF(allowed_uses, 1, CHARINDEX(',',allowed_uses+','), '')
from urbansim.zoning WHERE allowed_uses is not null
union all
select zoning_id, LEFT(Data, CHARINDEX(',',Data+',')-1),
    STUFF(Data, 1, CHARINDEX(',',Data+','), '')
from tmp
where Data > ''
)

INSERT INTO urbansim.zoning_allowed_use (zoning_id, development_type_id)
select zoning_id, allowed_use_id
from tmp
order by zoning_id, allowed_use_id
