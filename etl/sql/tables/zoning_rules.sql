USE spacecore
IF OBJECT_ID('input.zoning_rules', 'u') IS NOT NULL
	DROP TABLE input.zoning_rules
GO
CREATE TABLE input.zoning_rules(
	id int NOT NULL
	,name nvarchar(35) NULL
	,min_far numeric(38, 8) NULL
	,max_far numeric(38, 8) NULL
	,min_front_setback numeric(38, 8) NULL
	,max_front_setback numeric(38, 8) NULL
	,min_dua numeric(38, 8) NULL
	,max_dua numeric(38, 8) NULL
	,rear_setback numeric(38, 8) NULL
	,side_setback numeric(38, 8) NULL
	,max_building_height int NULL
	--CONSTRAINT OBJECTID PRIMARY KEY (id)
)
GO

INSERT INTO input.zoning_rules WITH(TABLOCK) (
	id
	,name
	,min_far
	,max_far
	,min_front_setback
	,max_front_setback
	,min_dua
	,max_dua
	,rear_setback
	,side_setback
	--,max_building_height
	)
SELECT 	
	id
	,name
	,min_far
	,max_far
	,min_front_setback
	,max_front_setback
	,min_dua
	,max_dua
	,rear_setback
	,side_setback
	--,max_building_height
FROM pecas_sr13.urbansim.zoning
