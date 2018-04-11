/*######################################## CREATE DATABASE ########################################*/
/*
CREATE DATABASE urbansim;
GO
ALTER DATABASE urbansim MODIFY FILE 
( NAME = N'urbansim', SIZE = 3048KB , MAXSIZE = 104857600KB, FILEGROWTH = 524288KB )
GO
ALTER DATABASE urbansim MODIFY FILE 
( NAME = N'urbansim_log', SIZE = 1024KB , MAXSIZE = 10485760KB , FILEGROWTH = 262144KB)
GO
*/

/*######################################## CREATE SCHEMAS ########################################*/
/*
USE urbansim;
GO
CREATE SCHEMA urbansim;
GO
CREATE SCHEMA ref;
GO
*/

/*######################################## LOAD TABLES ########################################*/
USE urbansim;
GO

DECLARE @source nvarchar(50)
DECLARE @target nvarchar(50)
DECLARE @table nvarchar(35)
DECLARE @pkey nvarchar(35)

/*#################### SET TABLES, RUN ONE AT A TIME ####################*/
/*
--PARCELS TEST
SET @source = 'spacecore.urbansim.parcels_test';	--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.urbansim.parcels_test';		--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'parcels_test';						--URBANSIM TABLE
SET @pkey = 'parcel_id';							--URBANSIM PRIMARY KEY
*/
/*
--PARCEL
SET @source = 'spacecore.urbansim.parcels';			--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.urbansim.parcel';			--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'parcel';								--URBANSIM TABLE
SET @pkey = 'parcel_id';							--URBANSIM PRIMARY KEY
*/
/*
--BUILDING
SET @source = 'spacecore.urbansim.buildings';		--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.urbansim.building';			--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'building';							--URBANSIM TABLE
SET @pkey = 'building_id';							--URBANSIM PRIMARY KEY
*/
/*
--HOUSEHOLD
SET @source = 'spacecore.urbansim.households';		--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.urbansim.household';		--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'household';							--URBANSIM TABLE
SET @pkey = 'household_id';							--URBANSIM PRIMARY KEY
*/
/*
--JOB
SET @source = 'spacecore.urbansim.jobs';			--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.urbansim.job';				--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'job';									--URBANSIM TABLE
SET @pkey = 'job_id';								--URBANSIM PRIMARY KEY
*/
/*
--JOB_SPACE
SET @source = 'spacecore.urbansim.job_spaces';			--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.urbansim.job_space';				--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'job_space';									--URBANSIM TABLE
SET @pkey = 'job_space_id';								--URBANSIM PRIMARY KEY
*/
/*
--BUILDING_SQFT_PER_JOB																							--???
SET @source = 'spacecore.urbansim.building_sqft_per_job';		--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.urbansim.building_sqft_per_job';		--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'building_sqft_per_job';							--URBANSIM TABLE
SET @pkey = 'parcel_id';										--URBANSIM PRIMARY KEY
*/
/*
--NETWORK NODE
SET @source = 'spacecore.urbansim.nodes';			--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.urbansim.node';				--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'node';								--URBANSIM TABLE
SET @pkey = 'node';									--URBANSIM PRIMARY KEY
*/
/*
--NETWORK EDGE
SET @source = 'spacecore.urbansim.edges';			--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.urbansim.edge';				--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'edge';								--URBANSIM TABLE
SET @pkey = 'edge';									--URBANSIM PRIMARY KEY
*/
/*
--ZONING
SET @source = 'spacecore.urbansim.parcel_zoning_schedule2';		--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.urbansim.parcel_zoning_schedule2';		--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'parcel_zoning_schedule2';							--URBANSIM TABLE
SET @pkey = 'pzs2_id';											--URBANSIM PRIMARY KEY
*/
/*
--ZONING
SET @source = 'spacecore.urbansim.zoning_allowed_use';			--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.urbansim.zoning_allowed_use';			--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'zoning_allowed_use';								--URBANSIM TABLE
SET @pkey = 'zoning_allowed_use_id';							--URBANSIM PRIMARY KEY
*/
/*
--ZONING
SET @source = 'spacecore.urbansim.zoning';			--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.urbansim.zoning';			--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'zoning';								--URBANSIM TABLE
SET @pkey = 'zoning_id';							--URBANSIM PRIMARY KEY
*/
/*
--ZONING
SET @source = 'spacecore.urbansim.zoning_schedule';				--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.urbansim.zoning_schedule';				--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'zoning_schedule';									--URBANSIM TABLE
SET @pkey = 'zoning_schedule_id';								--URBANSIM PRIMARY KEY
*/
/*
--ZONING PARCEL
SET @source = 'spacecore.urbansim.zoning_parcels';				--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.urbansim.zoning_parcel';				--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'zoning_parcel';									--URBANSIM TABLE
SET @pkey = 'zoning_parcels_id';								--URBANSIM PRIMARY KEY
*/
/*
--SCHEDULED DEVELOPMENT SITES
SET @source = 'spacecore.gis.scheduled_development_sites';			--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.ref.scheduled_development_site';		--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'scheduled_development_site';							--URBANSIM TABLE
SET @pkey = 'ogr_fid';											--URBANSIM PRIMARY KEY
*/
/*
--SCHEDULED DEVELOPMENT PARCELS
SET @source = 'spacecore.urbansim.scheduled_development_parcels';	--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.urbansim.scheduled_development_parcel';		--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'scheduled_development_parcel';						--URBANSIM TABLE
SET @pkey = 'parcel_id';											--URBANSIM PRIMARY KEY
*/
/*
--REF DEVELOPMENT TYPE
SET @source = 'spacecore.ref.development_type';		--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.ref.development_type';		--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'development_type';					--URBANSIM TABLE
SET @pkey = 'development_type_id';					--URBANSIM PRIMARY KEY
*/
/*
--REF DEVELOPMENT TYPE LU CODE
SET @source = 'spacecore.ref.development_type_lu_code';		--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.ref.development_type_lu_code';		--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'development_type_lu_code';					--URBANSIM TABLE
SET @pkey = 'lu_code';					--URBANSIM PRIMARY KEY
*/
/*
--REF DEVELOPMENT TYPE LU CODE
SET @source = 'spacecore.ref.development_type_lu_code';		--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.ref.development_type_lu_code';		--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'development_type_lu_code';					--URBANSIM TABLE
SET @pkey = 'lu_code';					--URBANSIM PRIMARY KEY
*/
/*
--REF JURISDICTION
SET @source = 'spacecore.ref.jurisdiction';		--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.ref.jurisdiction';		--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'jurisdiction';					--URBANSIM TABLE
SET @pkey = 'jurisdiction_id';					--URBANSIM PRIMARY KEY
*/
/*
--REF LU CODE
SET @source = 'spacecore.ref.lu_code';		--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.ref.lu_code';		--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'lu_code';					--URBANSIM TABLE
SET @pkey = 'lu_code';					--URBANSIM PRIMARY KEY
*/
/*
--REF SR13 CAPACITY
SET @source = 'spacecore.ref.sr13_capacity';		--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.ref.sr13_capacity';		--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'sr13_capacity';					--URBANSIM TABLE
SET @pkey = 'sr13_capacity_id';					--URBANSIM PRIMARY KEY
*/
/*
--REF SR14 CAPACITY
SET @source = 'spacecore.ref.sr14_capacity';		--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.ref.sr14_capacity';		--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'sr14_capacity';					--URBANSIM TABLE
SET @pkey = 'capacity_id';					--URBANSIM PRIMARY KEY
*/
/*
--CAPACITY
SET @source = 'spacecore.urbansim.capacity';			--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.urbansim.capacity';				--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'capacity';								--URBANSIM TABLE
SET @pkey = 'capacity_id';								--URBANSIM PRIMARY KEY
*/
/*
--GENERAL PLAN PARCELS
SET @source = 'spacecore.urbansim.general_plan_parcels';	--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.urbansim.general_plan_parcel';		--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'general_plan_parcel';							--URBANSIM TABLE
SET @pkey = 'parcel_id';									--URBANSIM PRIMARY KEY
*/

--EXEC('SELECT * FROM '+ @source)

/*#################### BEGIN LOAD ####################*/
--DROP CURRENT TABLE
IF OBJECT_ID(@target) IS NOT NULL
BEGIN
    EXEC(
	'DROP TABLE ' + @target
	)
END

--LOAD NEW TABLE
EXEC(
	'SELECT * INTO ' + @target + ' FROM ' + @source
)
/*
--SET ID COLUMN TO NOT NULL SO WE CAN CREATE  PRIMARY KEY
EXEC(
	'ALTER TABLE ' + @target + ' ALTER COLUMN ' + @pkey + ' int NOT NULL' 
)
*/
--CREATE PRIMARY KEY SO WE CAN CREATE SPATIAL INDEXES
EXEC(
	'ALTER TABLE ' + @target + ' ADD CONSTRAINT pk_urbansim_' + @table +'_'+ @pkey + ' PRIMARY KEY CLUSTERED (' + @pkey + ')' 
)

--SET SHAPE AND CENTROID COLUMNS TO BE NOT NULL SO WE CAN CREATE A SPATIAL INDEX
IF COL_LENGTH(@target, 'shape') IS NOT NULL
	EXEC(
		'ALTER TABLE ' + @target + ' ALTER COLUMN shape geometry NOT NULL'
	)
IF COL_LENGTH(@target, 'centroid') IS NOT NULL
	EXEC(
		'ALTER TABLE ' + @target + ' ALTER COLUMN centroid geometry NOT NULL'
	)

--CREATE SPATIAL INDEXES
--SHAPE
IF COL_LENGTH(@target, 'shape') IS NOT NULL
	EXEC(
		'CREATE SPATIAL INDEX [ix_spatial_urbansim_' + @table + '_shape] ON ' + @target +
		'(
			[shape]
		) USING  GEOMETRY_GRID
			WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
			CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
		;'
	)
--CENTROID
IF COL_LENGTH(@target, 'centroid') IS NOT NULL
	EXEC(
		'CREATE SPATIAL INDEX [ix_spatial_urbansim_' + @table + '_centroid] ON ' + @target +
		'(
			[centroid]
		) USING  GEOMETRY_GRID
			WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
			CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
		;'
	)
