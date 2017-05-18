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
--/*
--PARCELS TEST
SET @source = 'spacecore.urbansim.parcels_test';	--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.urbansim.parcels_test';		--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'parcels_test';						--URBANSIM TABLE
SET @pkey = 'parcel_id';							--URBANSIM PRIMARY KEY
--*/
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
--BUILDING_SQFT_PER_JOB
SET @source = 'spacecore.urbansim.building_sqft_per_job';	--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.urbansim.building_sqft_per_job';	--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'building_sqft_per_job';						--URBANSIM TABLE
SET @pkey = 'parcel_id';									--URBANSIM PRIMARY KEY
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
SET @source = 'spacecore.urbansim.zoning';			--SPACECORE DATABASE SCHEMA TABLE
SET @target = 'urbansim.urbansim.zoning';			--URBANSIM DATABASE SCHEMA TABLE
SET @table = 'zoning';								--URBANSIM TABLE
SET @pkey = 'zone';									--URBANSIM PRIMARY KEY-*/

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