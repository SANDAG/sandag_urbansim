USE spacecore
IF OBJECT_ID('input.costar_transactions_staging', 'u') IS NOT NULL
	DROP TABLE input.costar_transactions_staging
GO

/* CREATE COSTAR_TRANSACTIONS_STAGING TABLE */
CREATE TABLE input.costar_transactions_staging(
	[PropertyID] nvarchar(max) NULL
	,[lease sign date] nvarchar(max) NULL
	,[start date] nvarchar(max) NULL
	,[sqft leased] nvarchar(max) NULL
	,[floor leased] nvarchar(max) NULL
	,[suite] nvarchar(max) NULL
	,[asking rent/sf/month] nvarchar(max) NULL
	,[effective rent/sf/month] nvarchar(max) NULL
	,[use] nvarchar(max) NULL
	,[services] nvarchar(max) NULL
	,[Lease term] nvarchar(max) NULL
	,[Lease term type] nvarchar(max) NULL
	,[expiration date] nvarchar(max) NULL
	,[tenant] nvarchar(max) NULL
	,[deal type] nvarchar(max) NULL
	,[move in date] nvarchar(max) NULL
	,[months on market] nvarchar(max) NULL
	,[lease comp id] nvarchar(max) NULL
	,[lease type] nvarchar(max) NULL
)
GO

/* LOAD COSTAR_TRANSACTIONS DATA FROM .TXT FILE */
BULK INSERT input.costar_transactions_staging 
FROM '\\nasb8\transdata\socioec\urbansim\data\price\costar2016LeaseExport.txt'
	WITH (
	FIRSTROW = 2
	,FIELDTERMINATOR = '\t'
	,ROWTERMINATOR = '\n'
	)
;
