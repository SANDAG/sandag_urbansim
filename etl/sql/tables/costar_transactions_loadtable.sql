USE spacecore
IF OBJECT_ID('input.costar_transactions') IS NOT NULL
	TRUNCATE TABLE input.costar_transactions
GO

/* LOAD COSTAR_TRANSACTION DATA FROM EXCEL WORKBOOK */
INSERT INTO input.costar_transactions WITH (TABLOCK) (
	[property_id]
	,[lease_sign_date]
	,[start_date]
	,[sqft_leased]
	,[floor_leased]
	,[suite]
	,[asking_rent_sf_month]
	,[effective_rent_sf_month]
	,[use]
	,[services]
	,[Lease_term]
	,[Lease_term_type]
	,[expiration_date]
	,[tenant]
	,[deal_type]
	,[move_in_date]
	,[months_on_market]
	,[lease_comp_id]
	,[lease_type]
	)
SELECT
	[PropertyID]
	,[lease sign date]
	,[start date]
	,CASE WHEN [sqft leased] = '-' THEN NULL
			WHEN ISNUMERIC(REPLACE(REPLACE([sqft leased], '"', ''), ',', '')) = 1 
				THEN CAST(REPLACE(REPLACE([sqft leased], '"', ''), ',', '')AS int)
			ELSE NULL
	END AS [sqft leased]
	,RTRIM(LTRIM(REPLACE([floor leased], '"', '')))
	,[suite]
	,CASE WHEN ISNUMERIC(REPLACE([asking rent/sf/month], '$', '')) = 1 
				THEN CAST(RTRIM(LTRIM(REPLACE([asking rent/sf/month], '$', ''))) AS numeric(10,2))
			ELSE NULL
	END AS [asking rent/sf/month]
	,CASE WHEN ISNUMERIC(REPLACE([effective rent/sf/month], '$', '')) = 1 
				THEN CAST(RTRIM(LTRIM(REPLACE([effective rent/sf/month], '$', ''))) AS numeric(10,2))
			ELSE NULL
	END AS [effective rent/sf/month]
	,[use]
	,[services]
	,CAST([Lease term] AS smallint)
	,[Lease term type]
	,[expiration date]
	,[tenant]
	,[deal type]
	,[move in date]
	,CAST([months on market] AS smallint)
	,CAST([lease comp id] AS bigint)
	,[lease type]
FROM input.costar_transactions_staging