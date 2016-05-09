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
	,[sqft leased]
	,[floor leased]
	,[suite]
	,[asking rent/sf/month]
	,[effective rent/sf/month]
	,[use]
	,[services]
	,[Lease term]
	,[Lease term type]
	,[expiration date]
	,[tenant]
	,[deal type]
	,[move in date]
	,[months on market]
	,[lease comp id]
	,[lease type]
FROM OPENROWSET('Microsoft.Jet.OLEDB.4.0',
                'Excel 12.0;Database=T:\socioec\urbansim\data\price\costar2016LeaseExport.xlsx',
                'SELECT * FROM [Sheet1$]')
