USE spacecore
IF OBJECT_ID('input.costar_transactions', 'u') IS NOT NULL
	DROP TABLE input.costar_transactions
GO

/* CREATE COSTAR_TRANSACTION TABLE */
CREATE TABLE input.costar_transactions(
	[transaction_id] int IDENTITY(1,1) NOT NULL
	,[property_id] int NOT NULL		--change [PropertyID] to [property_id]
	,[lease_sign_date] nvarchar(max) NULL
	,[start_date] nvarchar(max) NULL
	,[sqft_leased] int NULL
	,[floor_leased] nvarchar(max) NULL
	,[suite] nvarchar(max) NULL
	,[asking_rent_sf_month] numeric(12,2) NULL
	,[effective_rent_sf_month] numeric(12,2) NULL
	,[use] nvarchar(max) NULL
	,[services] nvarchar(max) NULL
	,[Lease_term] smallint NULL
	,[Lease_term_type] nvarchar(max) NULL
	,[expiration_date] nvarchar(max) NULL
	,[tenant] nvarchar(max) NULL
	,[deal_type] nvarchar(max) NULL
	,[move_in_date] nvarchar(max) NULL
	,[months_on_market] smallint NULL
	,[lease_comp_id] bigint NULL
	,[lease_type] nvarchar(max) NULL
	CONSTRAINT pk_input_costar_transactions_transcaction_id PRIMARY KEY (transaction_id)
)
GO

