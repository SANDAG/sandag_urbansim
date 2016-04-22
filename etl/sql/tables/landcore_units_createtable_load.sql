USE spacecore
IF OBJECT_ID('input.landcore_units') IS NOT NULL
	DROP TABLE input.landcore_units
GO
CREATE TABLE input.landcore_units(
	id int IDENTITY(1,1) NOT NULL
	,parcel_id int
	,du smallint
)
INSERT INTO input.landcore_units WITH (TABLOCK)(
	parcel_id
	,du
)
SELECT l.parcelID
	,l.du 
	FROM core.landcore l 
		INNER JOIN dbo.numbers_test n
		ON l.du >= n.number
	ORDER BY parcelID
