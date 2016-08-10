USE spacecore
IF OBJECT_ID('input.zoning_allowed_uses', 'u') IS NOT NULL
	DROP TABLE input.zoning_allowed_uses
GO
CREATE TABLE input.zoning_allowed_uses(
	zoning_id int NULL
	,zoning_rules_code_name nvarchar(35) NULL
	,development_type_id int NULL
CONSTRAINT pk_zoning_allowed_uses UNIQUE NONCLUSTERED
	(
	zoning_id, development_type_id
	)
)
GO

INSERT INTO input.zoning_allowed_uses WITH(TABLOCK) (
	zoning_id
	,zoning_rules_code_name
	,development_type_id
	)
SELECT DISTINCT zp.id
	,z.zoning_rules_code_name
	,x.development_type_id
FROM pecas_sd_run.s21.zoning_rules_i z
	INNER JOIN pecas_sd_run.s21.zoning_permissions p
		ON p.zoning_rules_code = z.zoning_rules_code
			INNER JOIN pecas_sr13.urbansim.space_type_development_type x
			ON x.space_type_id = p.space_type_id
				INNER JOIN ws.dbo.zoning zp
				ON zp.name = z.zoning_rules_code_name
WHERE p.acknowledged_use = 0
ORDER BY z.zoning_rules_code_name
