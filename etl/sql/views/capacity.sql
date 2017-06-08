--PULLS FROM CAPACITYFEEDBACK ON sql2014b8
SELECT parcel_id
	,jurisdicti
	,Cap_Type
	,Feed_Type
	,New_Cap
	,CASE
		WHEN Feed_Type = 1 AND Cap_Type = 1 THEN 'ZONING'	--FROM JOIN
		WHEN Feed_Type = 1 AND Cap_Type = 2 THEN 'Sr13'		--sr13cap.[sr13_cap_hs_growth_adjusted]		--FROM JOIN
		WHEN Feed_Type = 1 AND Cap_Type = 3 THEN CAST(New_Cap AS varchar)
		WHEN Feed_Type = 1 AND Cap_Type = 4 THEN CAST(New_Cap AS varchar)
		WHEN Feed_Type = 2 THEN 'DELETE'
		WHEN Feed_Type = 3 AND Cap_Type = 1 THEN CAST(New_Cap AS varchar)
		WHEN Feed_Type = 3 AND Cap_Type = 2 THEN CAST(New_Cap AS varchar)
	END AS sr14_cap
	,Notes
	,CASE
		WHEN Cap_Type = 0 THEN 'UNKNOWN'
		WHEN Cap_Type = 1 AND Feed_Type = 3 THEN 'ZONING_OVERRIDE'
		WHEN Cap_Type = 1 THEN 'ZONING'
		WHEN Cap_Type = 2 AND Feed_Type = 3 THEN 'Sr13 OVERRIDE'
		WHEN Cap_Type = 2 THEN 'Sr13'
		WHEN Cap_Type = 3 THEN 'NEW_CAPACITY'
		WHEN Cap_Type = 4 THEN 'SCHEDULED_DEV'
	END AS cap_source
FROM [WS].[dbo].[CAPACITYFEEDBACK]
--JOIN (SELECT * FROM  OPENQUERY(sql2014b8, 'SELECT * FROM [spacecore].[ref].[sr13_capacity]')) AS sr13cap
--ON [CAPACITYFEEDBACK].parcel_id = sr13cap.ludu2015_parcel_id
WHERE SR14 = 1
AND [jurisdicti] NOT IN(14, 19)

