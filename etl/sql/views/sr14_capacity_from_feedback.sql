--RUNS ON sql2014a8
--PULLS FROM [CAPACITYFEEDBACK] ON sql2014b8
USE spacecore
 
IF OBJECT_ID('ref.sr14_capacity_from_feedback') IS NOT NULL
    DROP TABLE ref.sr14_capacity_from_feedback
GO
 
SELECT
    IDENTITY(int, 1, 1) AS capacity_id
    ,cap.parcel_id
    ,jurisdicti
    ,Cap_Type
    ,Feed_Type
    ,sr13cap.[sr13_cap_hs_growth_adjusted]
    ,zoning.[addl_units] AS zoning
    ,New_Cap
    ,CAST(
        CASE
            WHEN Feed_Type = 1 AND Cap_Type = 1 THEN zoning.[addl_units]                        --FROM JOIN
            WHEN Feed_Type = 1 AND Cap_Type = 2 THEN sr13cap.[sr13_cap_hs_growth_adjusted]      --FROM JOIN
            --WHEN Feed_Type = 1
            WHEN Feed_Type = 2 THEN NULL
            WHEN Feed_Type = 3 AND Cap_Type = 1 THEN New_Cap
            WHEN Feed_Type = 3 AND Cap_Type = 2 THEN New_Cap
        END
    AS INT) AS sr14_cap
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
    ,NULLIF(cap.siteid, 0) AS site_id
INTO ref.sr14_capacity_from_feedback
FROM (SELECT * FROM OPENQUERY(sql2014b8, 'SELECT * FROM [WS].[dbo].[CAPACITYFEEDBACK]')) AS cap
LEFT JOIN [ref].[sr13_capacity] AS sr13cap
    ON cap.parcel_id = sr13cap.ludu2015_parcel_id
LEFT JOIN [staging].[additional_units_schedule1] AS zoning
    ON cap.parcel_id = zoning.parcel_id
WHERE SR14 = 1
AND [jurisdicti] NOT IN(14, 19)
ORDER BY cap.parcel_id
