IF OBJECT_ID('input.household') IS NOT NULL
  DROP FUNCTION urbansim.edges
GO


-- ==================================================
-- Author:      Daniels, Clint
-- Create date: April 13, 2016
-- Description: Generate the household input file
--              that will be used to match households
--              with buildings
-- ==================================================
CREATE FUNCTION input.household
(
    @scenario_id smallint
)
RETURNS TABLE 
AS
RETURN 
(
  SELECT 
    hh.scenario_id
    ,hh.lu_hh_id
    ,null building_id
    ,geo.zone as mgra
    ,null as tenure
    ,hh.persons as persons
    ,hh.workers as workers
    ,householder.age age_of_head
    ,hh.hh_income as income
    ,ISNULL(children.children,0) children
    ,householder.race_id
    ,hh.autos as cars
  FROM 
    abm_13_2_3.abm.lu_hh hh
    INNER JOIN abm_13_2_3.ref.geography_zone geo ON hh.geography_zone_id = geo.geography_zone_id
    INNER JOIN abm_13_2_3.abm.lu_person householder ON hh.lu_hh_id = householder.lu_hh_id AND householder.pnum = 1 AND householder.scenario_id = @scenario_id
    LEFT JOIN (SELECT lu_hh_id, count(*) children FROM abm_13_2_3.abm.lu_person WHERE age <= 17 and scenario_id = @scenario_id group by lu_hh_id) children ON hh.lu_hh_id = children.lu_hh_id
  WHERE
    hh.scenario_id = @scenario_id
)
GO
