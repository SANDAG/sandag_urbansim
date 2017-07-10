/*#################### ASSIGN JOB_SPACES FROM EDD EMP ####################*/
DECLARE @employment_vacancy float = 0.1;
/* ##### ASSIGN JOB_SPACES TO SINGLE BUILDING SUBPARCELS ##### */
WITH bldg_single AS (
	SELECT
		lc.subParcel AS subparcel_id
		--,SUM(CAST(CEILING(emp_adj*(1+@employment_vacancy))AS int)) emp
		--,SUM(CAST(CEILING(emp_adj)AS int)) emp
		,SUM(emp_adj) emp
		,naics
	FROM gis.ludu2015 lc
	INNER JOIN socioec_data.ca_edd.emp_2013 emp 
	ON lc.Shape.STContains(emp.shape) = 1
	INNER JOIN (SELECT subparcel_id
				FROM urbansim.buildings 
				--WHERE assign_jobs = 1		--DO NOT USE MIL/PF BUILDINGS
				GROUP BY subparcel_id 
				HAVING COUNT(*) = 1) single_bldg	
	ON lc.subParcel = single_bldg.subparcel_id
	WHERE emp.emp_adj IS NOT NULL
	GROUP BY lc.subParcel, naics
	--ORDER BY lc.subParcel, naics
)
SELECT
	usb.building_id
	,bldg.subparcel_id
	,usb.block_id
	,bldg.emp AS job_spaces
	,bldg.naics AS edd_naics
	,usb.development_type_id AS bldg_dev_type_id
FROM urbansim.buildings AS usb
JOIN bldg_single AS bldg
	ON usb.subparcel_id = bldg.subparcel_id
ORDER BY subparcel_id, building_id
;
********************************************************************************************
/* ##### ASSIGN JOB_SPACES TO MULTIPLE BUILDING SUBPARCELS ##### */
--DECLARE @employment_vacancy float = 0.1;

--WHERE subparcel_id = 12273

WITH emp AS(
	SELECT
		emp_lc.subparcel_id
		,naics
		,ROW_NUMBER() OVER (PARTITION BY emp_lc.subparcel_id ORDER BY naics_case, emp DESC) AS row_num
		,emp
	FROM (SELECT
				subParcel AS subparcel_id
				,LEFT(naics, 2) AS naics
				,CASE LEFT(naics, 2)
					WHEN 62 THEN 1		--Health Care and Social Assistance
					WHEN 72 THEN 2		--Accommodation and Food Services
					WHEN 61 THEN 3		--Educational Services					
					WHEN 54 THEN 4		--Professional, Scientific, and Technical Services
					ELSE 5
				END AS naics_case
				,SUM(emp_adj) AS emp
			FROM socioec_data.ca_edd.emp_2013 AS emp
			JOIN gis.ludu2015 lc
				ON lc.Shape.STContains(emp.shape) = 1
			WHERE emp_adj IS NOT NULL
			GROUP BY subParcel
				,LEFT(naics, 2)
			) AS emp_lc 
	JOIN (SELECT subparcel_id
			FROM urbansim.buildings 
			GROUP BY subparcel_id
			HAVING COUNT(*) > 1) bldg_m
		ON emp_lc.subparcel_id = bldg_m.subparcel_id
	
	WHERE emp_lc.subparcel_id = 12273				--<<TEST
	ORDER BY subparcel_id, row_num
)
bldg AS(
	SELECT
		block_id
		,usb.subparcel_id
		,building_id
		,development_type_id AS bldg_dev_type_id
		,shape.STArea()
		,non_residential_sqft
	FROM urbansim.buildings AS usb
	--GROUP BY block_id, subparcel_id, building_id
	JOIN (SELECT subparcel_id
			CASE development_type_id
				WHEN 
			FROM urbansim.buildings 
			GROUP BY subparcel_id
			HAVING COUNT(*) > 1) bldg_m
	ON usb.subparcel_id = bldg_m.subparcel_id
	WHERE usb.subparcel_id = 12273				--<<TEST
	ORDER BY block_id, usb.subparcel_id, building_id
)
SELECT
	block_id
	,subparcel_id
	,building_id
	,bldg_dev_type_id
FROM bldg
JOIN emp ON bldg.









*****************************************************************************
/* ##### ASSIGN JOB_SPACES TO MULTIPLE BUILDING SUBPARCELS ##### */
--DECLARE @employment_vacancy float = 0.1;
-----MAY WANT TO THINK ABOUT EXCLUDING REALLY SMALL BUILDINGS FROM THIS QUERY
WITH bldg_multi AS(
	SELECT usb.subparcel_id
		,usb.building_id
		,emp/COUNT(*) OVER (PARTITION BY usb.subparcel_id) +
		CASE
			WHEN ROW_NUMBER() OVER (PARTITION BY usb.subparcel_id ORDER BY usb.shape.STArea()) <= (emp % COUNT(*) OVER (PARTITION BY usb.subparcel_id)) THEN 1
			ELSE 0
		END emp
		--,naics
	FROM (SELECT * FROM urbansim.buildings WHERE assign_jobs = 1) AS usb			--DO NOT USE MIL/PF BUILDINGS
	JOIN (SELECT subParcel, naics, SUM(emp_adj) emp
			FROM gis.ludu2015 lc
			--INNER JOIN (SELECT subParcel, SUM(CAST(CEILING(emp_adj*(1+@employment_vacancy))AS int)) emp
			INNER JOIN socioec_data.ca_edd.emp_2013 emp 
				ON lc.Shape.STContains(emp.shape) = 1
			WHERE emp.emp_adj IS NOT NULL
			GROUP BY lc.subParcel, naics) lc
	ON usb.subparcel_id = lc.subParcel
	WHERE usb.subparcel_id IN (SELECT subparcel_id FROM urbansim.buildings GROUP BY subparcel_id HAVING COUNT(*) > 1)
)
SELECT
	bldgs.building_id
	,bldgs.subparcel_id
	,usb.block_id
	,bldgs.emp AS job_spaces
	,bldgs.naics AS edd_naics
	,usb.development_type_id AS bldg_dev_type_id

FROM 
	urbansim.buildings  usb		
	INNER JOIN bldg_multi AS bldgs ON usb.building_id = bldgs.building_id
	
**************************************************************************************************************************	
	WITH bldgs AS (
	SELECT
		usb.subparcel_id
		,usb.building_id
		,lc.emp/ COUNT(*) OVER (PARTITION BY usb.subparcel_id) +
		CASE
			WHEN ROW_NUMBER() OVER (PARTITION BY usb.subparcel_id ORDER BY usb.shape.STArea()) <= (lc.emp % COUNT(*) OVER (PARTITION BY usb.subparcel_id)) THEN 1 
			ELSE 0 
		END jobs
	FROM (SELECT * FROM urbansim.buildings WHERE assign_jobs = 1) usb		--DO NOT USE MIL/PF BUILDINGS
	--INNER JOIN (SELECT subParcel, SUM(CAST(CEILING(emp_adj*(1+@employment_vacancy))AS int)) emp
	INNER JOIN (SELECT subParcel, SUM(emp_adj) emp
				FROM gis.ludu2015 lc
				INNER JOIN socioec_data.ca_edd.emp_2013 emp 
					ON lc.Shape.STContains(emp.shape) = 1
				WHERE emp.emp_adj IS NOT NULL
				GROUP BY lc.subParcel) lc
	ON usb.subparcel_id = lc.subParcel
	WHERE usb.subparcel_id IN (SELECT subparcel_id FROM urbansim.buildings usb GROUP BY subparcel_id HAVING count(*) > 1)
)
SELECT
	bldgs.jobs AS job_spaces 
FROM
	(SELECT * FROM urbansim.buildings WHERE assign_jobs = 1) usb		--DO NOT USE MIL/PF BUILDINGS
	INNER JOIN bldgs ON usb.building_id = bldgs.building_id
;









/* ##### ASSIGN JOB_SPACES TO MULTIPLE BUILDING SUBPARCELS ##### */
--DECLARE @employment_vacancy float = 0.1;
-----MAY WANT TO THINK ABOUT EXCLUDING REALLY SMALL BUILDINGS FROM THIS QUERY
WITH bldgs AS (
  SELECT
    usb.subparcel_id
   ,usb.building_id
   ,lc.emp/ COUNT(*) OVER (PARTITION BY usb.subparcel_id) +
     CASE 
       WHEN ROW_NUMBER() OVER (PARTITION BY usb.subparcel_id ORDER BY usb.shape.STArea()) <= (lc.emp % COUNT(*) OVER (PARTITION BY usb.subparcel_id)) THEN 1 
       ELSE 0 
	 END jobs
  FROM
    (SELECT * FROM urbansim.buildings WHERE assign_jobs = 1) usb		--DO NOT USE MIL/PF BUILDINGS
    INNER JOIN (SELECT subParcel, SUM(CAST(CEILING(emp_adj*(1+@employment_vacancy))AS int)) emp
		FROM gis.ludu2015 lc
		INNER JOIN socioec_data.ca_edd.emp_2013 emp 
		ON lc.Shape.STContains(emp.shape) = 1
		WHERE emp.emp_adj IS NOT NULL
		GROUP BY lc.subParcel) lc
	ON usb.subparcel_id = lc.subParcel
  WHERE
    usb.subparcel_id IN (SELECT subparcel_id FROM urbansim.buildings usb GROUP BY subparcel_id HAVING count(*) > 1)
	)
UPDATE
	usb
SET
	usb.job_spaces = bldgs.jobs
FROM
	(SELECT * FROM urbansim.buildings WHERE assign_jobs = 1) usb		--DO NOT USE MIL/PF BUILDINGS
	INNER JOIN bldgs ON usb.building_id = bldgs.building_id
;
