/*#################### ASSIGN JOB_SPACES FROM EDD ####################*/
DECLARE @employment_vacancy float = 0.1;
/* ##### ASSIGN JOB_SPACES TO BUILDINGS BY SUBPARCEL ##### */
WITH emp AS(
	SELECT
		--emp2013.subparcel_id AS subparcel_id2013
		--emp2015.subparcel_id AS subparcel_id2015
		COALESCE(emp2013.subparcel_id, emp2015.subparcel_id) AS subparcel_id
		--emp2013.emp AS emp2013
		--emp2015.emp AS emp2015
		--emp2013.sector_id AS sector_id2013
		--emp2015.sector_id AS sector_id2015
		,COALESCE(emp2013.sector_id, emp2015.sector_id) AS sector_id
		,CASE
			WHEN ISNULL(emp2013.emp, 0) >= ISNULL(emp2015.emp, 0) THEN ISNULL(emp2013.emp, 0)
			ELSE emp2015.emp
		END AS emp
	FROM (
		SELECT lc.subParcel AS subparcel_id
			--,SUM(CAST(CEILING(ISNULL(emp_adj,0)*(1+@employment_vacancy))AS int)) AS emp
			,SUM(CAST(CEILING(ISNULL(emp_adj,0))AS int)) AS emp
			,emp.sandag_industry_id AS sector_id
		FROM gis.ludu2015 lc
		LEFT JOIN socioec_data.ca_edd.emp_2013 AS emp
		ON lc.Shape.STContains(emp.shape) = 1
		WHERE emp.emp_adj IS NOT NULL
		GROUP BY lc.subParcel, emp.sandag_industry_id
	) AS emp2013
	FULL OUTER JOIN (
		SELECT lc.subParcel AS subparcel_id
			--,SUM(CAST(CEILING(ISNULL(emp_adj,0)*(1+@employment_vacancy))AS int)) AS emp
			,SUM(CAST(CEILING(ISNULL(emp_adj,0))AS int)) AS emp
			,emp.sandag_industry_id AS sector_id
		FROM gis.ludu2015 lc
		LEFT JOIN (
			SELECT
				CASE
					WHEN emp1 >= emp2 AND emp1 >= emp3 THEN emp1
					WHEN emp2 >= emp1 AND emp2 >= emp3 THEN emp2
					WHEN emp3 >= emp1 AND emp3 >= emp2 THEN emp3
				END AS emp_adj
				,sandag_industry_id
				,COALESCE([point_2014],[point_parcels]) AS shape
			FROM (SELECT ISNULL(emp1,0) AS emp1, ISNULL(emp2,0) AS emp2, ISNULL(emp3,0) AS emp3, sandag_industry_id, [point_2014], [point_parcels], own FROM [ws].[dbo].[CA_EDD_EMP_2015]) x
			WHERE own = 5							--PRIVATE SECTOR
			) AS emp
		ON lc.Shape.STContains(emp.shape) = 1
		WHERE emp.emp_adj IS NOT NULL
		GROUP BY lc.subParcel, emp.sandag_industry_id
	) AS emp2015
	ON emp2013.subparcel_id = emp2015.subparcel_id
	AND emp2013.sector_id = emp2015.sector_id
)
SELECT
	usb.subparcel_id
	,usb.building_id
	,usb.block_id
	,usb.development_type_id
	--,emp.emp AS emp_total
	--,COUNT(*) OVER (PARTITION BY usb.subparcel_id, emp.sector_id) AS bldgs--
	,emp.emp/ COUNT(*) OVER (PARTITION BY usb.subparcel_id, emp.sector_id) +
		CASE 
			WHEN ROW_NUMBER() OVER (PARTITION BY usb.subparcel_id, emp.sector_id ORDER BY usb.shape.STArea() DESC) <= (emp.emp % COUNT(*) OVER (PARTITION BY usb.subparcel_id, emp.sector_id)) THEN 1 
			ELSE 0 
		END 
		AS job_spaces
	,emp.sector_id
INTO urbansim.job_spaces
FROM
	(SELECT * FROM urbansim.buildings WHERE assign_jobs = 1) usb
JOIN emp
	ON usb.subparcel_id = emp.subparcel_id
;
