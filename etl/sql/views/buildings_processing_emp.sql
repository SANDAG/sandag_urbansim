DECLARE @employment_vacancy float = 0.3;


/* ##### INSERT PLACEHOLDER BUILDINGS FOR EMP ##### */
/*
INSERT INTO ws.dbo.buildings(
	development_type_id
	,parcel_id
	,improvement_value
	,residential_units
	,residential_sqft
	,non_residential_sqft
	,job_spaces
	,non_residential_rent_per_sqft
	,price_per_sqft
	,stories
	,year_built
	,shape
	,data_source
	,subparcel_id
	,centroid
	,luz_id
	--,mgra
	)
SELECT
	NULL AS development_type_id
	,lc.parcelID
	,NULL AS improvement_value
	,0 AS residential_units
	,0 AS residential_sqft
	,NULL AS non_residential_sqft
	,emp.emp_adj AS job_spaces
	,NULL AS non_residential_rent_per_sqft
	,NULL AS price_per_sqft
	,NULL AS stories
	,NULL AS year_built
	,lc.Shape.STPointOnSurface().STBuffer(1) AS shape
	,'PLACEHOLDER' AS data_source
	,lc.subParcel
	,NULL AS centroid
	,NULL AS luz_id
	--,lc.mgra as mgra
FROM gis.landcore lc
INNER JOIN socioec_data.ca_edd.emp_2013 emp 
ON lc.Shape.STContains(emp.shape) = 1
LEFT JOIN (SELECT subparcel_id FROM ws.dbo.buildings GROUP BY subparcel_id) wsb
ON lc.subParcel = wsb.subparcel_id
WHERE emp.emp_adj IS NOT NULL
AND wsb.subparcel_id IS NULL
*/

/* ##### CREATE CENTROIDS FOR PLACEHOLDER BUILDINGS ##### */
/*UPDATE ws.dbo.buildings
SET centroid = shape.STPointOnSurface(), subparcel_assignment = 'PLACEHOLDER'
WHERE centroid IS NULL
*/

/* ##### ASSIGN JOBS TO SINGLE BUILDING SUBPARCELS ##### */
/*
WITH single_bldg_jobs AS (
	SELECT lc.subParcel AS subparcel_id
		,SUM(CAST(ROUND(emp_adj*(1+@employment_vacancy),0)AS int)) emp
	FROM gis.landcore lc
	INNER JOIN socioec_data.ca_edd.emp_2013 emp 
	ON lc.Shape.STContains(emp.shape) = 1
	INNER JOIN (SELECT subparcel_id FROM ws.dbo.buildings GROUP BY subparcel_id HAVING COUNT(*) = 1) single_bldg
	ON lc.subParcel = single_bldg.subparcel_id
	WHERE emp.emp_adj IS NOT NULL
	GROUP BY lc.subParcel
	)
UPDATE 
	wsb
SET 
	wsb.job_spaces = sb.emp
FROM 
ws.dbo.buildings wsb
JOIN single_bldg_jobs sb
ON wsb.subparcel_id = sb.subparcel_id
;
*/

/* ##### ASSIGN JOBS TO MULTIPLE BUILDING SUBPARCELS ##### */
/*
-----MAY WANT TO THINK ABOUT EXCLUDING REALLY SMALL BUILDINGS FROM THIS QUERY
WITH bldgs AS (
  SELECT
    wsb.subparcel_id
   ,wsb.building_id
   ,lc.emp/ COUNT(*) OVER (PARTITION BY wsb.subparcel_id) +
     CASE 
       WHEN ROW_NUMBER() OVER (PARTITION BY wsb.subparcel_id ORDER BY wsb.shape.STArea()) <= (lc.emp % COUNT(*) OVER (PARTITION BY wsb.subparcel_id)) THEN 1 
       ELSE 0 
	 END jobs
  FROM
    ws.dbo.buildings wsb
    INNER JOIN (SELECT subParcel, SUM(CAST(ROUND(emp_adj*(1+@employment_vacancy),0)AS int)) emp
		FROM gis.landcore lc
		INNER JOIN socioec_data.ca_edd.emp_2013 emp 
		ON lc.Shape.STContains(emp.shape) = 1
		WHERE emp.emp_adj IS NOT NULL
		GROUP BY lc.subParcel) lc
	ON wsb.subparcel_id = lc.subParcel
  WHERE
    wsb.subparcel_id IN (SELECT subparcel_id FROM ws.dbo.buildings wsb GROUP BY subparcel_id HAVING count(*) > 1)
	)


UPDATE wsb
  SET wsb.job_spaces = bldgs.jobs
FROM
	ws.dbo.buildings wsb
	INNER JOIN bldgs ON wsb.building_id = bldgs.building_id
;
*/



/* ##### ASSIGN BLOCK ID ##### */
/*
UPDATE
	wsb
SET
	wsb.block_id = b.blockid10
FROM
	ws.dbo.buildings wsb
INNER JOIN
	(SELECT lc.subParcel
		,b.BLOCKID10
	FROM gis.landcore lc
	JOIN [spacecore].[ref].[blocks]b
	ON b.Shape.STContains(lc.shape.STPointOnSurface()) = 1
	) b
ON wsb.subparcel_id = b.subParcel

UPDATE
	wsb
SET
	wsb.block_id = b.blockid10
FROM
	ws.dbo.buildings wsb
INNER JOIN
	(SELECT lc.subParcel
		,b.BLOCKID10
	FROM gis.landcore lc
	JOIN [spacecore].[ref].[blocks]b
	ON b.Shape.STContains(lc.shape.STBuffer(-200).STPointOnSurface()) = 1
	WHERE lc.subParcel IN (SELECT subparcel_id FROM ws.dbo.buildings WHERE block_id IS NULL)
	) b
ON wsb.subparcel_id = b.subParcel
WHERE block_id IS NULL
*/

/*  */
WITH bldg as (
SELECT
  ROW_NUMBER() OVER (PARTITION BY block_id ORDER BY building_id) idx
  ,building_id
  ,block_id
FROM
(SELECT
  building_id
  ,subparcel_id
  ,job_spaces
  ,block_id
FROM
ws.dbo.buildings,spacecore.ref.numbers n
WHERE n.numbers <= job_spaces) bldgs
),
jobs AS (
SELECT 
  ROW_NUMBER() OVER (PARTITION BY block_id ORDER BY job_id) idx
  ,job_id
  ,block_id
  ,sector_id
FROM
  spacecore.input.jobs_wac_2013
)
SELECT jobs.job_id
	,jobs.sector_id
	,bldg.building_id
	,jobs.block_id
FROM bldg
RIGHT JOIN jobs
ON bldg.block_id = jobs.block_id
AND bldg.idx = jobs.idx
WHERE job_id IS NULL



/* CHECKS */
SELECT SUM(emp_adj)
FROM socioec_data.ca_edd.emp_2013

SELECT SUM(job_spaces)
FROM ws.dbo.buildings

SELECT COUNT(*)
FROM spacecore.input.jobs_wac_2013