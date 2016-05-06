USE [pecas_sr13];
GO

/* We have to sum emp by activity by parcel and then join to the parcel file and the emp file.
   Apportion the floorspace on the parcel by each activity's share of total emp. Then divide this
   derived sqft used by each activity by the employment in that activity to get the sqft per employee. */
WITH empTotalByLckey AS
(
	SELECT e.lckey
	,SUM(e.emp_pecas) emp
	FROM pecas_sr13_dev.gis.emp_2012 e
	GROUP BY e.lckey
), s AS
(
	SELECT e.lckey
		  ,x.development_type_id
		  ,SUM(e.emp_pecas) as emp
		  ,(CASE
			  WHEN empTotalByLckey.emp = 0 THEN 0
			  ELSE SUM(e.emp_pecas) / CAST(empTotalByLckey.emp as float)
			END) * p.floorspace AS sqftUsed
	FROM pecas_sr13_dev.gis.emp_2012 e
		INNER JOIN empTotalByLckey
		ON empTotalByLckey.lckey = e.lckey
			INNER JOIN staging.parcel p
				ON p.lckey = e.lckey
				AND p.yr = 2012
					INNER JOIN urbansim.space_type_development_type x
					ON x.space_type_id = p.space_type_id
	GROUP BY e.lckey
		,x.development_type_id
		,p.floorspace
		,empTotalByLckey.emp
)
SELECT development_type_id
	,CASE	
		WHEN SUM(emp) = 0 THEN 0
		ELSE SUM(sqftUsed) / SUM(emp)
	 END as sqft_per_emp
INTO spacecore.input.sqft_per_emp_by_dev_type
FROM s
WHERE s.sqftUsed > 0
GROUP BY development_type_id
ORDER BY development_type_id