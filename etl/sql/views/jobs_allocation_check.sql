USE spacecore
;

/* --OLD VERSION
WITH job_spaces AS(
	SELECT source, sector_id, SUM(job_spaces) AS job_spaces
	FROM urbansim.job_spaces_20180516
	GROUP BY sector_id, source
)
,jobs_wac AS(
	SELECT sector_id, COUNT(*) AS jobs_wac
	FROM input.jobs_wac_2012_2016_3
	WHERE yr = 2015
	GROUP BY sector_id
)
,jobs_gov AS(
	SELECT sector_id, COUNT(*) AS jobs_gov
	FROM input.jobs_gov_2012_2016_3
	WHERE yr = 2015
	GROUP BY sector_id
)
,jobs_mil AS(
	SELECT sector_id, COUNT(*) AS jobs_mil
	FROM input.jobs_military_2012_2016
	WHERE yr = 2015
	GROUP BY sector_id
)
,jobs_sem AS(
	SELECT (sector_id-100) AS sector_id, COUNT(*) AS jobs_sem
	FROM input.jobs_selfemployed_2012_2016
	WHERE yr = 2015
	GROUP BY sector_id
)
, allocated AS(
	SELECT sector_id, COUNT(*) AS allocated
	FROM urbansim.jobs_20180516
	GROUP BY sector_id
)
SELECT
	job_spaces.sector_id
	,i.sandag_industry_name
	,job_spaces
	,job_spaces.source AS js_source
	,jobs_wac
	,jobs_gov
	,jobs_mil
	,jobs_sem
	,(ISNULL(jobs_wac, 0) + ISNULL(jobs_gov, 0) + ISNULL(jobs_mil, 0) + ISNULL(jobs_sem, 0)) AS jobs_total
	,allocated AS 'jobs (allocated)'
	,job_spaces - allocated AS vacancy
FROM job_spaces
FULL OUTER JOIN jobs_wac
	ON job_spaces.sector_id = jobs_wac.sector_id
FULL OUTER JOIN jobs_gov
	ON job_spaces.sector_id = jobs_gov.sector_id
FULL OUTER JOIN jobs_mil
	ON job_spaces.sector_id = jobs_mil.sector_id
FULL OUTER JOIN jobs_sem
	ON job_spaces.sector_id = jobs_sem.sector_id
FULL OUTER JOIN allocated
	ON job_spaces.sector_id = allocated.sector_id
JOIN [socioec_data].[ca_edd].[sandag_industry] AS i
ON job_spaces.sector_id = i.sandag_industry_id
ORDER BY COALESCE(jobs_wac.sector_id, jobs_sem.sector_id, job_spaces.sector_id)
*/

--BY SECTOR
WITH job_spaces AS(
	SELECT sector_id, SUM(job_spaces) AS job_spaces
	FROM urbansim.job_spaces
	GROUP BY sector_id
)
,jobs_wac AS(
	SELECT sector_id, COUNT(*) AS jobs_wac
	FROM input.jobs_wac_2012_2016_3
	WHERE yr = 2015
	GROUP BY sector_id
)
,jobs_gov AS(
	SELECT sector_id, COUNT(*) AS jobs_gov
	FROM input.jobs_gov_2012_2016_3
	WHERE yr = 2015
	GROUP BY sector_id
)
,jobs_mil AS(
	SELECT sector_id, COUNT(*) AS jobs_mil
	FROM input.jobs_military_2012_2016
	WHERE yr = 2015
	GROUP BY sector_id
)
,jobs_sem AS(
	SELECT (sector_id-100) AS sector_id, COUNT(*) AS jobs_sem
	FROM input.jobs_selfemployed_2012_2016
	WHERE yr = 2015
	GROUP BY sector_id
)
, allocated AS(
	SELECT sector_id, COUNT(*) AS allocated
	FROM urbansim.jobs
	GROUP BY sector_id
)
SELECT
	i.sandag_industry_id AS sector_id
	,i.sandag_industry_name
	,job_spaces
	,jobs_wac
	,jobs_gov
	,jobs_mil
	,jobs_sem
	,(ISNULL(jobs_wac, 0) + ISNULL(jobs_gov, 0) + ISNULL(jobs_mil, 0) + ISNULL(jobs_sem, 0)) AS jobs_total
	,allocated AS 'jobs (allocated)'
	,job_spaces - allocated AS vacancy
FROM [socioec_data].[ca_edd].[sandag_industry] AS i
LEFT JOIN job_spaces
	ON i.sandag_industry_id = job_spaces.sector_id
LEFT JOIN jobs_wac
	ON i.sandag_industry_id = jobs_wac.sector_id
LEFT JOIN jobs_gov
	ON i.sandag_industry_id = jobs_gov.sector_id
LEFT JOIN jobs_mil
	ON i.sandag_industry_id = jobs_mil.sector_id
LEFT JOIN jobs_sem
	ON i.sandag_industry_id = jobs_sem.sector_id
LEFT JOIN allocated
	ON i.sandag_industry_id = allocated.sector_id
ORDER BY i.sandag_industry_id
;

--DETAILED
WITH job_spaces AS(
	SELECT source, sector_id, SUM(job_spaces) AS job_spaces
	FROM urbansim.job_spaces
	GROUP BY sector_id, source
)
,jobs_wac AS(
	SELECT sector_id, COUNT(*) AS jobs_wac, 'EDD' AS source
	FROM input.jobs_wac_2012_2016_3
	WHERE yr = 2015
	GROUP BY sector_id
)
,jobs_gov AS(
	SELECT sector_id, COUNT(*) AS jobs_gov, 'GOV' AS source
	FROM input.jobs_gov_2012_2016_3
	WHERE yr = 2015
	GROUP BY sector_id
)
,jobs_mil AS(
	SELECT sector_id, COUNT(*) AS jobs_mil, 'MIL' AS source
	FROM input.jobs_military_2012_2016
	WHERE yr = 2015
	GROUP BY sector_id
)
,jobs_sem AS(
	SELECT (sector_id-100) AS sector_id, COUNT(*) AS jobs_sem, 'EDD' AS source
	FROM input.jobs_selfemployed_2012_2016
	WHERE yr = 2015
	GROUP BY sector_id
)
, allocated AS(
	SELECT sector_id, COUNT(*) AS allocated, IIF(source IN ('WAC', 'SEM'), 'EDD', source) AS source
	FROM urbansim.jobs
	GROUP BY sector_id, IIF(source IN ('WAC', 'SEM'), 'EDD', source)
)
SELECT
	i.sandag_industry_id AS sector_id
	,i.sandag_industry_name
	,job_spaces
	,job_spaces.source AS js_source
	,jobs_wac
	,jobs_gov
	,jobs_mil
	,jobs_sem
	,(ISNULL(jobs_wac, 0) + ISNULL(jobs_gov, 0) + ISNULL(jobs_mil, 0) + ISNULL(jobs_sem, 0)) AS jobs_total
	,allocated AS 'jobs (allocated)'
	,job_spaces - allocated AS vacancy
FROM [socioec_data].[ca_edd].[sandag_industry] AS i
LEFT JOIN job_spaces
ON i.sandag_industry_id = job_spaces.sector_id
LEFT JOIN jobs_wac
	ON i.sandag_industry_id = jobs_wac.sector_id
	AND job_spaces.source = jobs_wac.source
LEFT JOIN jobs_gov
	ON i.sandag_industry_id = jobs_gov.sector_id
	AND job_spaces.source = jobs_gov.source
LEFT JOIN jobs_mil
	ON i.sandag_industry_id = jobs_mil.sector_id
	AND job_spaces.source = jobs_mil.source
LEFT JOIN jobs_sem
	ON i.sandag_industry_id = jobs_sem.sector_id
	AND job_spaces.source = jobs_sem.source
LEFT JOIN allocated
	ON i.sandag_industry_id = allocated.sector_id
	AND job_spaces.source = allocated.source
ORDER BY i.sandag_industry_id, js_source
