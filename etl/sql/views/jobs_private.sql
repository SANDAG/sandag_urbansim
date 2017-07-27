/*#################### GENERATE JOBS TABLE										####################*/
TRUNCATE TABLE urbansim.jobs;

/*#################### RUN 1/2 - ALLOCATE WAC JOBS BY BLOCK						####################*/
WITH spaces as (
SELECT 
	ROW_NUMBER() OVER (PARTITION BY block_id ORDER BY row_space, job_spaces ) AS idx	--row_block
	,block_id
	,building_id
FROM(
	SELECT
		ROW_NUMBER() OVER (PARTITION BY building_id ORDER BY job_spaces)*100/job_spaces AS row_space
		,block_id
		,parcel_id
		,building_id
		,job_spaces
	FROM (SELECT * FROM urbansim.buildings WHERE assign_jobs = 1) usb		--DO NOT USE MIL/PF BUILDINGS
	JOIN ref.numbers AS n ON n.numbers <= job_spaces
	) x
),
jobs AS (
	SELECT 
	  ROW_NUMBER() OVER (PARTITION BY block_id ORDER BY job_id) idx
	  ,job_id
	  ,block_id
	  ,sector_id
	FROM
	  spacecore.input.jobs_wac_2012_2016
	WHERE yr = 2015
)
INSERT INTO urbansim.jobs (job_id, sector_id, building_id)
SELECT jobs.job_id
	,jobs.sector_id
	,spaces.building_id
FROM spaces
JOIN jobs
ON spaces.block_id = jobs.block_id
AND spaces.idx = jobs.idx
;
--CHECKS
SELECT SUM(job_spaces) FROM urbansim.buildings
SELECT COUNT(*) FROM input.jobs_wac_2012_2016 WHERE yr = 2015
SELECT COUNT(*) FROM spacecore.urbansim.jobs
SELECT COUNT(*) FROM input.jobs_wac_2012_2016 WHERE yr = 2015 AND job_id NOT IN (SELECT job_id FROM urbansim.jobs)


/*#################### RUN 2/2 - ALLOCATE REMAINING WAC JOBS TO NEAREST BLOCK	 ####################*/
/*### THIS RUN HAS AN ITERATIVE STEP WITHIN AN ITERATIVE STEP ###*/
/*
	STEP 1 FOR INITIAL DISTANCE,
	THEN STEP 2 ITERATES ALLOCATION UNTIL OUT OF JOB SPACES.
	STEP 1 FOR NEXT DISTANCE AND THEN STEP 2 TO ALLOCATE.
	DO AGAIN UNTIL OUT OF JOBS TO ALLOCATE.
*/

/*
BEFORE PROCEEDING, MAKE SURE NUMBER OF JOB SPACES BY SECTOR
IS GREATER THAN NUMBER OF JOBS PER SECTOR TO BE ALLOCATED
*/
/*
--CHECK
WITH job_spaces AS(
	SELECT sector_id, SUM(job_spaces) AS job_spaces
	FROM urbansim.job_spaces
	GROUP BY sector_id
)
,jobs AS(
	SELECT sector_id, COUNT(*) AS jobs
	FROM input.jobs_wac_2012_2016
	WHERE yr = 2015
	GROUP BY sector_id
)
SELECT
	COALESCE(jobs.sector_id, job_spaces.sector_id)
	,job_spaces
	,jobs
	,job_spaces - jobs AS extra_spaces
FROM job_spaces
FULL OUTER JOIN jobs
	ON job_spaces.sector_id = jobs.sector_id
ORDER BY COALESCE(jobs.sector_id, job_spaces.sector_id)
*/

/*##### STEP 1 - CALCULATE DISTANCES INTO STAGING TABLE #####*/
DECLARE @radius		int = 1320;	--1/4 MILE SEARCH RADIUS START
DECLARE @radius_i	int = 1320;	--1/4 MILE SEARCH RADIUS, INCREMENTS
DECLARE @run smallint = 1;	--NUMBERED RUNS TO TRACK ALLOCATION

--** WHILE LOOP START>> *********************************************************************************************************************
WHILE (
	--CHECK FOR REMAINING JOBS
	SELECT COUNT(*)
	FROM input.jobs_wac_2012_2016
	WHERE yr = 2015
	AND job_id NOT IN(SELECT job_id FROM urbansim.jobs)
	) > 0
BEGIN

	DROP TABLE staging.near;

	WITH spaces AS(
		SELECT
			js.building_id
			,js.sector_id
			,js.job_spaces - COALESCE(jsu.job_spaces_used, 0) AS job_spaces
			,usb.shape
		FROM urbansim.job_spaces AS js
		LEFT JOIN(SELECT building_id
						,sector_id
						,COUNT(building_id) AS job_spaces_used
					FROM urbansim.jobs
					GROUP BY building_id
						,sector_id) AS jsu
			ON js.building_id = jsu.building_id
			AND js.sector_id = jsu.sector_id
		JOIN urbansim.buildings AS usb ON js.building_id = usb.building_id
		WHERE js.job_spaces - COALESCE(jsu.job_spaces_used, 0) > 0
		--ORDER BY building_id, sector_id--
	)
	, jobs AS(
		SELECT 
			job_id
			,wac.block_id
			,sector_id
			,b.cent
		FROM input.jobs_wac_2012_2016 AS wac
		JOIN (SELECT BLOCKID10 AS block_id, Shape.STCentroid() AS cent FROM ref.blocks) AS b ON wac.block_id = b.block_id 
		WHERE yr = 2015
		AND job_id NOT IN(SELECT job_id FROM urbansim.jobs)
	)
	, distances AS(				--**DO FOR DISTANCE INCREMENTS OF 1/4 MILE (1,320, 2,640, 3,960... ft)
		SELECT
			building_id
			,b.block_id
			,CAST(b.cent.STDistance(usb.shape) AS int) AS dist
		FROM (SELECT *
				FROM urbansim.buildings
				WHERE building_id IN (	SELECT DISTINCT js.building_id
												--js.building_id, js.sector_id, job_spaces, job_spaces_used--
										FROM urbansim.job_spaces AS js
										JOIN (SELECT building_id				--JOB SPACES USED
													,sector_id
													,COUNT(building_id) AS job_spaces_used
												FROM urbansim.jobs
												GROUP BY building_id
													,sector_id) AS jsu
										ON js.building_id = jsu.building_id
										AND js.sector_id = jsu.sector_id
										WHERE job_spaces > job_spaces_used
										--ORDER BY building_id--
										)
			)AS usb
		JOIN (SELECT BLOCKID10 AS block_id, Shape.STCentroid() AS cent FROM ref.blocks) AS b
			ON b.cent.STBuffer(@radius).STIntersects(usb.shape) = 1
	)
		SELECT
			jobs.job_id
			,jobs.sector_id
			,spaces.building_id
			,spaces.job_spaces
			,distances.dist
		INTO staging.near								--**SELECT INTO
		FROM jobs
		JOIN spaces
			ON jobs.cent.STBuffer(@radius).STIntersects(spaces.shape) = 1
			AND jobs.sector_id = spaces.sector_id
		JOIN distances
			ON jobs.block_id = distances.block_id
			AND spaces.building_id = distances.building_id


	PRINT @radius
	PRINT @run

		/*##### STEP 2 - ALLOCATE JOBS BY DISTANCE #####*/
		--** WHILE LOOP START>> *********************************************************************************************************************
		--START ALLOCATION
		WHILE (SELECT SUM(job_spaces) FROM staging.near) > 0					--ITERATE- RUN MULTIPLE TIMES FOR EACH DISTANCE, UNTIL ALLOCATION IS COMPLETE
		BEGIN
			WITH nearest AS(	
				SELECT
					ROW_NUMBER() OVER(PARTITION BY job_id ORDER BY job_id, dist) dist_id
					,job_id
					,sector_id
					,building_id
					,job_spaces
					,dist
				FROM staging.near
				WHERE job_spaces > 0
			)
			,grab AS(	
				SELECT
					ROW_NUMBER() OVER(PARTITION BY building_id ORDER BY dist) row_id
					,job_id
					,sector_id
					,building_id
					,job_spaces
					--,dist--
				FROM nearest
				WHERE dist_id = 1
				--ORDER BY building_id--
			)
			INSERT INTO urbansim.jobs (job_id, sector_id, building_id, source, run)			--**SELECT INTO
			SELECT 
				job_id
				,sector_id
				,building_id
				,'WAC'
				,@run																		--**INSERT RUN NUMBER
			FROM grab
			WHERE row_id <= job_spaces
			ORDER BY building_id, sector_id--
			;

			--UPDATE JOBS LIST
			DELETE FROM staging.near WHERE job_id IN (SELECT job_id FROM urbansim.jobs)
			;

			--UPDATE JOB SPACES
			UPDATE n
			SET job_spaces = js.job_spaces_old - jsu.job_spaces_used
			FROM staging.near AS n
			JOIN(SELECT building_id
							,COUNT(building_id) AS job_spaces_used
						FROM urbansim.jobs
						GROUP BY building_id) AS jsu
				ON n.building_id = jsu.building_id
			JOIN(SELECT building_id
							,job_spaces AS job_spaces_old
						FROM urbansim.job_spaces) AS js
				ON n.building_id = js.building_id
			PRINT 'ANOTHER LOOP FINISHED'

			IF (SELECT COUNT(*) FROM staging.near) = 0
			BEGIN
				BREAK
				PRINT 'ALL JOBS ALLOCATED'
			END

		END
		PRINT 'OUT OF JOB SPACES'
		;
		--** WHILE LOOP END << *********************************************************************************************************************
	SET @radius = @radius + @radius_i
	SET @run = @run + 1

END;
--** WHILE LOOP END << *********************************************************************************************************************

--CHECK FOR REMAINING JOBS
SELECT COUNT(*)
FROM input.jobs_wac_2012_2016
WHERE yr = 2015
AND job_id NOT IN(SELECT job_id FROM urbansim.jobs)
;

