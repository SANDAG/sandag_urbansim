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
--/*
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
--*/


/*##### STEP 1 - CALCULATE DISTANCES INTO STAGING TABLE #####*/
DECLARE @sector_id	smallint = 1;	--SECTOR ID

--** WHILE LOOP START>> ******************************************************************
WHILE (@sector_id <= 20)
BEGIN
			
	DECLARE @radius		int = 1320;		--1/4 MILE SEARCH RADIUS START
	DECLARE @radius_i	int = 1320;		--1/4 MILE SEARCH RADIUS, INCREMENTS
	DECLARE @run		smallint = 1;	--NUMBERED RUNS TO TRACK ALLOCATION
	
	--** WHILE LOOP START>> ******************************************************************
	WHILE (
		--CHECK FOR REMAINING JOBS
		SELECT COUNT(*)
		FROM input.jobs_wac_2012_2016
		WHERE yr = 2015
		AND job_id NOT IN(SELECT job_id FROM urbansim.jobs)
		AND sector_id = @sector_id
		) > 0
	BEGIN

		--DROP TABLE #spaces
		--DROP TABLE #jobs
		--DROP TABLE #near

		--SPACES #TABLE
		SELECT
			js.building_id
			,js.sector_id
			,js.job_spaces - COALESCE(jsu.job_spaces_used, 0) AS job_spaces
			,usb.shape
		INTO #spaces
		FROM urbansim.job_spaces AS js
		LEFT JOIN(SELECT building_id
						,COUNT(building_id) AS job_spaces_used
					FROM urbansim.jobs
					WHERE sector_id = @sector_id
					GROUP BY building_id) AS jsu
			ON js.building_id = jsu.building_id
		JOIN urbansim.buildings AS usb ON js.building_id = usb.building_id
		WHERE js.job_spaces - COALESCE(jsu.job_spaces_used, 0) > 0
		AND js.sector_id = @sector_id
		--CREATE SPATIAL INDEX
		ALTER TABLE #spaces ALTER COLUMN building_id bigint NOT NULL
		ALTER TABLE #spaces ADD CONSTRAINT pk_spaces_building_id PRIMARY KEY CLUSTERED (building_id) 
		ALTER TABLE #spaces ALTER COLUMN shape geometry NOT NULL
		CREATE SPATIAL INDEX [ix_spatial_spaces_shape] ON #spaces
		(
			shape
		) USING  GEOMETRY_GRID
			WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
			CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
		;

		--JOBS #TABLE
		SELECT 
			job_id
			,wac.block_id
			,sector_id
			,Shape.STCentroid() AS cent
			,Shape.STCentroid().STBuffer(@radius) AS buff
		INTO #jobs
		FROM input.jobs_wac_2012_2016 AS wac
		JOIN (SELECT BLOCKID10 AS block_id, Shape FROM ref.blocks) AS b ON wac.block_id = b.block_id 
		WHERE yr = 2015
		AND job_id NOT IN(SELECT job_id FROM urbansim.jobs)
		AND sector_id = @sector_id
		--CREATE SPATIAL INDEX
		ALTER TABLE #jobs ALTER COLUMN job_id int NOT NULL
		ALTER TABLE #jobs ADD CONSTRAINT pk_jobs_job_id PRIMARY KEY CLUSTERED (job_id) 
		ALTER TABLE #jobs ALTER COLUMN cent geometry NOT NULL
		CREATE SPATIAL INDEX [ix_spatial_spaces_cent] ON #jobs
		(
			cent
		) USING  GEOMETRY_GRID
			WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
			CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
		ALTER TABLE #jobs ALTER COLUMN buff geometry NOT NULL
		CREATE SPATIAL INDEX [ix_spatial_spaces_buff] ON #jobs
		(
			buff
		) USING  GEOMETRY_GRID
			WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
			CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
		;

		PRINT 'JOBS AND SPACES READY'
		--FIND NEAR FEATURES AND STORE TO TABLE
		SELECT
			jobs.job_id
			,jobs.sector_id
			,spaces.building_id
			,spaces.job_spaces
			,CAST(jobs.cent.STDistance(spaces.shape) AS int) AS dist
		INTO #near																		--**SELECT INTO
		FROM #jobs AS jobs
			,#spaces AS spaces
		WHERE jobs.buff.STIntersects(spaces.shape) = 1
		PRINT 'DISTANCES CALCULATED'

		DROP TABLE #spaces	--SELECT * FROM #spaces
		DROP TABLE #jobs	--SELECT * FROM #jobs

		PRINT @radius
		PRINT @run


		/*##### STEP 2 - ALLOCATE JOBS BY DISTANCE #####*/
		--** WHILE LOOP START>> ******************************************************************
		--START ALLOCATION
		WHILE (SELECT SUM(job_spaces) FROM #near) > 0					--ITERATE- RUN MULTIPLE TIMES FOR EACH DISTANCE, UNTIL ALLOCATION IS COMPLETE
		BEGIN

			--NEAREST #TABLE
			SELECT
				ROW_NUMBER() OVER(PARTITION BY job_id ORDER BY job_id, dist) dist_id
				,job_id
				,sector_id
				,building_id
				,job_spaces
				,dist
			INTO #nearest
			FROM #near
			WHERE job_spaces > 0

			--GRAB #TABLE	
			SELECT
				ROW_NUMBER() OVER(PARTITION BY building_id ORDER BY dist) row_id
				,job_id
				,sector_id
				,building_id
				,job_spaces
			INTO #grab
			FROM #nearest
			WHERE dist_id = 1

			--ALLOCATE JOBS AND STORE TO TABLE
			INSERT INTO urbansim.jobs (job_id, sector_id, building_id, source, run)			--**SELECT INTO
			SELECT 
				job_id
				,sector_id
				,building_id
				,'WAC'
				,@run																		--**INSERT RUN NUMBER
			FROM #grab
			WHERE row_id <= job_spaces
			;

			DROP TABLE #nearest
			DROP TABLE #grab

			--UPDATE JOBS LIST
			DELETE FROM #near WHERE job_id IN (SELECT job_id FROM urbansim.jobs WHERE run > 0)
			;

			--UPDATE JOB SPACES
			UPDATE n
			SET job_spaces = js.job_spaces_old - jsu.job_spaces_used
			FROM #near AS n
			JOIN(SELECT building_id
							,COUNT(building_id) AS job_spaces_used
						FROM urbansim.jobs
						WHERE sector_id = @sector_id
						GROUP BY building_id) AS jsu
				ON n.building_id = jsu.building_id
			JOIN(SELECT building_id
							,job_spaces AS job_spaces_old
						FROM urbansim.job_spaces
						WHERE sector_id = @sector_id
						) AS js
				ON n.building_id = js.building_id
			PRINT 'ANOTHER LOOP FINISHED'

			IF (SELECT COUNT(*) FROM #near) = 0
			BEGIN
				DROP TABLE #near
				BREAK
				PRINT 'ALL JOBS ALLOCATED'
			END;
	
		END;
		PRINT 'OUT OF JOB SPACES'
		;
		--** WHILE LOOP END << ******************************************************************
	DROP TABLE IF EXISTS dbo.#near
	SET @radius = @radius + @radius_i
	SET @run = @run + 1

	END;
	--** WHILE LOOP END << ******************************************************************

SET @sector_id = @sector_id + 1
END;
--** WHILE LOOP END << ******************************************************************

--CHECK FOR REMAINING JOBS
SELECT COUNT(*)
FROM input.jobs_wac_2012_2016
WHERE yr = 2015
AND job_id NOT IN(SELECT job_id FROM urbansim.jobs)
;



