USE spacecore
;

--DEFINE YEAR FOR JOBS
DECLARE @yr int = 2016
;
SELECT @yr AS 'year'
;

/*
BEFORE PROCEEDING, MAKE SURE NUMBER OF JOB SPACES BY SECTOR
IS GREATER THAN NUMBER OF JOBS PER SECTOR TO BE ALLOCATED
*/
--GENERAL CHECK
SELECT SUM(job_spaces) AS job_spaces_edd FROM urbansim.job_spaces WHERE [source] = 'EDD'
SELECT COUNT(*) AS jobs FROM input.jobs_wac_2012_2016_3 WHERE yr = @yr
;

--/*
--SECTOR CHECK
WITH job_spaces AS(
	SELECT sector_id, SUM(job_spaces) AS job_spaces
	FROM urbansim.job_spaces
	WHERE [source] = 'EDD'
	GROUP BY sector_id
)
,jobs AS(
	SELECT sector_id, COUNT(*) AS jobs
	FROM input.jobs_wac_2012_2016_3
	WHERE yr = @yr
	GROUP BY sector_id
)
SELECT
	COALESCE(jobs.sector_id, job_spaces.sector_id) AS  sector_id
	,job_spaces
	,jobs
	,job_spaces - jobs AS extra_job_spaces
FROM job_spaces
FULL OUTER JOIN jobs
	ON job_spaces.sector_id = jobs.sector_id
ORDER BY COALESCE(jobs.sector_id, job_spaces.sector_id);
--*/

/*#################### GENERATE JOBS TABLE										####################*/
--CREATE TABLE
DROP TABLE IF EXISTS urbansim.jobs
;
CREATE TABLE urbansim.jobs (
	job_id bigint NOT NULL PRIMARY KEY
	,sector_id int NOT NULL INDEX ix_jobs_sector_id NONCLUSTERED (sector_id)
	,building_id bigint NOT NULL INDEX ix_jobs_building_id NONCLUSTERED (building_id)
	,source nchar(3) NOT NULL
	,run int
)
;

/*#################### RUN 1/2 - ALLOCATE WAC JOBS BY BLOCK						####################*/
WITH spaces as (
SELECT 
	ROW_NUMBER() OVER (PARTITION BY block_id, sector_id ORDER BY sector_id, row_space, job_spaces ) AS idx
	,block_id
	,building_id
	,sector_id
FROM(
	SELECT
		ROW_NUMBER() OVER (PARTITION BY usb.building_id ORDER BY s.job_spaces)*100/s.job_spaces AS row_space
		,usb.block_id
		,usb.parcel_id
		,usb.building_id
		,s.sector_id
		,s.job_spaces
	FROM (SELECT * FROM urbansim.buildings WHERE assign_jobs = 1) usb		--DO NOT USE MIL/PF BUILDINGS
	JOIN [spacecore].[urbansim].[job_spaces] AS s ON usb.building_id = s.building_id
	JOIN ref.numbers AS n ON n.numbers <= s.job_spaces
	WHERE s.[source] = 'EDD'
	--WHERE usb.building_id = 31
	--WHERE usb.block_id IN (60730203071005, 60730203062012)
	) x
),
jobs AS (
	SELECT 
	  ROW_NUMBER() OVER (PARTITION BY block_id, sector_id ORDER BY job_id) idx
	  ,job_id
	  ,block_id
	  ,sector_id
	FROM
	  spacecore.input.jobs_wac_2012_2016_3
	WHERE yr = @yr
	--AND block_id IN (60730203071005, 60730203062012)
)
INSERT INTO urbansim.jobs (job_id, sector_id, building_id, source, run)
SELECT jobs.job_id
	,jobs.sector_id
	,spaces.building_id
	--,spaces.block_id				--XX_TEST
	,'WAC' AS source
	,0 AS run
FROM spaces
JOIN jobs
ON spaces.block_id = jobs.block_id
AND spaces.sector_id = jobs.sector_id
AND spaces.idx = jobs.idx
;

--SELECT * FROM urbansim.jobs WHERE building_id = 31
--SELECT * FROM urbansim.jobs WHERE building_id = 178
--SELECT * FROM urbansim.job_spaces WHERE building_id = 31
--SELECT * FROM urbansim.job_spaces WHERE building_id = 178
--SELECT * FROM urbansim.jobs WHERE block_id = 60730203071005 AND sector_id = 16

----CHECKS
--SELECT SUM(job_spaces) FROM urbansim.job_spaces WHERE [source] = 'EDD'
--SELECT COUNT(*) FROM input.jobs_wac_2012_2016_3 WHERE yr = 2014
--SELECT COUNT(*) FROM spacecore.urbansim.jobs
--SELECT COUNT(*) FROM input.jobs_wac_2012_2016_3 WHERE yr = 2014 AND job_id NOT IN (SELECT job_id FROM urbansim.jobs)
--;

/*#################### RUN 2/2 - ALLOCATE REMAINING WAC JOBS TO NEAREST BLOCK	 ####################*/
/*### THIS RUN HAS AN ITERATIVE STEP WITHIN AN ITERATIVE STEP ###*/
/*
	STEP 1 FOR INITIAL DISTANCE,
	THEN STEP 2 ITERATES ALLOCATION UNTIL OUT OF JOB SPACES.
	STEP 1 FOR NEXT DISTANCE AND THEN STEP 2 TO ALLOCATE.
	DO AGAIN UNTIL OUT OF JOBS TO ALLOCATE.
*/
--DECLARE @yr			int = 2012;
DECLARE @sector_id	smallint = 1;		--xxBRAKES
DECLARE @radius		int = 1320;			--1/4 MILE SEARCHRADIUS, INITIAL
DECLARE @radius_i	int = 1320;			--1/4 MILE SEARCHRADIUS, INCREMENT
DECLARE @run		smallint = 1;		--NUMBERED RUNS TO TRACK ALLOCATION DISTANCE

/*##### STEP 0 - BEGIN SECTOR, GET JOBS AND SPACES	#####*/
WHILE (@sector_id <= 20)		--xxBRAKES
BEGIN
	PRINT 'BEGIN SECTOR #' + CAST(@sector_id AS char)+ '------------------------------------------------------------'
	
	--SPACES
	DROP TABLE IF EXISTS #spaces
	SELECT
		usb.building_id
		--,js.sector_id
		,js.job_spaces - ISNULL(jsu.job_spaces_used, 0) AS job_spaces		--xxBRAKES^^
	INTO #spaces
	FROM urbansim.buildings AS usb
	JOIN urbansim.job_spaces AS js
		ON js.building_id = usb.building_id
		AND js.sector_id = @sector_id
		AND js.[source] = 'EDD'
	FULL JOIN (SELECT
				building_id
				,COUNT(building_id) AS job_spaces_used
			FROM urbansim.jobs
			WHERE sector_id = @sector_id
			GROUP BY building_id) AS jsu
		ON jsu.building_id = js.building_id
	WHERE js.job_spaces - ISNULL(jsu.job_spaces_used, 0) > 0		--xxBRAKES
	PRINT 'FOUND SPACES'

	--SELECT SUM(job_spaces) FROM #spaces
	--SELECT * FROM #spaces ORDER BY building_id;			--xxCHECK
	

	--JOBS LIST
	DROP TABLE IF EXISTS #jobs_list
	;
	SELECT 
		job_id
		,wac.block_id
		,sector_id
	INTO #jobs_list
	FROM input.jobs_wac_2012_2016_3 AS wac
	WHERE yr = @yr
	AND NOT EXISTS (SELECT * FROM urbansim.jobs AS jobs WHERE wac.job_id = jobs.job_id)		--xxBRAKES
	AND sector_id = @sector_id
	PRINT 'FOUND JOBS'	

	/*##### STEP 1 - CREATE BUFFER AND FIND JOBS NEAR JOB SPACES	#####*/
	--BREAK IF JOBS LIST EMPTY
	WHILE (SELECT COUNT(*) FROM #jobs_list) > 0
	BEGIN
		PRINT 'START FIND JOBS NEAR JOB SPACES'
		
		--SPACES BUILDINGS
		DROP TABLE IF EXISTS #spaces_bldg;
		SELECT
			s.building_id
			,b.shape
		INTO #spaces_bldg
		FROM #spaces AS s
		JOIN urbansim.buildings AS b 
			ON s.building_id = b.building_id
		;
		--CREATE SPATIAL INDEX
		ALTER TABLE #spaces_bldg ALTER COLUMN building_id int NOT NULL;
		ALTER TABLE #spaces_bldg ADD CONSTRAINT pk_spaces_building_id PRIMARY KEY CLUSTERED (building_id);
		ALTER TABLE #spaces_bldg ALTER COLUMN shape geometry NOT NULL;
		CREATE SPATIAL INDEX [ix_spatial_buildings_shape] ON #spaces_bldg
		(
			shape
		) USING  GEOMETRY_GRID
			WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
			CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

		--JOBS BLOCKS
		DROP TABLE IF EXISTS #jobs_block;
		SELECT
			CAST(b.block_id AS bigint) AS block_id
			,Shape.STCentroid() AS cent
			,b.Shape.STCentroid().STBuffer(@radius) AS buff
		INTO #jobs_block
		FROM (SELECT DISTINCT block_id FROM #jobs_list) AS j
		JOIN (SELECT BLOCKID10 AS block_id, Shape FROM ref.blocks) AS b ON j.block_id = b.block_id
		;
		--CREATE SPATIAL INDEX
		ALTER TABLE #jobs_block ALTER COLUMN block_id bigint NOT NULL
		ALTER TABLE #jobs_block ADD CONSTRAINT pk_jobs_job_id PRIMARY KEY CLUSTERED (block_id) 
		ALTER TABLE #jobs_block ALTER COLUMN buff geometry NOT NULL
		CREATE SPATIAL INDEX [ix_spatial_jobs_cent] ON #jobs_block
		(
			cent
		) USING  GEOMETRY_GRID
			WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
			CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
		CREATE SPATIAL INDEX [ix_spatial_jobs_buff] ON #jobs_block
		(
			buff
		) USING  GEOMETRY_GRID
			WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
			CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
			;
		PRINT 'SPACES AND JOBS LOCATIONS CREATED'

		--FIND NEAR FEATURES AND STORE TO TABLE
		DROP TABLE IF EXISTS #dist;
		SELECT
			j.block_id
			,s.building_id
			,CAST(j.cent.STDistance(s.shape) AS int) AS dist
		INTO #dist																		--**SELECT INTO
		FROM #jobs_block AS j
			,#spaces_bldg AS s
		WHERE j.buff.STIntersects(s.shape) = 1
		PRINT 'FOUND JOBS NEAR SPACES'
		

		--JOIN DISTANCES
		DROP TABLE IF EXISTS #near;
		SELECT
			j.job_id
			,j.block_id
			,s.building_id
			,s.job_spaces
			,d.dist
		INTO #near
		FROM #jobs_list AS j
		JOIN #dist AS d
			ON d.block_id = j.block_id
		JOIN #spaces AS s
			ON s.building_id = d.building_id
		PRINT 'DISTANCES OBTAINED'





		/*##### STEP 2 - ALLOCATE JOBS BY DISTANCE #####*/
		--START ALLOCATION
		--BREAK IF NO NEAR FOUND
		WHILE (SELECT SUM(job_spaces) FROM #near) > 0					--ITERATE- RUN MULTIPLE TIMES FOR EACH DISTANCE, UNTIL ALLOCATION IS COMPLETE
		--AND (SELECT COUNT(*) FROM #jobs) > 0
		BEGIN
			PRINT 'ALLOCATING JOBS TO NEAR SPACES'	
			--NEAREST #TABLE
			DROP TABLE IF EXISTS #nearest
			SELECT
				ROW_NUMBER() OVER(PARTITION BY job_id ORDER BY job_id, dist) dist_id
				,job_id
				--,sector_id
				,building_id
				,job_spaces
				,dist
			INTO #nearest
			FROM #near
			WHERE job_spaces > 0
			--SELECT * FROM #nearest
			PRINT 'NEAREST'

			--GRAB #TABLE	
			DROP TABLE IF EXISTS #grab
			SELECT
				ROW_NUMBER() OVER(PARTITION BY building_id ORDER BY dist) row_id
				,job_id
				--,sector_id
				,building_id
				,job_spaces
			INTO #grab
			FROM #nearest
			WHERE dist_id = 1
			--SELECT * FROM #grab
			PRINT 'GRAB'

			--ALLOCATE JOBS AND STORE TO TABLE
			INSERT INTO urbansim.jobs (job_id, sector_id, building_id, source, run)			--**SELECT INTO
			SELECT 
				job_id
				,@sector_id AS sector_id
				,building_id
				,'WAC'
				,@run																		--**INSERT RUN NUMBER
			FROM #grab
			WHERE row_id <= job_spaces
			PRINT 'JOBS STORED'
			;

			DROP TABLE #nearest
			DROP TABLE #grab

			--UPDATE JOBS LIST
			DELETE FROM #near WHERE job_id IN (SELECT job_id FROM urbansim.jobs WHERE run = @run);
			
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
						AND [source] = 'EDD'
						) AS js
				ON n.building_id = js.building_id
							
		END;

		--UPDATE JOBS LIST
		DELETE FROM #jobs_list WHERE job_id IN (SELECT job_id FROM urbansim.jobs WHERE run = @run);

		--UPDATE JOB SPACES
		UPDATE n
		SET job_spaces = js.job_spaces_old - jsu.job_spaces_used
		FROM #spaces AS n
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
					AND [source] = 'EDD'
					) AS js
			ON n.building_id = js.building_id

		--INCREMENT RADIUS FOR BUFFER
		SET @radius = @radius + @radius_i
		--INCREMENT RUN NUMBER
		SET @run = @run + 1

		SELECT COUNT(*) FROM #jobs_list;
		--SELECT * FROM #jobs;			--xxCHECK

	END;

	PRINT 'COMPLETED SECTOR #' + CAST(@sector_id AS char)+ '------------------------------------------------------------';
	SET @sector_id = @sector_id + 1
	SET @radius = 1
	SET @run = 1
END;

