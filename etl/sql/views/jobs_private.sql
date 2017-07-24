/*
	RUN STEP 1 FOR INITIAL DISTANCE
	THEN RUN STEP 2 TO ITERATE ALLOCATION UNTIL OUT OF JOB SPACES
	RUN STEP 1 FOR NEXT DISTANCE AND THEN RUN STEP 2 TO ALLOCATE
	DO AGAIN UNTIL OUT OF JOBS TO ALLOCATE
*/

/*##### STEP 1 - CALCULATE DISTANCES INTO STAGING TABLE #####*/
--DROP TABLE staging.near_1r
WITH spaces AS(
	SELECT
		ROW_NUMBER() OVER (ORDER BY block_id, building_id) AS j_id
		,block_id
		,parcel_id
		,building_id
		,job_spaces
		,shape
	FROM(SELECT usb.block_id
			,usb.parcel_id
			,usb.building_id
			,usb.job_spaces - COALESCE(jsu.job_spaces_used, 0) AS job_spaces
			,usb.shape
		FROM (SELECT * FROM urbansim.buildings WHERE assign_jobs = 1) usb		--DO NOT USE MIL/PF BUILDINGS
		LEFT JOIN(SELECT building_id
					,COUNT(building_id) AS job_spaces_used
				FROM urbansim.jobs
				GROUP BY building_id) AS jsu
			ON usb.building_id = jsu.building_id
		) AS usb
	WHERE job_spaces > 0
)
, jobs AS(
	SELECT 
		job_id
		,block_id
		,sector_id
		,b.cent
	FROM (SELECT *
		FROM input.jobs_wac_2012_2016
		WHERE yr = 2015
		AND job_id NOT IN(SELECT job_id FROM urbansim.jobs)) w
	JOIN (SELECT BLOCKID10, Shape.STCentroid() AS cent FROM ref.blocks) AS b
	ON w.block_id = b.BLOCKID10
)
	SELECT
		--ROW_NUMBER() OVER (PARTITION BY jobs.job_id ORDER BY jobs.job_id, jobs.cent.STDistance(spaces.shape)) row_id
		jobs.job_id
		,jobs.sector_id
		,spaces.building_id
		,spaces.block_id
		,spaces.job_spaces
		,jobs.cent.STDistance(spaces.shape) AS dist
	INTO staging.near_13r								--**INSERT RUN NUMBER
	FROM jobs
	JOIN spaces
		ON jobs.cent.STBuffer(132000).STIntersects(spaces.shape) = 1				--**DO FOR DISTANCE INCREMENTS OF 1/4 MILE (1,320, 2,640, 3,960, 5,280, 6,600, 7,920, 9,240, 10,560, 26,400, 52,800, 79,200 ft)
;

--CHECK
SELECT COUNT(*) FROM staging.near_1r
SELECT * FROM staging.near_1r


--DELETE FROM urbansim.jobs WHERE run = 1
--SELECT * FROM urbansim.jobs WHERE run = 1


--UPDATE DISTANCE TABLE
DROP TABLE staging.near
SELECT * INTO staging.near FROM staging.near_1r			--**INSERT RUN NUMBER

/*##### STEP 2 - ALLOCATE JOBS BY DISTANCE #####*/
--** WHILE LOOP START>> *********************************************************************************************************************
--START ALLOCATION
WHILE (SELECT SUM(job_spaces) FROM staging.near) > 0
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
	INSERT INTO urbansim.jobs (job_id, sector_id, building_id, source, run)					--ITERATE- RUN MULTIPLE TIMES FOR EACH DISTANCE, UNTIL ALLOCATION IS COMPLETE
	SELECT 
		job_id
		,sector_id
		,building_id
		,'WAC'
		,1												--**INSERT RUN NUMBER
	FROM grab
	WHERE row_id <= job_spaces
	;

	--UPDATE JOBS LIST
	DELETE FROM staging.near WHERE job_id IN (SELECT job_id FROM urbansim.jobs)
	;

	--UPDATE JOB SPACES
	UPDATE n
	SET job_spaces = usb.job_spaces_old - jsu.job_spaces_used
	FROM staging.near AS n
	JOIN(SELECT building_id
					,COUNT(building_id) AS job_spaces_used
				FROM urbansim.jobs
				GROUP BY building_id) AS jsu
		ON n.building_id = jsu.building_id
	JOIN(SELECT building_id
					,job_spaces AS job_spaces_old
				FROM urbansim.buildings) AS usb
		ON n.building_id = usb.building_id
	PRINT 'ANOTHER LOOP FINISHED'

END
PRINT 'OUT OF JOB SPACES'
;
--** WHILE LOOP END << *********************************************************************************************************************

--CHECK FOR REMAINING JOBS
SELECT COUNT(*)
FROM input.jobs_wac_2012_2016
WHERE yr = 2015
AND job_id NOT IN(SELECT job_id FROM urbansim.jobs)
;

--CHECK ALLOCATION BY RUN
SELECT run, COUNT(*)
FROM urbansim.jobs
GROUP BY run
ORDER BY run

