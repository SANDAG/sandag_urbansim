/* Populate table: staging.schedule1_municipal_zoning 
	 acres_constrained: 
		acres multiplied by proportion developable (proportion_undevelopable subtracted from one)
	allowed_dev_type: 
		dev_type_aggregated cte. allowed development types aggregated from zoning_allowed_use
		using parent_zoning_id from zoning or zoning_id when parent_zoning does not exist.
		zoning_id from zoning_parcels filtered on zoning schedule id
 	current_units: 
		residential_units cte. sum of residential units over every building on a parcel
	jurisdiction_id:
		from urbansim.parcels
	max_dua_units: 
		acres_constrained multiplied by max dua 
	parcel_id:
		from urbansim.parcels

*/

INSERT INTO staging.schedule1_municipal_zoning (
	parcel_id, rounding_scenario, jurisdiction_id, source_zoning_schedule_id, zone, 
	zoning_id, parent_zoning, allowed_dev_type, 
	acres, proportion_undevelopable,acres_constrained, 
	min_dua, max_dua, max_dua_units, max_res_units, cap_hs, current_units) (
WITH dev_type_aggregated AS (
SELECT 	zp.parcel_id, coalesce(zoning.parent_zoning_id, zoning.zoning_id) as lookup_zoning_id, 
	array_agg(allowed.development_type_id) as allowed_dev_type 
FROM	urbansim.zoning_allowed_use allowed
JOIN	urbansim.zoning zoning
ON 	allowed.zoning_id = coalesce(zoning.parent_zoning_id, zoning.zoning_id)
JOIN 	urbansim.zoning_parcels zp
ON 	zoning.zoning_id = zp.zoning_id
WHERE 	zp.zoning_schedule_id = 1 AND zoning.jurisdiction_id = 9
GROUP BY lookup_zoning_id, zp.parcel_id),
residential_units AS (
SELECT 	p.parcel_id, coalesce(SUM(b.residential_units),0) AS current_units
FROM	urbansim.buildings b
RIGHT JOIN urbansim.parcels p 
ON 	b.parcel_id = p.parcel_id
WHERE 	p.jurisdiction_id = 9
GROUP BY p.parcel_id)
SELECT 	parcels.parcel_id, 8 as rounding_scenario, parcels.jurisdiction_id, zoning.zoning_schedule_id, zoning.zone, 
	zoning.zoning_id, zoning.parent_zoning_id, dev.allowed_dev_type,
	parcels.parcel_acres as acres, parcels.proportion_undevelopable, 
	(1 - COALESCE(parcels.proportion_undevelopable,0)) * parcels.parcel_acres  AS acres_constrained, 
	zoning.min_dua, zoning.max_dua,  
	(1 - COALESCE(parcels.proportion_undevelopable,0)) * parcels.parcel_acres * zoning.max_dua AS max_dua_units,
	zoning.max_res_units, zoning.cap_hs, res.current_units
FROM 	urbansim.zoning zoning
JOIN 	urbansim.zoning_parcels zp 
ON 	zoning.zoning_id = zp.zoning_id 
JOIN 	urbansim.parcels parcels
ON 	zp.parcel_id = parcels.parcel_id
LEFT JOIN dev_type_aggregated dev
ON 	zp.parcel_id = dev.parcel_id
JOIN 	residential_units res
ON 	res.parcel_id = zp.parcel_id
WHERE 	zp.zoning_schedule_id = 1 AND zoning.jurisdiction_id = 9);

--rounding scenarios:

----rounding scenario 1
-- UPDATE staging.schedule1_municipal_zoning
-- SET max_dua_units_rounded = (
-- CASE
-- WHEN max_dua_units > 0 AND max_dua_units < 1 THEN 1
-- ELSE ROUND(max_dua_units)
-- END)
-- where jurisdiction_id = 17 and rounding_scenario = 1;

-- -- rounding scenario 2
-- UPDATE staging.schedule1_municipal_zoning
-- SET max_dua_units_rounded = 
-- ROUND(max_dua_units)
-- where jurisdiction_id = 17 and rounding_scenario = 2;

-- -- -- rounding scenario 3
-- UPDATE staging.schedule1_municipal_zoning
-- SET max_dua_units_rounded = 
-- ROUND(ROUND(max_dua_units,1))
-- where jurisdiction_id = 17 and rounding_scenario = 3;

-- -- -- rounding scenario 4
-- UPDATE staging.schedule1_municipal_zoning
-- SET max_dua_units_rounded = 
-- ROUND(max_dua_units-0.7+0.5)
-- where jurisdiction_id = 17 and rounding_scenario = 4;

-- -- -- rounding scenario 5
-- UPDATE staging.schedule1_municipal_zoning
-- SET max_dua_units_rounded = 
-- FLOOR(max_dua_units)
-- where jurisdiction_id = 10 and rounding_scenario = 5;

-- -- -- rounding scenario 6
-- UPDATE staging.schedule1_municipal_zoning
-- SET max_dua_units_rounded = (
-- CASE
-- WHEN max_dua_units > 0 AND max_dua_units < 1 THEN 1
-- ELSE FLOOR(max_dua_units)
-- END)
-- where jurisdiction_id = 10 and rounding_scenario = 6;

-- -- -- rounding scenario 7
-- UPDATE staging.schedule1_municipal_zoning
-- SET max_dua_units_rounded = 
-- CEIL(max_dua_units)
-- where jurisdiction_id = 9 and rounding_scenario = 7;


-- -- -- rounding scenario 8
UPDATE staging.schedule1_municipal_zoning
SET max_dua_units_rounded = 
ROUND(ROUND(max_dua_units,1))
where jurisdiction_id = 9 and rounding_scenario = 8;

----- end rounding scenarios

-- calculate minimum of max_dua_units and max_res_units 
UPDATE staging.schedule1_municipal_zoning
SET minimum_of_max_dua_units_and_max_res_units = (
CASE
WHEN max_dua_units_rounded IS NULL AND max_res_units is NULL THEN 0
ELSE round(LEAST(max_dua_units_rounded, max_res_units))
END)
WHERE jurisdiction_id = 9;


-- get addl_units by subtracting current units
UPDATE staging.schedule1_municipal_zoning
SET addl_units =
minimum_of_max_dua_units_and_max_res_units - current_units
WHERE jurisdiction_id = 9;


--set addl_units less than zero to zero
UPDATE staging.schedule1_municipal_zoning
SET addl_units = 0
WHERE addl_units < 0 and jurisdiction_id = 9;


-- add scheduled dev column
UPDATE staging.schedule1_municipal_zoning
SET scheduled_development = FALSE where jurisdiction_id = 9;


-- set scheduled dev TRUE for parcels in scheduled development
UPDATE staging.schedule1_municipal_zoning
SET scheduled_development = TRUE
WHERE parcel_id IN (SELECT parcel_id FROM urbansim.scheduled_development_parcels) and jurisdiction_id = 9;


-- set siteid for scheduled dev
UPDATE staging.schedule1_municipal_zoning s1
SET siteid_sched_dev = subquery.siteid
FROM (SELECT parcel_id, siteid
	FROM urbansim.scheduled_development_parcels) AS subquery
WHERE s1.parcel_id = subquery.parcel_id and jurisdiction_id = 9;


--set addl_units to zero for scheduled dev parcels
UPDATE staging.schedule1_municipal_zoning
SET addl_units = 0
WHERE scheduled_development = TRUE and jurisdiction_id = 9;


--set addl_units for sched developments 
--note: all units assigned to one parcel in the sched dev for simplicity
UPDATE staging.schedule1_municipal_zoning s1
SET addl_units = subquery.units_per_parcel
FROM (
WITH parcels_one AS (
SELECT siteid_sched_dev, MIN(parcel_id) as parcelid
FROM staging.schedule1_municipal_zoning
WHERE siteid_sched_dev > 0
GROUP BY siteid_sched_dev)
SELECT os.parcelid, (COALESCE(sfu,0) + COALESCE(mfu,0)) AS units_per_parcel
FROM urbansim.scheduled_development sd
JOIN parcels_one os
ON os.siteid_sched_dev = sd."siteID"
) AS subquery
WHERE s1.parcel_id = subquery.parcelid and jurisdiction_id = 2;
