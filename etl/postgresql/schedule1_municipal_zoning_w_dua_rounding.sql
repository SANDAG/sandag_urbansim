--------------------------------------------------------------------------------
-------------------------------------------------------------------------------
/* Populate table:  urbansim_output.res_capacity 
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


-- 
--
select 'new',jurisdiction_id,sum(addl_units) 
from urbansim_output.res_capacity 
GROUP BY jurisdiction_id
union 
select 'old',jurisdiction_id,sum(addl_units) 
from staging.schedule1_municipal_zoning
GROUP BY jurisdiction_id,rounding_scenario
order by jurisdiction_id

*/ 

--DELETE FROM urbansim_output.res_capacity where 
----jurisdiction_id NOT IN (1,2,3,4) and 
--schedule_id = 1

INSERT INTO urbansim_output.res_capacity (
	parcel_id, jurisdiction_id, schedule_id, scheduled_development, 
        source_zoning_schedule_id, zone, zoning_id, parent_zoning_id, 
       allowed_dev_type, acres, undevelopable_proportion, 
       developable_acres, 
       zoning_min_dua, zoning_max_dua, 
       zoning_max_dua_units, 
       zoning_max_res_units, 
       buildings_res_units) (
WITH dev_type_aggregated AS (
SELECT 	zp.parcel_id, coalesce(zoning.parent_zoning_id, zoning.zoning_id) as lookup_zoning_id, 
	array_agg(allowed.development_type_id) as allowed_dev_type 
FROM	urbansim.zoning_allowed_use allowed
JOIN	urbansim.zoning zoning
ON 	allowed.zoning_id = coalesce(zoning.parent_zoning_id, zoning.zoning_id)
JOIN 	urbansim.zoning_parcels zp
ON 	zoning.zoning_id = zp.zoning_id
WHERE 	zp.zoning_schedule_id = 1 
AND zoning.jurisdiction_id NOT IN (1,2,3,4)
GROUP BY lookup_zoning_id, zp.parcel_id),
residential_units AS (
SELECT 	p.parcel_id, coalesce(SUM(b.residential_units),0) AS current_units
FROM	urbansim.buildings b
RIGHT JOIN urbansim.parcels p 
ON 	b.parcel_id = p.parcel_id
WHERE 	p.jurisdiction_id NOT IN (1,2,3,4)
GROUP BY p.parcel_id)
SELECT 	parcels.parcel_id, parcels.jurisdiction_id, 1 as schedule_id, FALSE as scheduled_development,
	zoning.zoning_schedule_id, zoning.zone, zoning.zoning_id, zoning.parent_zoning_id, 
	dev.allowed_dev_type, parcels.parcel_acres, parcels.proportion_undevelopable, 
	(1 - COALESCE(parcels.proportion_undevelopable,0)) * parcels.parcel_acres,
	zoning.min_dua, zoning.max_dua,  
	(1 - COALESCE(parcels.proportion_undevelopable,0)) * parcels.parcel_acres * zoning.max_dua,
	zoning.max_res_units,  res.current_units
FROM 	urbansim.zoning zoning
JOIN 	urbansim.zoning_parcels zp 
ON 	zoning.zoning_id = zp.zoning_id 
JOIN 	urbansim.parcels parcels
ON 	zp.parcel_id = parcels.parcel_id
LEFT JOIN dev_type_aggregated dev
ON 	zp.parcel_id = dev.parcel_id
JOIN 	residential_units res
ON 	res.parcel_id = zp.parcel_id
WHERE 	zp.zoning_schedule_id = 1 
AND zoning.jurisdiction_id NOT IN (1,2,3,4)
);

-- -- rounding 
UPDATE urbansim_output.res_capacity res
SET zoning_max_dua_units_rounded = 
ROUND(res.zoning_max_dua_units - t1.threshold + 0.5)
FROM   urbansim.urbansim.dua_rounding_thresholds t1
WHERE  res.jurisdiction_id = t1.jurisdiction_id AND
res.jurisdiction_id NOT IN (1,2,3,4) AND res.schedule_id = 1;


-- calculate minimum_of_max_dua_units_and_max_res_units for parcels 
UPDATE urbansim_output.res_capacity
SET minimum_of_max_dua_units_rounded_and_max_res_units = (
CASE
WHEN zoning_max_dua_units_rounded IS NULL AND zoning_max_res_units IS NULL THEN NULL
ELSE round(LEAST(zoning_max_dua_units_rounded, zoning_max_res_units))
END)
WHERE 
jurisdiction_id NOT IN (1,2,3,4) and 
schedule_id = 1;

-- set addl_units by subtracting current units
UPDATE urbansim_output.res_capacity
SET addl_units = minimum_of_max_dua_units_rounded_and_max_res_units - buildings_res_units
WHERE 
jurisdiction_id NOT IN (1,2,3,4) and 
schedule_id = 1;

	
--set negative capacity to zero
UPDATE urbansim_output.res_capacity
SET addl_units = 0
WHERE addl_units < 0 and 
jurisdiction_id NOT IN (1,2,3,4) and 
schedule_id = 1;

-- set scheduled dev true for parcels in scheduled development
UPDATE urbansim_output.res_capacity
SET scheduled_development = TRUE
WHERE parcel_id IN (SELECT parcel_id FROM urbansim.scheduled_development_parcels) 
and jurisdiction_id NOT IN (1,2,3,4) 
and schedule_id = 1;


-- set siteid for scheduled de
UPDATE urbansim_output.res_capacity s2
SET siteid_sched_dev = subquery.siteid
FROM (SELECT parcel_id, siteid
	FROM urbansim.scheduled_development_parcels) AS subquery
WHERE s2.parcel_id = subquery.parcel_id and 
jurisdiction_id NOT IN (1,2,3,4) and 
schedule_id = 1;


--set addl_units to zero for scheduled dev
UPDATE urbansim_output.res_capacity
SET addl_units = 0
WHERE scheduled_development = TRUE and 
jurisdiction_id NOT IN (1,2,3,4) and 
schedule_id = 1;


--set addl_units for sched dev (all units to one parcel in the sched dev)
UPDATE urbansim_output.res_capacity s2
SET addl_units = subquery.units_per_parcel
FROM (
WITH parcels_one AS (
SELECT siteid_sched_dev, MIN(parcel_id) as parcelid
FROM urbansim_output.res_capacity
WHERE siteid_sched_dev > 0
GROUP BY siteid_sched_dev)
SELECT os.parcelid, (COALESCE(sfu,0) + COALESCE(mfu,0)) AS units_per_parcel
FROM urbansim.scheduled_development sd
JOIN parcels_one os
ON os.siteid_sched_dev = sd."siteID"
) AS subquery
WHERE s2.parcel_id = subquery.parcelid and 
jurisdiction_id NOT IN (1,2,3,4) and 
schedule_id = 1;
