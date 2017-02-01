--------------------------------------------------------------------------------
-------------------------------------------------------------------------------
/* Populate table:  urbansim_output.res_capacity_ludu2015_to_sr13 
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
-- 
-- select scenario_id,sum(addl_units) 
-- from urbansim_output.res_capacity_ludu2015_to_sr13 
-- where jurisdiction_id NOT IN (14,19)
-- GROUP BY scenario_id
-- 
-- SELECT count(*)  
-- FROM urbansim_output.res_capacity_ludu2015_to_sr13 
-- where jurisdiction_id NOT IN (14,19)
-- GROUP BY scenario_id
-- 
--  select * FROM urbansim_output.res_capacity_ludu2015_to_sr13 
--  where jurisdiction_id NOT IN (14,19) and scenario_id = 1


--DELETE FROM urbansim_output.res_capacity_ludu2015_to_sr13 where jurisdiction_id NOT IN (14,19) and scenario_id = 1


INSERT INTO urbansim_output.res_capacity_ludu2015_to_sr13 (
	parcel_id, jurisdiction_id, scenario_id, scheduled_development, 
        parcel_id_2015_to_sr13, source_zoning_schedule_id, zone, zoning_id, parent_zoning_id, 
       allowed_dev_type, acres, undevelopable_proportion, 
       developable_acres, 
       zoning_min_dua, zoning_max_dua, 
       zoning_max_dua_units, 
       zoning_max_res_units, 
       buildings_res_units, 
       ludu2015_du, sr13_du, sr13_cap_hs_growth_adjusted, 
       sr13_cap_hs_with_negatives) (
WITH dev_type_aggregated AS (
SELECT 	zp.parcel_id, coalesce(zoning.parent_zoning_id, zoning.zoning_id) as lookup_zoning_id, 
	array_agg(allowed.development_type_id) as allowed_dev_type 
FROM	urbansim.zoning_allowed_use allowed
JOIN	urbansim.zoning zoning
ON 	allowed.zoning_id = coalesce(zoning.parent_zoning_id, zoning.zoning_id)
JOIN 	urbansim.zoning_parcels zp
ON 	zoning.zoning_id = zp.zoning_id
WHERE 	zp.zoning_schedule_id = 2 AND zoning.jurisdiction_id NOT IN (14,19)
GROUP BY lookup_zoning_id, zp.parcel_id),
residential_units AS (
SELECT 	p.parcel_id, coalesce(SUM(b.residential_units),0) AS current_units
FROM	urbansim.buildings b
RIGHT JOIN urbansim.parcels p 
ON 	b.parcel_id = p.parcel_id
WHERE 	p.jurisdiction_id NOT IN (14,19)
GROUP BY p.parcel_id)
SELECT 	parcels.parcel_id, parcels.jurisdiction_id, 1 as scenario_id, FALSE as scheduled_development,
	sr13.update_2015, zoning.zoning_schedule_id, zoning.zone, zoning.zoning_id, zoning.parent_zoning_id, 
	dev.allowed_dev_type, parcels.parcel_acres, parcels.proportion_undevelopable, 
	(1 - COALESCE(parcels.proportion_undevelopable,0)) * parcels.parcel_acres,
	zoning.min_dua, zoning.max_dua,  
	(1 - COALESCE(parcels.proportion_undevelopable,0)) * parcels.parcel_acres * zoning.max_dua,
	zoning.max_res_units,  res.current_units,
	sr13.ludu2015_du,sr13.sr13_du,zoning.cap_hs,
	sr13.sr13_cap_hs_with_negatives
FROM 	urbansim.zoning zoning
JOIN 	urbansim.zoning_parcels zp 
ON 	zoning.zoning_id = zp.zoning_id 
JOIN 	urbansim.parcels parcels
ON 	zp.parcel_id = parcels.parcel_id
LEFT JOIN dev_type_aggregated dev
ON 	zp.parcel_id = dev.parcel_id
LEFT JOIN ref.sr13_capacity sr13
ON 	zp.parcel_id = sr13.ludu2015_parcel_id
JOIN 	residential_units res
ON 	res.parcel_id = zp.parcel_id
WHERE 	zp.zoning_schedule_id = 2 AND zoning.jurisdiction_id NOT IN (14,19));

-- -- rounding
--Santee use FLOOR instead of round 
UPDATE urbansim_output.res_capacity_ludu2015_to_sr13
SET zoning_max_dua_units_rounded = 
FLOOR(zoning_max_dua_units)
where jurisdiction_id NOT IN (14,19) and scenario_id = 1;



-- calculate minimum_of_max_dua_units_and_max_res_units for parcels not in series 13
UPDATE urbansim_output.res_capacity_ludu2015_to_sr13
SET minimum_of_max_dua_units_rounded_and_max_res_units = (
CASE
WHEN zoning_max_dua_units_rounded IS NULL AND zoning_max_res_units is NULL THEN 0
ELSE round(LEAST(zoning_max_dua_units_rounded, zoning_max_res_units))
END)
WHERE jurisdiction_id NOT IN (14,19) and scenario_id = 1 and 
parcel_id NOT IN (SELECT ludu2015_parcel_id from ref.sr13_capacity sr13);


-- set addl_units to cap_hs
UPDATE urbansim_output.res_capacity_ludu2015_to_sr13
SET addl_units = sr13_cap_hs_growth_adjusted
WHERE jurisdiction_id NOT IN (14,19) and scenario_id = 1;


-- for parcels not in series 13, calc addl_units as minimum of max_dua and max_res_units
UPDATE urbansim_output.res_capacity_ludu2015_to_sr13
SET addl_units =
minimum_of_max_dua_units_rounded_and_max_res_units - buildings_res_units
WHERE jurisdiction_id NOT IN (14,19) and scenario_id = 1 and 
parcel_id NOT IN (SELECT ludu2015_parcel_id from ref.sr13_capacity sr13);

	
--set negative capacity to zero
UPDATE urbansim_output.res_capacity_ludu2015_to_sr13
SET addl_units = 0
WHERE addl_units < 0 and jurisdiction_id NOT IN (14,19) and scenario_id = 1;


-- set scheduled dev true for parcels in scheduled development
UPDATE urbansim_output.res_capacity_ludu2015_to_sr13
SET scheduled_development = TRUE
WHERE parcel_id IN (SELECT parcel_id FROM urbansim.scheduled_development_parcels) and jurisdiction_id NOT IN (14,19) and scenario_id = 1;


-- set siteid for scheduled de
UPDATE urbansim_output.res_capacity_ludu2015_to_sr13 s2
SET siteid_sched_dev = subquery.siteid
FROM (SELECT parcel_id, siteid
	FROM urbansim.scheduled_development_parcels) AS subquery
WHERE s2.parcel_id = subquery.parcel_id and jurisdiction_id NOT IN (14,19) and scenario_id = 1;


--set addl_units to zero for scheduled dev
UPDATE urbansim_output.res_capacity_ludu2015_to_sr13
SET addl_units = 0
WHERE scheduled_development = TRUE and jurisdiction_id NOT IN (14,19) and scenario_id = 1;


--set addl_units for sched dev (all units to one parcel in the sched dev)
UPDATE urbansim_output.res_capacity_ludu2015_to_sr13 s2
SET addl_units = subquery.units_per_parcel
FROM (
WITH parcels_one AS (
SELECT siteid_sched_dev, MIN(parcel_id) as parcelid
FROM urbansim_output.res_capacity_ludu2015_to_sr13
WHERE siteid_sched_dev > 0
GROUP BY siteid_sched_dev)
SELECT os.parcelid, (COALESCE(sfu,0) + COALESCE(mfu,0)) AS units_per_parcel
FROM urbansim.scheduled_development sd
JOIN parcels_one os
ON os.siteid_sched_dev = sd."siteID"
) AS subquery
WHERE s2.parcel_id = subquery.parcelid and jurisdiction_id NOT IN (14,19) and scenario_id = 1;
