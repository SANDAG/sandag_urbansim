INSERT INTO staging.schedule2_sr13 (
	parcel_id, jurisdiction_id, source_zoning_schedule_id, zone, 
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
WHERE 	zp.zoning_schedule_id = 2 AND zoning.jurisdiction_id = 8
GROUP BY lookup_zoning_id, zp.parcel_id),
residential_units AS (
SELECT 	p.parcel_id, coalesce(SUM(b.residential_units),0) AS current_units
FROM	urbansim.buildings b
RIGHT JOIN urbansim.parcels p 
ON 	b.parcel_id = p.parcel_id
WHERE 	p.jurisdiction_id = 8
GROUP BY p.parcel_id)
SELECT 	parcels.parcel_id, parcels.jurisdiction_id, zoning.zoning_schedule_id, zoning.zone, 
	zoning.zoning_id, zoning.parent_zoning_id, dev.allowed_dev_type,
	round(CAST(parcels.parcel_acres as numeric), 5) as acres, round(parcels.proportion_undevelopable,5) as proportion_undevelopable, 
	round(CAST((1 - COALESCE(parcels.proportion_undevelopable,0)) * parcels.parcel_acres as numeric),5) AS acres_constrained,
	zoning.min_dua, zoning.max_dua,  
	round(CAST((1 - COALESCE(parcels.proportion_undevelopable,0)) * parcels.parcel_acres * zoning.max_dua as numeric),1) AS max_dua_units,
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
WHERE 	zp.zoning_schedule_id = 2 AND zoning.jurisdiction_id = 8);


-- calculate minimum_of_max_dua_units_and_max_res_units for parcels not in series 13
UPDATE staging.schedule2_sr13
SET minimum_of_max_dua_units_and_max_res_units = (
CASE 	
WHEN round(coalesce(max_dua_units, 2147483647)) < coalesce(max_res_units, 2147483647)  Then round(coalesce(max_dua_units, 2147483647))
WHEN coalesce(max_res_units, 2147483647) < round(coalesce(max_dua_units, 2147483647)) Then coalesce(max_res_units, 2147483647)
WHEN max_dua_units IS NULL AND max_res_units is NULL Then 0
ELSE coalesce(max_res_units, 2147483647)
END)
WHERE jurisdiction_id = 8 and parcel_id NOT IN (SELECT parcel_id from staging.sr13_capacity sr13);


-- set addl_units to cap_hs
UPDATE staging.schedule2_sr13
SET addl_units = cap_hs
WHERE jurisdiction_id = 8;


-- for parcels not in series 13, calc addl_units as minimum of max_dua and max_res_units
UPDATE staging.schedule2_sr13
SET addl_units =
minimum_of_max_dua_units_and_max_res_units - current_units
WHERE jurisdiction_id = 8 and parcel_id NOT IN (SELECT parcel_id from staging.sr13_capacity sr13);


--set negative capacity to zero
UPDATE staging.schedule2_sr13
SET addl_units = 0
WHERE addl_units < 0 and jurisdiction_id = 8;


-- add scheduled dev column
UPDATE staging.schedule2_sr13
SET scheduled_development = FALSE where jurisdiction_id = 8;


-- set scheduled dev true for parcels in scheduled development
UPDATE staging.schedule2_sr13
SET scheduled_development = TRUE
WHERE parcel_id IN (SELECT parcel_id FROM urbansim.scheduled_development_parcels) and jurisdiction_id = 8;


-- set siteid for scheduled dev
UPDATE staging.schedule2_sr13 s2
SET siteid_sched_dev = subquery.siteid
FROM (SELECT parcel_id, siteid
	FROM urbansim.scheduled_development_parcels) AS subquery
WHERE s2.parcel_id = subquery.parcel_id and jurisdiction_id = 8;


--set addl_units to zero for scheduled dev
UPDATE staging.schedule2_sr13
SET addl_units = 0
WHERE scheduled_development = TRUE and jurisdiction_id = 8;


--set addl_units for sched dev (all units to one parcel in the sched dev)
UPDATE staging.schedule2_sr13 s2
SET addl_units = subquery.units_per_parcel
FROM (
WITH parcels_one AS (
SELECT siteid_sched_dev, MIN(parcel_id) as parcelid
FROM staging.schedule2_sr13
WHERE siteid_sched_dev > 0
GROUP BY siteid_sched_dev)
SELECT os.parcelid, (COALESCE(sfu,0) + COALESCE(mfu,0)) AS units_per_parcel
FROM urbansim.scheduled_development sd
JOIN parcels_one os
ON os.siteid_sched_dev = sd."siteID"
) AS subquery
WHERE s2.parcel_id = subquery.parcelid and jurisdiction_id = 8;
