SELECT parcel_id, jurisdiction_id, schedule_id,
--scenario_id, scheduled_development, 
--siteid_sched_dev, 
--source_zoning_schedule_id, 
zone, 
--zoning_id, 
--parent_zoning_id, allowed_dev_type, acres, undevelopable_proportion, 
--developable_acres, zoning_min_dua, 
--zoning_max_dua, 
       zoning_max_dua_units, 
       zoning_max_dua_units_rounded, 
       zoning_max_res_units, 
       minimum_of_max_dua_units_rounded_and_max_res_units, 
       buildings_res_units 
--addl_units
FROM urbansim_output.res_capacity
WHERE parcel_id in(550994,5077223,29176,171017,400549,5100046,34114,
38634,5048945,195731,242210,318124,412735,29800,5062753,748530,
341743,379559,122864,502343,260396,255314,5040050,1555455,21515,
690805,125128,44344,97440,632544,5068968,5273101,5002467,5001463,
382190,5008781,5284438,203366)and schedule_id = 1 
ORDER BY jurisdiction_id, parcel_id



