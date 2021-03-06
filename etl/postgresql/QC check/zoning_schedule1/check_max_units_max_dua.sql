﻿SELECT parcel_id, 
       jurisdiction_id, 
       schedule_id, 
       --scheduled_development, 
       --siteid_sched_dev, 
       --parcel_id_2015_to_sr13, 
       --source_zoning_schedule_id, 
       zone, 
       zoning_id, 
       --parent_zoning_id, 
       allowed_dev_type, 
       acres, 
       undevelopable_proportion, 
       developable_acres, 
       --zoning_min_dua, 
       zoning_max_dua, 
       --zoning_max_dua_units, 
       zoning_max_dua_units_rounded, 
       zoning_max_res_units, 
       minimum_of_max_dua_units_rounded_and_max_res_units, 
       --buildings_res_units 
       --ludu2015_du, 
       --sr13_du, 
       --sr13_cap_hs_growth_adjusted, 
       --sr13_cap_hs_with_negatives, 
       addl_units
  FROM urbansim_output.res_capacity

  WHERE schedule_id = 1 and zoning_max_dua IS NULL  and  zoning_max_res_units IS NULL and allowed_dev_type IS NULL
  --WHERE schedule_id = 1 and zoning_max_dua >0 and      zoning_max_res_units IS NULL;
  --WHERE schedule_id = 1 and zoning_max_dua IS NULL and zoning_max_res_units >0;
  ORDER BY jurisdiction_id
