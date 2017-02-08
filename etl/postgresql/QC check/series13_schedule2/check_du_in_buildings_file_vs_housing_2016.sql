SELECT jurisdiction_id, 
       sum(buildings_res_units) buildings_res_units 
FROM urbansim_output.res_capacity_ludu2015_to_sr13
GROUP BY jurisdiction_id 
ORDER BY jurisdiction_id  
