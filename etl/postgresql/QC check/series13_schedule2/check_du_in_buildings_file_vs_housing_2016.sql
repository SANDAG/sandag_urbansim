SELECT jurisdiction_id, 
	schedule_id, 
       sum(buildings_res_units) buildings_res_units 
FROM urbansim_output.res_capacity
GROUP BY jurisdiction_id, schedule_id
ORDER BY jurisdiction_id, schedule_id  
