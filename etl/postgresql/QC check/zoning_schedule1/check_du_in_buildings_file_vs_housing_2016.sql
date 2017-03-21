SELECT jurisdiction_id, 
       sum(buildings_res_units) buildings_res_units 
FROM urbansim_output.res_capacity_zoning
GROUP BY jurisdiction_id 
ORDER BY jurisdiction_id  