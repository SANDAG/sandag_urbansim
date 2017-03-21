SELECT sum(buildings_res_units) as buildings_res_units, 
       sum(sr13_du) as sr13_du, 
       sum(buildings_res_units) - sum(sr13_du) as difference
FROM urbansim_output.res_capacity
WHERE schedule_id = 2;
