
WITH zaia AS (
SELECT jurisdiction_id,
       schedule_id, 
       COUNT(parcel_id) as count_sr13_parcel_ids_wo_sched_dev,
       SUM(buildings_res_units) as sr13_res_units_wo_sched_dev,
       SUM(addl_units) as sr13_addl_units_wo_sched_dev 
  FROM urbansim_output.res_capacity
  WHERE not scheduled_development and source_zoning_schedule_id = 1 and schedule_id = 1 
  GROUP BY jurisdiction_id, schedule_id), 

ka as (

SELECT jurisdiction_id,
       schedule_id,
       COUNT(parcel_id) as count_sr13_and_2015_parcel_ids,
       SUM(buildings_res_units) as sr13_and_new_parcel_ids_res_units,
       SUM(addl_units) as sr13_and_new_parcel_ids_addl_units 
  FROM urbansim_output.res_capacity w
  WHERE schedule_id = 1
  GROUP BY jurisdiction_id, schedule_id 
  ORDER BY jurisdiction_id, schedule_id), 

o as (

SELECT jurisdiction_id,
       schedule_id,
       COUNT(parcel_id) as count_sr13_parcel_ids,
       SUM(buildings_res_units) as sr13_res_units,
       SUM(addl_units) as sr13_addl_units
  FROM urbansim_output.res_capacity o
  WHERE source_zoning_schedule_id = 1 and schedule_id = 1
  GROUP BY jurisdiction_id, schedule_id 
  ORDER BY jurisdiction_id, schedule_id), 

luzia AS (
SELECT jurisdiction_id,
       schedule_id,
       COUNT(parcel_id) as count_sr13_parcel_ids_wo_sched_dev_w_capacity
  FROM urbansim_output.res_capacity
  WHERE not scheduled_development and source_zoning_schedule_id = 1 and addl_units > 0 and schedule_id = 1
  GROUP BY jurisdiction_id, schedule_id)
  

  SELECT ka.jurisdiction_id,
         ka.schedule_id,
  sr13_and_new_parcel_ids_res_units,
  sr13_res_units,
  sr13_res_units_wo_sched_dev,
  sr13_and_new_parcel_ids_addl_units,   
  sr13_addl_units,
  sr13_addl_units_wo_sched_dev,
  count_sr13_and_2015_parcel_ids,  
  count_sr13_parcel_ids,  
  count_sr13_parcel_ids_wo_sched_dev,
  count_sr13_parcel_ids_wo_sched_dev_w_capacity
 
  FROM zaia JOIN ka    ON zaia.jurisdiction_id = ka.jurisdiction_id
	    JOIN o     ON zaia.jurisdiction_id = o.jurisdiction_id 
	    JOIN luzia ON zaia.jurisdiction_id = luzia.jurisdiction_id 
  ORDER BY ka.jurisdiction_id, schedule_id 




