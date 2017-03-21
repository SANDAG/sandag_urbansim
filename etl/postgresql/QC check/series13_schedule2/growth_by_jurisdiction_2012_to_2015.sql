SELECT jurisdiction_id,
       schedule_id,
       SUM(sr13_cap_hs_with_negatives) as with_neg, 
       SUM(sr13_cap_hs_growth_adjusted) as growth_adj, 
       SUM(addl_units) as addl_units, 
       sum(sr13_cap_hs_with_negatives) - sum(addl_units) as growth 
  FROM urbansim_output.res_capacity
  WHERE not scheduled_development and source_zoning_schedule_id = 2 and sr13_cap_hs_with_negatives >0 and schedule_id = 2 
  GROUP BY jurisdiction_id, schedule_id 
  ORDER BY jurisdiction_id, schedule_id
