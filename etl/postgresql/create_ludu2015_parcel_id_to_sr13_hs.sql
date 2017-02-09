-- Table: urbansim_output.ludu2015_parcel_id_to_sr13_caphs

--DROP TABLE urbansim_output.res_capacity_ludu2015_to_sr13;

CREATE TABLE urbansim_output.res_capacity
(
  parcel_id integer NOT NULL,
  jurisdiction_id smallint,
  schedule_id integer,
  scheduled_development boolean,
  siteid_sched_dev integer,
  parcel_id_2015_to_sr13 character varying, --parcel to parcel, parcel and centroid to parcel, centroid to parcel, NULL (2015 parcel_id does not exist in sr13) 
  source_zoning_schedule_id integer, 
  zone character varying,
  zoning_id integer,
  parent_zoning_id integer,
  allowed_dev_type integer[],
  acres numeric,
  undevelopable_proportion numeric,
  developable_acres numeric,
  zoning_min_dua numeric,
  zoning_max_dua numeric,
  zoning_max_dua_units numeric,
  zoning_max_dua_units_rounded integer,
  zoning_max_res_units numeric,
  minimum_of_max_dua_units_rounded_and_max_res_units numeric,
  buildings_res_units numeric,
  ludu2015_du integer,
  sr13_du integer,
  sr13_cap_hs_growth_adjusted numeric,
  sr13_cap_hs_with_negatives integer,
  addl_units numeric,

  CONSTRAINT pk_res_capacity_ludu2015_to_sr13 PRIMARY KEY (parcel_id, schedule_id),
  CONSTRAINT fk_schedule_res_capacity FOREIGN KEY (schedule_id)  
  REFERENCES urbansim_output.schedule_res_capacity (schedule_id) 
)
