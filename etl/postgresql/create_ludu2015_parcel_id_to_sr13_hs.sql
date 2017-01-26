-- Table: urbansim_output.ludu2015_parcel_id_to_sr13_caphs

-- DROP TABLE urbansim_output.ludu2015_parcel_id_to_sr13_caphs;

CREATE TABLE urbansim_output.ludu2015_parcel_id_to_sr13_caphs
(
  parcel_id integer NOT NULL,
  jurisdiction_id smallint,
  scenario_id integer,
  scheduled_development boolean,
  siteid_sched_dev integer,
  parcel_id_2015_to_sr13 character varying, --parcel to parcel, parcel and centroid to parcel, centroid to parcel, NULL (2015 parcel_id does not exist in sr13) 
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
  sr13_cap_hs_negatives_to_zeros numeric,
  sr13_cap_hs integer,
  growth_adjusted_sr13_cap_hs integer,
  aggregated_addl_units numeric,

  CONSTRAINT uk_ludu2015_parcel_id_to_sr13_caphs UNIQUE (parcel_id, scenario_id)
)
