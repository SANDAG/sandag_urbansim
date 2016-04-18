USE spacecore
GO

IF OBJECT_ID('urbansim.scheduled_development_event') IS NOT NULL
    DROP TABLE urbansim.scheduled_development_event
GO

CREATE TABLE urbansim.schdeduled_development_event (
  scheduled_development_event_id int not null primary key
  ,year_built smallint not null
  ,sqft_per_unit int not null
  ,residential_units int not null
  ,non_residential_sqft int not null
)
GO

INSERT INTO urbansim.schdeduled_development_event 
    (scheduled_development_event_id, year_built, 
    sqft_per_unit, residential_units, non_residential_sqft)
  SELECT
    siteid
    ,phase
    ,sqft_prunt
    ,res_unit
    ,nonres_sqf
  FROM
    input.site_spec_stg
GO