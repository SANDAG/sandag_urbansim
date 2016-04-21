USE spacecore
GO

IF OBJECT_ID('urbansim.scheduled_development_event') IS NOT NULL
    DROP TABLE urbansim.scheduled_development_event
GO

CREATE TABLE urbansim.scheduled_development_event (
  scheduled_development_event_id int not null primary key
  ,parcel_id int not null
  ,development_type_id int not null
  ,year_built smallint not null
  ,sqft_per_unit int not null
  ,residential_units int not null
  ,non_residential_sqft int not null
  ,stories int not null
  ,improvement_value int not null
  ,res_price_per_sqft int not null
  ,nonres_rent_per_sqft int not null
)
GO

INSERT INTO urbansim.scheduled_development_event 
    (scheduled_development_event_id, parcel_id, development_type_id
	,year_built, sqft_per_unit, residential_units, non_residential_sqft
	,stories,improvement_value, res_price_per_sqft, nonres_rent_per_sqft)
  SELECT
    siteid
    ,p.parcel_id
	,placetype
	,phase
    ,sqft_prunt
    ,res_unit
    ,nonres_sqf
	,CASE WHEN avg_story < 1 THEN 1 ELSE avg_story END as stories
	,0 as improvement_value
	,0 as res_price_per_sqft
	,0 as nonres_rent_per_sqft
  FROM
    input.site_spec_stg
    INNER JOIN urbansim.parcels p ON geom.STCentroid().STIntersects(p.shape) = 1
GO