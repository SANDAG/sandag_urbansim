USE spacecore

/** ADD AREA AND LUZ **/
--ADD AREA
ALTER TABLE urbansim.buildings
ADD shape_area numeric(38,19)

UPDATE
    urbansim.buildings 
SET
    shape_area = shape.STArea()
WHERE
	[data_source] = 'SANDAG BLDG FOOTPRINT'

--SPATIAL SET LUZ_ID
ALTER TABLE urbansim.buildings
ADD luz_id int

UPDATE
    usb 
SET
    usb.luz_id = luz.luz_id
FROM
    urbansim.buildings usb
LEFT JOIN (SELECT zone as luz_id, shape 
			FROM data_cafe.ref.geography_zone z 
			INNER JOIN data_cafe.ref.geography_type t 
			ON z.geography_type_id = t.geography_type_id 
			WHERE t.geography_type = 'luz') luz 
ON usb.shape.STCentroid().STIntersects(luz.shape) = 1


/** CHECK RECORDS **/
SELECT [luz_id], [development_type_id], [shape_area]
FROM [urbansim].[buildings]
WHERE [data_source] = 'SANDAG BLDG FOOTPRINT'
AND [development_type_id] = 19
AND [residential_units] = 1
ORDER BY [luz_id], [development_type_id], [shape_area]


/** CALCULATE MEDIAN FOOTPRINT BY LUZ, FOR SINGLE FAMILY ONE UNIT **/
SELECT DISTINCT [luz_id], PERCENTILE_CONT(.5) WITHIN GROUP(ORDER BY [shape_area]) OVER(PARTITION BY [luz_id]) Median
FROM [urbansim].[buildings]
WHERE [data_source] = 'SANDAG BLDG FOOTPRINT'	--ACTUAL FOOTPRINTS
AND [development_type_id] = 19					--SINGLE FAMILY DETACHED RESIDENTIAL
AND [residential_units] = 1						--ONE UNIT
ORDER BY [luz_id]								--PARTITION Y LUZ_ID


--TODO
/** CALCULATE MEDIAN FOOTPRINT BY LUZ, FOR OFFICE **/
SELECT DISTINCT [luz_id], PERCENTILE_CONT(.5) WITHIN GROUP(ORDER BY [shape_area]) OVER(PARTITION BY [luz_id]) Median
FROM [urbansim].[buildings]
WHERE [data_source] = 'SANDAG BLDG FOOTPRINT'
AND [development_type_id] = 4
--AND [residential_units] = 1
ORDER BY [luz_id]
