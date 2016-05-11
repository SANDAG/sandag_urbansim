IF OBJECT_ID('urbansim.fee_schedule') IS NOT NULL
  DROP TABLE urbansim.fee_schedule
GO

CREATE TABLE urbansim.fee_schedule(
  fee_schedule_id int NOT NULL
  ,development_type_id smallint NOT NULL
  ,development_fee_per_unit_space_initial float NOT NULL
)

INSERT INTO urbansim.fee_schedule (fee_schedule_id, development_type_id, development_fee_per_unit_space_initial)
SELECT
  fee_schedule_id
  ,development_type_id
  ,ROUND(AVG(development_fee_per_unit_space_initial),2) as [development_fee_per_unit_space_initia]
FROM
  input.fee_schedule
GROUP BY
  fee_schedule_id, development_type_id
ORDER BY
  1,2