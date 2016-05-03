USE spacecore

/**1 IDENTIFY PARCELS WHERE EDD DATA HAS BEEN SITED, BUT NO BUILDING EXISTS IN OUR DATABASE **/
/**2 USING THE SQUARE FOOT PER EMPLOYEE DATA, DETERMINE THE NON-RESIDENTIAL SQUARE FOOTAGE NECESSARY TO SUPPORT THE EDD NUMBER OF EMPLOYEES **/
SELECT usp.parcel_id
	,usp.luz_id
	,usp.development_type_id
	--,ref.[development_type_id] devtype_from_lu															--LCKEY LEVEL LU TO DEVTYPE
--NOT GROUPED
	--,emp.emp_adj emp
	--,sqft.sqft_per_emp sqft_fromdev
	--,sqft_fromlu.sqft_per_emp sqft_fromlu																	--LCKEY LEVEL LU TO DEVTYPE
--GROUPED
	,SUM(emp_adj) emp
	,MAX(sqft.sqft_per_emp) sqft
--REQUIRED
	,(SUM(emp_adj)*MAX(sqft.sqft_per_emp)) emp_sqft
FROM urbansim.parcels usp
	JOIN socioec_data.ca_edd.emp_2013 emp
	ON usp.parcel_id = emp.parcelId		--emp.shape.STWithin(usp.shape) = 1								--EDD DATA SITED
	JOIN [ref].[development_type_lu_code] ref
	ON emp.lu = ref.[lu_code]
	JOIN [spacecore].[input].[sqft_per_emp_by_devType] sqft
	ON usp.luz_id = sqft.luz_id AND usp.development_type_id = sqft.development_type_id 
	--JOIN [spacecore].[input].[sqft_per_emp_by_devType] sqft_fromlu
	--ON usp.luz_id = sqft_fromlu.luz_id AND ref.[development_type_id] = sqft_fromlu.development_type_id	--LCKEY LEVEL LU TO DEVTYPE
WHERE usp.parcel_id NOT IN (SELECT DISTINCT parcel_id FROM urbansim.buildings)							--NO BUILDING
--AND usp.development_type_id != ref.[development_type_id]													--PARCEL LEVEL DEVTYPE != LCKEY LEVEL LU TO DEVTYPE
GROUP BY usp.parcel_id, usp.luz_id, usp.development_type_id 
ORDER BY usp.parcel_id, usp.luz_id, usp.development_type_id



/**3 USE PAR / COSTAR DATA, IF AVAILABLE, TO VALIDATE DERIVED NON-RESIDENTIAL SQUARE FOOTAGE **/

/**4 IDENTIFY THE NUMBER OF BUILDING STORIES BY DIVIDING THE DERIVED NON-RESIDENTIAL SQUARE FOOTAGE DIVIDED BY THE SYNTHESIZED, SETBACK-DERIVED FOOTPRINT AREA OF THE BUILDING **/

/**5 USE THE LODES CENSUS BLOCK LEVEL DATA TO ASSIGN JOBS TO THE TOTALITY BUILDINGS **/

/**6 IDENTIFY REMAINING NON-RESIDENTIAL, NON-VACANT PARCELS WITH NO BUILDINGS AND DETERMINE WORKFLOW TO SYNTHESIZE FLOOR SPACE ON THESE USE CASES **/


