USE spacecore

/**1 IDENTIFY PARCELS WHERE EDD DATA HAS BEEN SITED, BUT NO BUILDING EXISTS IN OUR DATABASE **/
/**2 USING THE SQUARE FOOT PER EMPLOYEE DATA, DETERMINE THE NON-RESIDENTIAL SQUARE FOOTAGE NECESSARY TO SUPPORT THE EDD NUMBER OF EMPLOYEES **/
SELECT usp.parcel_id
	,usp.luz_id
	,usp.development_type_id
--NOT GROUPED
	,emp_adj emp
	,sqft.sqft_per_emp sqft
--GROUPED
	--,SUM(emp_adj) emp
	--,MAX(sqft.sqft_per_emp) sqft
FROM urbansim.parcels usp
	JOIN socioec_data.ca_edd.emp_2013 emp
	ON usp.parcel_id = emp.parcelId		--emp.shape.STWithin(usp.shape) = 1								--EDD DATA SITED
	JOIN [spacecore].[input].[sqft_per_emp_by_devType] sqft
	ON usp.luz_id = sqft.luz_id AND usp.development_type_id = sqft.development_type_id 
WHERE usp.parcel_id NOT IN (SELECT DISTINCT parcel_id FROM urbansim.buildings)							--NO BUILDING
--GROUP BY usp.parcel_id, usp.luz_id, usp.development_type_id 
ORDER BY usp.parcel_id, usp.luz_id, usp.development_type_id



/**3 USE PAR / COSTAR DATA, IF AVAILABLE, TO VALIDATE DERIVED NON-RESIDENTIAL SQUARE FOOTAGE **/

/**4 IDENTIFY THE NUMBER OF BUILDING STORIES BY DIVIDING THE DERIVED NON-RESIDENTIAL SQUARE FOOTAGE DIVIDED BY THE SYNTHESIZED, SETBACK-DERIVED FOOTPRINT AREA OF THE BUILDING **/

/**5 USE THE LODES CENSUS BLOCK LEVEL DATA TO ASSIGN JOBS TO THE TOTALITY BUILDINGS **/

/**6 IDENTIFY REMAINING NON-RESIDENTIAL, NON-VACANT PARCELS WITH NO BUILDINGS AND DETERMINE WORKFLOW TO SYNTHESIZE FLOOR SPACE ON THESE USE CASES **/


