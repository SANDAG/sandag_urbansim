USE spacecore
GO

//** START DATA LOAD **//
INSERT INTO input.costar WITH (TABLOCK) (
	[property_id]
	,[amenities]
	,[anchor_tenants]
	,[architect_name]
	,[average_weighted_rent]
	,[avg_rent_direct_(industrial)]
	,[avg_rent_direct_(office)]
	,[avg_rent_direct_(retail)]
	,[avg_rent_sublet_(industrial)]
	,[avg_rent_sublet_(office)]
	,[avg_rent_sublet_(retail)]
	,[building_address]
	,[building_class]
	,[building_location]
	,[building_name]
	,[building_operating_expenses]
	,[building_park]
	,[building_status]
	,[building_tax_expenses]
	,[ceiling_height_range]
	,[city]
	,[closest_transit_stop]
	,[closest_transit_stop_dist_(mi)]
	,[closest_transit_stop_walk_time_(min)]
	,[column_spacing]
	,[construction_material]
	,[core_factor]
	,[county_name]
	,[cross_street]
	,[developer_name]
	,[direct_available_space]
	,[direct_services]
	,[direct_vacant_space]
	,[drive_ins]
	,[energy_star]
	,[exp_year]
	,[features]
	,[for_sale_price]
	,[for_sale_status]
	,[gas]
	,[heating]
	,[leed_certified]
	,[land_area_(ac)]
	,[last_sale_date]
	,[last_sale_price]
	,[latitude]
	,[leasing_company_address]
	,[leasing_company_city_state_zip]
	,[leasing_company_contact]
	,[leasing_company_fax]
	,[leasing_company_name]
	,[leasing_company_phone]
	,[longitude]
	,[market_name]
	,[max_building_contiguous_space]
	,[max_floor_contiguous_space]
	,[number_of_1_bedrooms]
	,[number_of_2_bedrooms]
	,[number_of_3_bedrooms]
	,[number_of_cranes]
	,[number_of_elevators]
	,[number_of_loading_docks]
	,[number_of_parking_spaces]
	,[number_of_stories]
	,[number_of_studios]
	,[number_of_units]
	,[office_space]
	,[ops_expense]
	,[ops_expense_per_sf]
	,[owner_address]
	,[owner_city_state_zip]
	,[owner_contact]
	,[owner_name]
	,[owner_phone]
	,[parking_ratio]
	,[percent_leased]
	,[power]
	,[primary_agent_name]
	,[property_manager_address]
	,[property_manager_city_state_zip]
	,[property_manager_contact]
	,[property_manager_name]
	,[property_manager_phone]
	,[propertytype]
	,[rail_lines]
	,[rentable_building_area]
	,[serial]
	,[services]
	,[sewer]
	,[smallest_available_space]
	,[sprinklers]
	,[state]
	,[sublet_available_space]
	,[sublet_services]
	,[sublet_vacant_space]
	,[submarket_cluster]
	,[submarket_name]
	,[tax_year]
	,[taxes_per_sf]
	,[taxes_total]
	,[total_available_space_(sf)]
	,[total_new_space_(sf)]
	,[total_relet_space_(sf)]
	,[total_sublet_space_(sf)]
	,[total_vacant_avail_relet_space_(sf)]
	,[total_vacant_avail_sublet_space_(sf)]
	,[total_vacant_available]
	,[typical_floor_size]
	,[water]
	,[year_built]
	,[year_renovated]
	,[zip]
	,[zoning]
	,[$price/unit]
	,[%_1_bed]
	,[%_2_bed]
	,[%_3_bed]
	,[%_4_bed]
	,[%_studios]
	,[affordable_type]
	,[anchor_gla]
	,[avg_asking/sf]
	,[avg_asking/unit]
	,[avg_concessions_%]
	,[avg_effective/sf]
	,[avg_effective/unit]
	,[avg_unit_sf]
	,[cap_rate]
	,[days_on_market]
	,[four_bedroom_asking_rent/sf]
	,[four_bedroom_asking_rent/unit]
	,[four_bedroom_avg_sf]
	,[four_bedroom_concessions_%]
	,[four_bedroom_effective_rent/sf]
	,[four_bedroom_effective_rent/unit]
	,[four_bedroom_vacancy_%]
	,[four_bedroom_vacant_units]
	,[market_segment]
	,[number_of_4_bedrooms]
	,[one_bedroom_asking_rent/sf]
	,[one_bedroom_asking_rent/unit]
	,[one_bedroom_avg_sf]
	,[one_bedroom_concessions_%]
	,[one_bedroom_effective_rent/sf]
	,[one_bedroom_effective_rent/unit]
	,[one_bedroom_vacancy_%]
	,[one_bedroom_vacant_units]
	,[parcel_number_1(min)]
	,[parcel_number_2(max)]
	,[parking_spaces/unit]
	,[rent_type]
	,[secondary_type]
	,[star_rating]
	,[studio_asking_rent/sf]
	,[studio_asking_rent/unit]
	,[studio_avg_sf]
	,[studio_concessions_%]
	,[studio_effective_rent/sf]
	,[studio_effective_rent/unit]
	,[studio_vacancy_%]
	,[studio_vacant_units]
	,[style]
	,[three_bedroom_asking_rent/sf]
	,[three_bedroom_asking_rent/unit]
	,[three_bedroom_avg_sf]
	,[three_bedroom_concessions_%]
	,[three_bedroom_effective_rent/sf]
	,[three_bedroom_effective_rent/unit]
	,[three_bedroom_vacancy_%]
	,[three_bedroom_vacant_units]
	,[total_buildings]
	,[two_bedroom_asking_rent/sf]
	,[two_bedroom_asking_rent/unit]
	,[two_bedroom_avg_sf]
	,[two_bedroom_concessions_%]
	,[two_bedroom_effective_rent/sf]
	,[two_bedroom_effective_rent/unit]
	,[two_bedroom_vacancy_%]
	,[two_bedroom_vacant_units]
	,[vacancy_%]
	,[centroid]
	)
SELECT 
	[propertyid]
	,[amenities]
	,[anchor tenants]
	,[architect name]
	,CASE WHEN [average weighted rent] = '-' THEN NULL
		WHEN ISNUMERIC([average weighted rent]) = 1 
			THEN CAST([average weighted rent] AS numeric (38,8))
		ELSE NULL
		END AS [average weighted rent]
	,CASE WHEN [avg rent_direct (industrial)] = '-' THEN NULL
		WHEN ISNUMERIC([avg rent_direct (industrial)]) = 1 
			THEN CAST([avg rent_direct (industrial)] AS numeric (38,8))
		ELSE NULL
		END AS [avg rent_direct (industrial)]
	,CASE WHEN [avg rent_direct (office)] = '-' THEN NULL
		WHEN ISNUMERIC([avg rent_direct (office)]) = 1 
			THEN CAST([avg rent_direct (office)] AS numeric (38,8))
		ELSE NULL
		END AS [avg rent_direct (office)]
	,CASE WHEN [avg rent_direct (retail)] = '-' THEN NULL
		WHEN ISNUMERIC([avg rent_direct (retail)]) = 1 
			THEN CAST([avg rent_direct (retail)] AS numeric (38,8))
		ELSE NULL
		END AS [avg rent_direct (retail)]
	,CASE WHEN [avg rent_sublet (industrial)] = '-' THEN NULL
		WHEN ISNUMERIC([avg rent_sublet (industrial)]) = 1 
			THEN CAST([avg rent_sublet (industrial)] AS numeric (38,8))
		ELSE NULL
		END AS [avg rent_sublet (industrial)]
	,CASE WHEN [avg rent_sublet (office)] = '-' THEN NULL
		WHEN ISNUMERIC([avg rent_sublet (office)]) = 1 
			THEN CAST([avg rent_sublet (office)] AS numeric (38,8))
		ELSE NULL
		END AS [avg rent_sublet (office)]
	,CASE WHEN [avg rent_sublet (retail)] = '-' THEN NULL
		WHEN ISNUMERIC([avg rent_sublet (retail)]) = 1 
			THEN CAST([avg rent_sublet (retail)] AS numeric (38,8))
		ELSE NULL
		END AS [avg rent_sublet (retail)]
	,[building address]
	,[building class]
	,[building location]
	,[building name]
	,[building operating expenses]
	,[building park]
	,[building status]
	,[building tax expenses]
	,[ceiling height range]
	,[city]
	,[closest transit stop]
	,CASE WHEN ISNUMERIC([closest transit stop dist (mi)]) = 1 
			THEN CAST([closest transit stop dist (mi)] AS numeric (38,8))
		ELSE NULL
		END AS [closest transit stop dist (mi)]
	,CASE WHEN ISNUMERIC([closest transit stop walk time (min)]) = 1 
			THEN CAST([closest transit stop walk time (min)] AS numeric (38,8))
		ELSE NULL
		END AS [closest transit stop walk time (min)]
	,[column spacing]
	,[construction material]
	,CASE WHEN ISNUMERIC([core factor]) = 1 
			THEN CAST([core factor] AS numeric (38,8))
		ELSE NULL
		END AS [core factor]
	,[county name]
	,[cross street]
	,[developer name]
	,CASE WHEN ISNUMERIC([direct available space]) = 1 
			THEN CAST([direct available space] AS int)
		ELSE NULL
		END AS [direct available space]
	,[direct services]
	,[direct vacant space]
	,[drive ins]
	,[energy star]
	,[exp year]
	,[features]
	,[for sale price]
	,[for sale status]
	,[gas]
	,[heating]
	,[leed certified]
	,CASE WHEN ISNUMERIC([land area (ac)]) = 1 
			THEN CAST([land area (ac)] AS numeric (38,8))
		ELSE NULL
		END AS [land area (ac)]
	,CASE WHEN ISNUMERIC([last sale date]) = 1 
			THEN CAST([last sale date] AS smalldatetime)
		ELSE NULL
		END AS [last sale date]
	,CASE WHEN ISNUMERIC([last sale price]) = 1 
			THEN CAST(REPLACE([last sale price],',','') AS numeric (38,8))
		ELSE NULL
		END AS [last sale price]
	,CASE WHEN ISNUMERIC([latitude]) = 1 
			THEN CAST([latitude] AS numeric (38,8))
		ELSE NULL
		END AS [latitude]
	,[leasing company address]
	,[leasing company city state zip]
	,[leasing company contact]
	,CASE WHEN ISNUMERIC([leasing company fax]) = 1 
			THEN CAST([leasing company fax] AS bigint)
		ELSE NULL
		END AS [leasing company fax]
	,[leasing company name]
	,CASE WHEN ISNUMERIC([leasing company phone]) = 1 
			THEN CAST([leasing company phone] AS bigint)
		ELSE NULL
		END AS [leasing company phone]
	,CASE WHEN ISNUMERIC([longitude]) = 1 
			THEN CAST([longitude] AS numeric (38,8))
		ELSE NULL
		END AS [longitude]
	,[market name]
	,CASE WHEN ISNUMERIC([max building contiguous space]) = 1 
			THEN CAST([max building contiguous space] AS numeric (38,8))
		ELSE NULL
		END AS [max building contiguous space]
	,CASE WHEN ISNUMERIC([max floor contiguous space]) = 1 
			THEN CAST([max floor contiguous space] AS numeric (38,8))
		ELSE NULL
		END AS [max floor contiguous space]
	,CASE WHEN ISNUMERIC([number of 1 bedrooms]) = 1 
			THEN CAST([number of 1 bedrooms] AS bigint)
		ELSE NULL
		END AS [number of 1 bedrooms]
	,CASE WHEN ISNUMERIC([number of 2 bedrooms]) = 1 
			THEN CAST([number of 2 bedrooms] AS bigint)
		ELSE NULL
		END AS [number of 2 bedrooms]
	,CASE WHEN ISNUMERIC([number of 3 bedrooms]) = 1 
			THEN CAST([number of 3 bedrooms] AS bigint)
		ELSE NULL
		END AS [number of 3 bedrooms]
	,CASE WHEN ISNUMERIC([number of cranes]) = 1 
			THEN CAST([number of cranes] AS bigint)
		ELSE NULL
		END AS [number of cranes]
	,CASE WHEN ISNUMERIC([number of elevators]) = 1 
			THEN CAST([number of elevators] AS bigint)
		ELSE NULL
		END AS [number of elevators]
	,CASE WHEN ISNUMERIC([number of loading docks]) = 1 
			THEN CAST([number of loading docks] AS bigint)
		ELSE NULL
		END AS [number of loading docks]
	,CASE WHEN ISNUMERIC([number of parking spaces]) = 1 
			THEN CAST([number of parking spaces] AS int)
		ELSE NULL
		END AS [number of parking spaces]
	,CASE WHEN ISNUMERIC([number of stories]) = 1 
			THEN CAST([number of stories] AS smallint)
		ELSE NULL
		END AS [number of stories]
	,CASE WHEN ISNUMERIC([number of studios]) = 1 
			THEN CAST([number of studios] AS smallint)
		ELSE NULL
		END AS [number of studios]
	,CASE WHEN ISNUMERIC([number of units]) = 1 
			THEN CAST([number of units] AS int)
		ELSE NULL
		END AS [number of units]

	,[office space]
	,CASE WHEN ISNUMERIC([ops expense]) = 1 
			THEN CAST([ops expense] AS numeric (38,8))
		ELSE NULL
		END AS [ops expense]
	,CASE WHEN ISNUMERIC([ops expense per sf]) = 1 
			THEN CAST([ops expense per sf] AS numeric (38,8))
		ELSE NULL
		END AS [ops expense per sf]
	,[owner address]
	,[owner city state zip]
	,[owner contact]
	,[owner name]
	,[owner phone]
	,CASE WHEN ISNUMERIC([parking ratio]) = 1 
			THEN CAST([parking ratio] AS numeric (38,8))
		ELSE NULL
		END AS [parking ratio]
	,CASE WHEN ISNUMERIC([percent leased]) = 1 
			THEN CAST([percent leased] AS numeric (38,8))
		ELSE NULL
		END AS [percent leased]
	,[power]
	,[primary agent name]
	,[property manager address]
	,[property manager city state zip]
	,[property manager contact]
	,[property manager name]
	,[property manager phone]
	,[propertytype]
	,[rail lines]
	,CASE WHEN ISNUMERIC([rentable building area]) = 1 
			THEN CAST([rentable building area] AS int)
		ELSE NULL
		END AS [rentable building area]
	,[serial]
	,[services]
	,[sewer]
	,CASE WHEN ISNUMERIC([smallest available space]) = 1 
			THEN CAST([smallest available space] AS numeric (38,8))
		ELSE NULL
		END AS [smallest available space]
	,[sprinklers]
	,[state]
	,CASE WHEN ISNUMERIC([sublet available space]) = 1 
			THEN CAST([sublet available space] AS numeric (38,8))
		ELSE NULL
		END AS [sublet available space]
	,[sublet services]
	,[sublet vacant space]
	,[submarket cluster]
	,[submarket name]
	,CASE WHEN ISNUMERIC([tax year]) = 1 
			THEN CAST([tax year] AS int)
		ELSE NULL
		END AS [tax year]
	,CASE WHEN ISNUMERIC([taxes per sf]) = 1 
			THEN CAST([taxes per sf] AS numeric (38,8))
		ELSE NULL
		END AS [taxes per sf]
	,CASE WHEN ISNUMERIC([taxes total]) = 1 
			THEN CAST([taxes total] AS numeric (38,8))
		ELSE NULL
		END AS [taxes total]
	,CASE WHEN ISNUMERIC([total available space (sf)]) = 1 
			THEN CAST([total available space (sf)] AS int)
		ELSE NULL
		END AS [total available space (sf)]
	,CASE WHEN ISNUMERIC([total new space (sf)]) = 1 
			THEN CAST([total new space (sf)] AS int)
		ELSE NULL
		END AS [total new space (sf)]
	,CASE WHEN ISNUMERIC([total relet space (sf)]) = 1 
			THEN CAST([total relet space (sf)] AS int)
		ELSE NULL
		END AS [total relet space (sf)]
	,CASE WHEN ISNUMERIC([total sublet space (sf)]) = 1 
			THEN CAST([total sublet space (sf)] AS int)
		ELSE NULL
		END AS [total sublet space (sf)]
	,CASE WHEN ISNUMERIC([total vacant avail relet space (sf)]) = 1 
			THEN CAST([total vacant avail relet space (sf)] AS int)
		ELSE NULL
		END AS [total vacant avail relet space (sf)]
	,CASE WHEN ISNUMERIC([total vacant avail sublet space (sf)]) = 1 
			THEN CAST([total vacant avail sublet space (sf)] AS int)
		ELSE NULL
		END AS [total vacant avail sublet space (sf)]
	,CASE WHEN ISNUMERIC([total vacant available]) = 1 
			THEN CAST([total vacant available] AS int)
		ELSE NULL
		END AS [total vacant available]
	,CASE WHEN ISNUMERIC([typical floor size]) = 1 
			THEN CAST([typical floor size] AS int)
		ELSE NULL
		END AS [typical floor size]
	,[water]
	,CASE WHEN ISNUMERIC([year built]) = 1 
			THEN CAST([year built] AS int)
		ELSE NULL
		END AS [year built]
	,CASE WHEN ISNUMERIC([year renovated]) = 1 
			THEN CAST([year renovated] AS int)
		ELSE NULL
		END AS [year renovated]
	,CASE WHEN ISNUMERIC([zip]) = 1 
			THEN CAST([zip] AS int)
		ELSE NULL
		END AS [zip]
	,[zoning]
	,CASE WHEN ISNUMERIC([$price/unit]) = 1 
			THEN CAST([$price/unit] AS numeric (38,8))
		ELSE NULL
		END AS [$price/unit]
	,CASE WHEN ISNUMERIC([% 1_bed]) = 1 
			THEN CAST([% 1_bed] AS numeric (38,8))
		ELSE NULL
		END AS [% 1_bed]
	,CASE WHEN ISNUMERIC([% 2_bed]) = 1 
			THEN CAST([% 2_bed] AS numeric (38,8))
		ELSE NULL
		END AS [% 2_bed]
	,CASE WHEN ISNUMERIC([% 3_bed]) = 1 
			THEN CAST([% 3_bed] AS numeric (38,8))
		ELSE NULL
		END AS [% 3_bed]
	,CASE WHEN ISNUMERIC([% 4_bed]) = 1 
			THEN CAST([% 4_bed] AS numeric (38,8))
		ELSE NULL
		END AS [% 4_bed]
	,CASE WHEN ISNUMERIC([% studios]) = 1 
			THEN CAST([% studios] AS numeric (38,8))
		ELSE NULL
		END AS [% studios]
	,[affordable type]
	,[anchor gla]
	,CASE WHEN ISNUMERIC([avg asking/sf]) = 1 
			THEN CAST([avg asking/sf] AS numeric (38,8))
		ELSE NULL
		END AS [avg asking/sf]
	,CASE WHEN ISNUMERIC([avg asking/unit]) = 1 
			THEN CAST([avg asking/unit] AS numeric (38,8))
		ELSE NULL
		END AS [avg asking/unit]
	,CASE WHEN ISNUMERIC([avg concessions %]) = 1 
			THEN CAST([avg concessions %] AS numeric (38,8))
		ELSE NULL
		END AS [avg concessions %]
	,CASE WHEN ISNUMERIC([avg effective/sf]) = 1 
			THEN CAST([avg effective/sf] AS numeric (38,8))
		ELSE NULL
		END AS [avg effective/sf]
	,CASE WHEN ISNUMERIC([avg effective/unit]) = 1 
			THEN CAST([avg effective/unit] AS numeric (38,8))
		ELSE NULL
		END AS [avg effective/unit]
	,CASE WHEN ISNUMERIC([avg unit sf]) = 1 
			THEN CAST([avg unit sf] AS numeric (38,8))
		ELSE NULL
		END AS [avg unit sf]
	,CASE WHEN ISNUMERIC([cap rate]) = 1 
			THEN CAST([cap rate] AS numeric (38,8))
		ELSE NULL
		END AS [cap rate]
	,CASE WHEN ISNUMERIC([days on market]) = 1 
			THEN CAST([days on market] AS numeric (38,8))
		ELSE NULL
		END AS [days on market]
	,CASE WHEN ISNUMERIC([four bedroom asking rent/sf]) = 1 
			THEN CAST([four bedroom asking rent/sf] AS numeric (38,8))
		ELSE NULL
		END AS [four bedroom asking rent/sf]
	,CASE WHEN ISNUMERIC([four bedroom asking rent/unit]) = 1 
			THEN CAST([four bedroom asking rent/unit] AS numeric (38,8))
		ELSE NULL
		END AS [four bedroom asking rent/unit]
	,CASE WHEN ISNUMERIC([four bedroom avg sf]) = 1 
			THEN CAST([four bedroom avg sf] AS numeric (38,8))
		ELSE NULL
		END AS [four bedroom avg sf]
	,CASE WHEN ISNUMERIC([four bedroom concessions %]) = 1 
			THEN CAST([four bedroom concessions %] AS numeric (38,8))
		ELSE NULL
		END AS [four bedroom concessions %]
	,CASE WHEN ISNUMERIC([four bedroom effective rent/sf]) = 1 
			THEN CAST([four bedroom effective rent/sf] AS numeric (38,8))
		ELSE NULL
		END AS [four bedroom effective rent/sf]
	,CASE WHEN ISNUMERIC([four bedroom effective rent/unit]) = 1 
			THEN CAST([four bedroom effective rent/unit] AS numeric (38,8))
		ELSE NULL
		END AS [four bedroom effective rent/unit]
	,CASE WHEN ISNUMERIC([four bedroom vacancy %]) = 1 
			THEN CAST([four bedroom vacancy %] AS numeric (38,8))
		ELSE NULL
		END AS [four bedroom vacancy %]
	,CASE WHEN ISNUMERIC([four bedroom vacant units]) = 1 
			THEN CAST([four bedroom vacant units] AS int)
		ELSE NULL
		END AS [four bedroom vacant units]
	,[market segment]
	,CASE WHEN ISNUMERIC([number of 4 bedrooms]) = 1 
			THEN CAST([number of 4 bedrooms] AS int)
		ELSE NULL
		END AS [number of 4 bedrooms]

	,CASE WHEN ISNUMERIC([one bedroom asking rent/sf]) = 1 
			THEN CAST([one bedroom asking rent/sf] AS numeric (38,8))
		ELSE NULL
		END AS [one bedroom asking rent/sf]
	,CASE WHEN ISNUMERIC([one bedroom asking rent/unit]) = 1 
			THEN CAST([one bedroom asking rent/unit] AS numeric (38,8))
		ELSE NULL
		END AS [one bedroom asking rent/unit]
	,CASE WHEN ISNUMERIC([one bedroom avg sf]) = 1 
			THEN CAST([one bedroom avg sf] AS numeric (38,8))
		ELSE NULL
		END AS [one bedroom avg sf]
	,CASE WHEN ISNUMERIC([one bedroom concessions %]) = 1 
			THEN CAST([one bedroom concessions %] AS numeric (38,8))
		ELSE NULL
		END AS [one bedroom concessions %]
	,CASE WHEN ISNUMERIC([one bedroom effective rent/sf]) = 1 
			THEN CAST([one bedroom effective rent/sf] AS numeric (38,8))
		ELSE NULL
		END AS [one bedroom effective rent/sf]
	,CASE WHEN ISNUMERIC([one bedroom effective rent/unit]) = 1 
			THEN CAST([one bedroom effective rent/unit] AS numeric (38,8))
		ELSE NULL
		END AS [one bedroom effective rent/unit]
	,CASE WHEN ISNUMERIC([one bedroom vacancy %]) = 1 
			THEN CAST([one bedroom vacancy %] AS numeric (38,8))
		ELSE NULL
		END AS [one bedroom vacancy %]
	,CASE WHEN ISNUMERIC([one bedroom vacant units]) = 1 
			THEN CAST([one bedroom vacant units] AS int)
		ELSE NULL
		END AS [one bedroom vacant units]
	,[parcel number 1(min)]
	,[parcel number 2(max)]
	,CASE WHEN ISNUMERIC([parking spaces/unit]) = 1 
			THEN CAST([parking spaces/unit] AS int)
		ELSE NULL
		END AS [parking spaces/unit]
	,[rent type]
	,[secondary type]
	,[star rating]
	,CASE WHEN ISNUMERIC([studio asking rent/sf]) = 1 
			THEN CAST([studio asking rent/sf] AS numeric (38,8))
		ELSE NULL
		END AS [studio asking rent/sf]
	,CASE WHEN ISNUMERIC([studio asking rent/unit]) = 1 
			THEN CAST([studio asking rent/unit] AS numeric (38,8))
		ELSE NULL
		END AS [studio asking rent/unit]
	,CASE WHEN ISNUMERIC([studio avg sf]) = 1 
			THEN CAST([studio avg sf] AS numeric (38,8))
		ELSE NULL
		END AS [studio avg sf]
	,CASE WHEN ISNUMERIC([studio concessions %]) = 1 
			THEN CAST([studio concessions %] AS numeric (38,8))
		ELSE NULL
		END AS [studio concessions %]
	,CASE WHEN ISNUMERIC([studio effective rent/sf]) = 1 
			THEN CAST([studio effective rent/sf] AS numeric (38,8))
		ELSE NULL
		END AS [studio effective rent/sf]
	,CASE WHEN ISNUMERIC([studio effective rent/unit]) = 1 
			THEN CAST([studio effective rent/unit] AS numeric (38,8))
		ELSE NULL
		END AS [studio effective rent/unit]
	,CASE WHEN ISNUMERIC([studio vacancy %]) = 1 
			THEN CAST([studio vacancy %] AS numeric (38,8))
		ELSE NULL
		END AS [studio vacancy %]
	,CASE WHEN ISNUMERIC([studio vacant units]) = 1 
			THEN CAST([studio vacant units] AS int)
		ELSE NULL
		END AS [studio vacant units]
	,[style]
	,CASE WHEN ISNUMERIC([three bedroom asking rent/sf]) = 1 
			THEN CAST([three bedroom asking rent/sf] AS numeric (38,8))
		ELSE NULL
		END AS [three bedroom asking rent/sf]
	,CASE WHEN ISNUMERIC([three bedroom asking rent/unit]) = 1 
			THEN CAST([three bedroom asking rent/unit] AS numeric (38,8))
		ELSE NULL
		END AS [three bedroom asking rent/unit]
	,CASE WHEN ISNUMERIC([three bedroom avg sf]) = 1 
			THEN CAST([three bedroom avg sf] AS numeric (38,8))
		ELSE NULL
		END AS [three bedroom avg sf]
	,CASE WHEN ISNUMERIC([three bedroom concessions %]) = 1 
			THEN CAST([three bedroom concessions %] AS numeric (38,8))
		ELSE NULL
		END AS [three bedroom concessions %]
	,CASE WHEN ISNUMERIC([three bedroom effective rent/sf]) = 1 
			THEN CAST([three bedroom effective rent/sf] AS numeric (38,8))
		ELSE NULL
		END AS [three bedroom effective rent/sf]
	,CASE WHEN ISNUMERIC([three bedroom effective rent/unit]) = 1 
			THEN CAST([three bedroom effective rent/unit] AS numeric (38,8))
		ELSE NULL
		END AS [three bedroom effective rent/unit]
	,CASE WHEN ISNUMERIC([three bedroom vacancy %]) = 1 
			THEN CAST([three bedroom vacancy %] AS numeric (38,8))
		ELSE NULL
		END AS [three bedroom vacancy %]
	,CASE WHEN ISNUMERIC([three bedroom vacant units]) = 1 
			THEN CAST([three bedroom vacant units] AS numeric (38,8))
		ELSE NULL
		END AS [three bedroom vacant units]
	,CASE WHEN ISNUMERIC([total buildings]) = 1 
			THEN CAST([total buildings] AS numeric (38,8))
		ELSE NULL
		END AS [total buildings]
		--
	,CASE WHEN ISNUMERIC([two bedroom asking rent/sf]) = 1 
			THEN CAST([two bedroom asking rent/sf] AS numeric (38,8))
		ELSE NULL
		END AS [two bedroom asking rent/sf]
	,CASE WHEN ISNUMERIC([two bedroom asking rent/unit]) = 1 
			THEN CAST([two bedroom asking rent/unit] AS numeric (38,8))
		ELSE NULL
		END AS [two bedroom asking rent/unit]
	,CASE WHEN ISNUMERIC([two bedroom avg sf]) = 1 
			THEN CAST([two bedroom avg sf] AS numeric (38,8))
		ELSE NULL
		END AS [two bedroom avg sf]
	,CASE WHEN ISNUMERIC([two bedroom concessions %]) = 1 
			THEN CAST([two bedroom concessions %] AS numeric (38,8))
		ELSE NULL
		END AS [two bedroom concessions %]
	,CASE WHEN ISNUMERIC([two bedroom effective rent/sf]) = 1 
			THEN CAST([two bedroom effective rent/sf] AS numeric (38,8))
		ELSE NULL
		END AS [two bedroom effective rent/sf]
	,CASE WHEN ISNUMERIC([two bedroom effective rent/unit]) = 1 
			THEN CAST([two bedroom effective rent/unit] AS numeric (38,8))
		ELSE NULL
		END AS [two bedroom effective rent/unit]
	,CASE WHEN ISNUMERIC([two bedroom vacancy %]) = 1 
			THEN CAST([two bedroom vacancy %] AS numeric (38,8))
		ELSE NULL
		END AS [two bedroom vacancy %]
	,CASE WHEN ISNUMERIC([two bedroom vacant units]) = 1 
			THEN CAST([two bedroom vacant units] AS int)
		ELSE NULL
		END AS [two bedroom vacant units]
	,CASE WHEN ISNUMERIC([vacancy %]) = 1 
			THEN CAST([vacancy %] AS numeric (38,8))
		ELSE NULL
		END AS [vacancy %]
	,[centroid]
FROM
    input.costar_staging
GO
//** END DATA LOAD **//

--CREATE SPATIAL INDEX
CREATE SPATIAL INDEX [ix_spatial_input_costar_centroid] ON input.costar
(
	centroid
) USING  GEOMETRY_GRID
	WITH (BOUNDING_BOX =(6152300, 1775400, 6613100, 2129400), GRIDS =(LEVEL_1 = MEDIUM,LEVEL_2 = MEDIUM,LEVEL_3 = MEDIUM,LEVEL_4 = MEDIUM), 
	CELLS_PER_OBJECT = 16, PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, SORT_IN_TEMPDB = OFF, DROP_EXISTING = OFF, ONLINE = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]

--ADD PARCEL_ID FIELD 
ALTER TABLE input.costar ADD parcel_id int NULL
--GET PARCEL_ID BY SPATIAL JOIN
UPDATE
	c
SET
	c.parcel_id = usp.parcel_id
FROM
    input.costar c
JOIN spacecore.urbansim.parcels usp ON c.centroid.STIntersects(usp.shape) = 1

--ADD SUBPARCEL_ID FIELD
ALTER TABLE input.costar ADD subparcel_id int NULL
--GET SUBPARCEL_ID BY SPATIAL JOIN
UPDATE
	c
SET
	c.subparcel_id = usl.subparcel
FROM
    input.costar c
JOIN spacecore.core.landcore usl ON c.centroid.STIntersects(usl.shape) = 1


//** FIND RECORDS WHERE PARCEL_ID IS NULL, LAT LONG NOT MAPPING TO PARCEL, ASSIGN TO NEAREST PARCEL**//

UPDATE
	c
SET
	c.parcel_id = p.parcel_id
FROM
	input.costar c
JOIN (
	SELECT row_id, property_id, parcel_id, dist 
	FROM (
		SELECT
			ROW_NUMBER() OVER (PARTITION BY c.property_id ORDER BY c.property_id, c.centroid.STDistance(p.shape)) row_id
			,c.property_id
			,p.parcel_id
			,c.centroid.STDistance(p.shape) AS dist
		FROM urbansim.parcels p
			INNER JOIN (SELECT * FROM input.costar WHERE parcel_id IS NULL) c ON c.centroid.STBuffer(1000).STIntersects(p.shape) = 1) x
	WHERE row_id = 1) p
ON c.property_id = p.property_id
WHERE c.parcel_id IS NULL

