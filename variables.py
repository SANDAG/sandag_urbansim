import numpy as np
import pandas as pd
import orca
from urbansim.utils import misc

#####  ASSESSOR TRANSACTIONS #####
@orca.column('assessor_transactions', 'node_id')
def col_assessor_node_id(parcels, assessor_transactions):
    return misc.reindex(parcels.node_id, assessor_transactions.parcel_id)

#####  BUILDINGS #####
@orca.column('buildings', 'building_sqft')
def building_sqft(buildings):
    return buildings.residential_sqft + buildings.non_residential_sqft


@orca.column('buildings', 'distance_to_coast')
def distance_to_coaset(buildings, parcels):
    return misc.reindex(parcels.distance_to_coast, buildings.parcel_id)


@orca.column('buildings', 'distance_to_coast_mi')
def distance_to_coast_mi(buildings):
    return buildings.distance_to_coast / 5280.0


@orca.column('buildings', 'distance_to_freeway')
def distance_to_freeway(buildings, parcels):
    return misc.reindex(parcels.distance_to_freeway, buildings.parcel_id)


@orca.column('buildings', 'distance_to_onramp')
def distance_to_onramp(settings, net, buildings):
    ramp_distance = settings['build_networks']['on_ramp_distance']
    distance_df = net.nearest_pois(ramp_distance, 'onramps', num_pois=1, max_distance=ramp_distance)
    distance_df.columns = ['distance_to_onramp']
    return misc.reindex(distance_df.distance_to_onramp, buildings.node_id)


@orca.column('buildings', 'distance_to_onramp_mi')
def distance_to_onramp_mi(buildings):
    return buildings.distance_to_onramp / 5280.0


@orca.column('buildings', 'distance_to_park')
def distance_to_park(settings, net, buildings):
    park_distance = settings['build_networks']['parks_distance']
    distance_df = net.nearest_pois(park_distance, 'parks', num_pois=1, max_distance=park_distance)
    distance_df.columns = ['distance_to_park']
    return misc.reindex(distance_df.distance_to_park, buildings.node_id)


@orca.column('buildings','distance_to_school')
def distance_to_school(settings, net, buildings):
    school_distance = settings['build_networks']['schools_distance']
    distance_df = net.nearest_pois(school_distance, 'schools', num_pois=1, max_distance=school_distance)
    distance_df.columns = ['distance_to_school']
    return misc.reindex(distance_df.distance_to_school, buildings.node_id)


@orca.column('buildings','distance_to_transit')
def distance_to_transit(settings, net, buildings):
    transit_distance = settings['build_networks']['transit_distance']
    distance_df = net.nearest_pois(transit_distance, 'transit', num_pois=1, max_distance=transit_distance)
    distance_df.columns = ['distance_to_transit']
    return misc.reindex(distance_df.distance_to_transit, buildings.node_id)


@orca.column('buildings','distance_to_transit_mi')
def distance_to_transit_mi(buildings):
    return buildings.distance_to_transit / 5280.0


@orca.column('buildings', 'is_office')
def is_office(buildings):
    return (buildings.building_type_id == 4).astype('int')


@orca.column('buildings', 'is_retail')
def is_retail(buildings):
    return (buildings.building_type_id == 5).astype('int')


@orca.column('buildings', 'luz_id')
def luz_id(buildings, parcels):
    return misc.reindex(parcels.luz_id, buildings.parcel_id).fillna(0)


@orca.column('buildings', 'parcel_size')
def building_parcel_size(buildings, parcels):
    return misc.reindex(parcels.parcel_size, buildings.parcel_id)


# for multi-family type 21, convert rents from res hedonic to price
# use residential hedonic result for other dev types
@orca.column('buildings', 'residential_price_adj')
def residential_price_adj( buildings, settings):
    if 'residential_price' not in orca.get_table('buildings').columns:
        return pd.Series(0, orca.get_table('buildings').index)
    return np.where(buildings['building_type_id'] == 21,
                    (buildings['residential_price'] * 12)/
                    (settings['res_sales_price_multiplier'] *
                     settings['sqftproforma_config']['cap_rate']),
                    buildings['residential_price'])


@orca.column('buildings', 'sqft_per_job', cache=True)
def sqft_per_job(buildings, building_sqft_per_job):
    bldgs = buildings.to_frame(['luz_id', 'building_type_id'])
    merge_df = pd.merge(bldgs, building_sqft_per_job.to_frame(), how='left', left_on=['luz_id', 'building_type_id'], right_index=True)
    merge_df.sqft_per_emp.fillna(-1, inplace=True)
    merge_df.loc[merge_df.sqft_per_emp < 40, 'sqft_per_emp'] = 40
    return merge_df.sqft_per_emp


@orca.column('buildings', 'sqft_per_unit', cache=True)
def unit_sqft(buildings):
    return (buildings.residential_sqft /
            buildings.residential_units.replace(0, 1)).fillna(0).astype('int')


@orca.column('buildings', 'vacant_residential_units')
def vacant_residential_units(buildings, households):
    return buildings.residential_units.sub(
        households.building_id.value_counts(), fill_value=0).astype('int64')


@orca.column('buildings', 'year_built_1940to1950')
def year_built_1940to1950(buildings):
    return (buildings.year_built >= 1940) & (buildings.year_built < 1950)


@orca.column('buildings', 'structure_age')
def structure_age(buildings):
    year = orca.get_injectable('year')
    if year is None:
        year = 2015
    return (year - buildings.year_built)


@orca.column('buildings', 'year_built_1950to1960')
def year_built_1950to1960(buildings):
    return (buildings.year_built >= 1950) & (buildings.year_built < 1960)


@orca.column('buildings', 'year_built_1960to1970')
def year_built_1960to1970(buildings):
    return (buildings.year_built >= 1960) & (buildings.year_built < 1970)


@orca.column('buildings', 'year_built_1970to1980')
def year_built_1970to1980(buildings):
    return (buildings.year_built >= 1970) & (buildings.year_built < 1980)


@orca.column('buildings', 'year_built_1980to1990')
def year_built_1980to1990(buildings):
    return (buildings.year_built >= 1980) & (buildings.year_built < 1990)


##### HOUSEHOLDS #####
@orca.column('households', 'income_quartile', cache=True)
def income_quartile(households):
    hh_inc = households.to_frame(['household_id', 'income'])
    bins = [hh_inc.income.min()-1, 30000, 59999, 99999, 149999, hh_inc.income.max()+1]
    group_names = range(1,     6)
    return pd.cut(hh_inc.income, bins, labels=group_names).astype('int64')


#####  NODES #######
@orca.column('nodes', 'nonres_occupancy_10000ft')
def nonres_occupancy_3000ft(nodes):
    return nodes.jobs_10000ft / (nodes.job_spaces_10000ft + 1.0)


@orca.column('nodes', 'res_occupancy_10000ft')
def res_occupancy_10000ft(nodes):
    return nodes.households_10000ft / (nodes.residential_units_10000ft + 1.0)


###### PARCELS ######
##################### Building purchase price based on parcel avg price ##
# @orca.column('parcels', 'building_purchase_price_sqft')
# def building_purchase_price_sqft(settings):
#     return parcel_average_price("residential") * settings['parcel_avg_pr_mult']


# @orca.column('parcels', 'building_purchase_price')
# def building_purchase_price(parcels):
#     return (parcels.total_sqft * parcels.building_purchase_price_sqft).\
#         reindex(parcels.index).fillna(0)
##########################################################################


##################### Building purchase price based on res hedonic #######
# avg price buildings on parcel instead of parcel avg for rent calc
@orca.column('parcels', 'avg_residential_price')
def avg_residential_price(parcels, buildings):
    return buildings.to_frame().residential_price_adj.\
        groupby(buildings.parcel_id).mean().reindex(parcels.index).fillna(0)


@orca.column('parcels', 'building_purchase_price')
def building_purchase_price(parcels, buildings):
    return (buildings.residential_price_adj * buildings.building_sqft).\
        groupby(buildings.parcel_id).sum().reindex(parcels.index).fillna(0)
##########################################################################


@orca.column('parcels', 'distance_to_onramp')
def parcels_distance_to_onramp(settings, net, parcels):
    ramp_distance = settings['build_networks']['on_ramp_distance']
    distance_df = net.nearest_pois(ramp_distance, 'onramps', num_pois=1, max_distance=ramp_distance)
    distance_df.columns = ['distance_to_onramp']
    return misc.reindex(distance_df.distance_to_onramp, parcels.node_id)


@orca.column('parcels', 'distance_to_park')
def parcels_distance_to_park(settings, net, parcels):
    park_distance = settings['build_networks']['parks_distance']
    distance_df = net.nearest_pois(park_distance, 'parks', num_pois=1, max_distance=park_distance)
    distance_df.columns = ['distance_to_park']
    return misc.reindex(distance_df.distance_to_park, parcels.node_id)


@orca.column('parcels','distance_to_school')
def parcels_distance_to_school(settings, net, parcels):
    school_distance = settings['build_networks']['schools_distance']
    distance_df = net.nearest_pois(school_distance, 'schools', num_pois=1, max_distance=school_distance)
    distance_df.columns = ['distance_to_school']
    return misc.reindex(distance_df.distance_to_school, parcels.node_id)


@orca.column('parcels','distance_to_transit')
def parcels_distance_to_transit(settings, net, parcels):
    transit_distance = settings['build_networks']['transit_distance']
    distance_df = net.nearest_pois(transit_distance, 'transit', num_pois=1, max_distance=transit_distance)
    distance_df.columns = ['distance_to_transit']
    return misc.reindex(distance_df.distance_to_transit, parcels.node_id)


# @orca.column('parcels', 'land_cost')
# def parcel_land_cost(settings, parcels):
#     return parcels.building_purchase_price + parcels.parcel_size * settings['default_land_cost']


@orca.column('parcels', 'land_cost')
def parcel_land_cost(settings, parcels):
    return np.where(parcels['building_purchase_price'] == 0,
                    (parcels.parcel_size * settings['default_land_cost']),
                     parcels['building_purchase_price'])


@orca.column('parcels', 'max_dua_zoning', cache=True)
def parcel_max_dua(parcels, zoning):
    return misc.reindex(zoning.max_dua, parcels.zoning_id)


@orca.column('parcels', 'zoned_du', cache=True)
def zoned_du(parcels):
    return (parcels.max_dua_zoning * parcels.parcel_acres).\
        reindex(parcels.index).fillna(0).round().astype('int')


@orca.column('parcels', 'max_far', cache=True)
def parcel_max_far(parcels, zoning, settings):
    return misc.reindex(zoning.max_far, parcels.zoning_id).fillna(settings['sqftproforma_config']['fars'][-1])


##Placeholder-  building height currently unconstrained (very high limit-  1000 ft.)
@orca.column('parcels', 'max_height', cache=True)
def parcel_max_height(parcels, zoning):
    return misc.reindex(zoning.max_height, parcels.zoning_id).fillna(350)


@orca.column('parcels', 'max_res_units', cache=True)
def parcel_max_res_units(parcels, zoning):
    return misc.reindex(zoning.max_res_units, parcels.zoning_id)


@orca.column('parcels', 'newest_building')
def newest_building(parcels, buildings):
    return buildings.year_built.groupby(buildings.parcel_id).max().\
        reindex(parcels.index).fillna(0)


@orca.column('parcels', 'parcel_size', cache=True)
def parcel_size(parcels, settings):
    return parcels.acres * 43560


@orca.column('parcels', 'parcel_acres')
def parcel_acres(parcels):
    return parcels.acres


@orca.column('parcels', 'lot_size_per_unit')
def log_size_per_unit(parcels):
    return parcels.parcel_size / parcels.total_residential_units.replace(0, 1)


@orca.column('parcels', 'total_sqft', cache=True)
def total_sqft(parcels, buildings):
    return buildings.building_sqft.groupby(buildings.parcel_id).sum().\
        reindex(parcels.index).fillna(0)


@orca.column('parcels', 'zone_id', cache=True)
def parcel_zone_id(parcels):
    return parcels.zoning_id

###### MISCELLANEOUS #######
@orca.injectable('add_extra_columns_func', autocall=False)
def add_extra_colums(df):
    buildings = orca.get_table('buildings')
    for col_name in buildings.local_columns:
        if col_name not in df.columns:
            df[col_name] = 0
    return df


@orca.injectable('building_sqft_per_job', cache=True)
def building_sqft_per_job(settings):
    return settings['building_sqft_per_job']


@orca.injectable('form_to_btype_func', autocall=False)
def form_to_btype(row):
    if row.form == 'office':
        return 4
    if row.form == 'retail':
        return 5
    if row.form == 'industrial':
        return 2
    if row.form == 'residential':
        return 19


@orca.injectable('parcel_sales_price_sqft_func', autocall=False)
def parcel_sales_price_sqft(use):
    s = parcel_average_price(use)
    import yaml
    with open('configs/settings.yaml', 'r') as f:
        settings = yaml.load(f)
    if use == "residential": s *= settings['res_sales_price_multiplier']
    return s


@orca.injectable('parcel_average_price', autocall=False)
def parcel_average_price(use):
    if len(orca.get_table('nodes').index) == 0:
        return pd.Series(0, orca.get_table('parcels').index)
    if not use in orca.get_table('nodes').columns:
        return pd.Series(0, orca.get_table('parcels').index)
    return misc.reindex(orca.get_table('nodes')[use],
                        orca.get_table('parcels').node_id)


@orca.injectable('parcel_is_allowed_func', autocall=False)
def parcel_is_allowed(form):
    parcels = orca.get_table('parcels')
    zoning_allowed_uses = orca.get_table('zoning_allowed_uses').to_frame()

    if form == 'sf_detached':
        allowed = zoning_allowed_uses[19]
    elif form == 'sf_attached':
        allowed = zoning_allowed_uses[20]
    elif form == 'mf_residential':
        allowed = zoning_allowed_uses[21]
    elif form == 'light_industrial':
        allowed = zoning_allowed_uses[2]
    elif form == 'heavy_industrial':
        allowed = zoning_allowed_uses[3]
    elif form == 'office':
        allowed = zoning_allowed_uses[4]
    elif form == 'retail':
        allowed = zoning_allowed_uses[5]
    elif form == 'residential':
        allowed = zoning_allowed_uses[19] | zoning_allowed_uses[20] | zoning_allowed_uses[21]
    else:
        df = pd.DataFrame(index=parcels.index)
        df['allowed'] = True
        allowed = df.allowed

    return allowed