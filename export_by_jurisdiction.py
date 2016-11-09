from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import pandas as pd
import os
import models as md
import yaml
import math
from urbansim_defaults import datasources


urbansim_engine = create_engine(get_connection_string("configs/dbconfig.yml", 'urbansim_database'))
data_cafe_engine = create_engine(get_connection_string("configs/dbconfig.yml", 'data_cafe'))

zone = datasources.settings()['jurisdiction']

bounding_box_sql =  """SELECT name as CITY
                        ,geometry::STGeomFromText(shape.STAsText(),4326).STEnvelope().STPointN(1).STX AS [Left]
                        ,geometry::STGeomFromText(shape.STAsText(),4326).STEnvelope().STPointN(1).STY AS [Bottom]
                        ,geometry::STGeomFromText(shape.STAsText(),4326).STEnvelope().STPointN(3).STX AS [Right]
                        ,geometry::STGeomFromText(shape.STAsText(),4326).STEnvelope().STPointN(3).STY AS [Top]
                        FROM ref.geography_zone
                        WHERE geography_type_id = 136
                        and zone = """ + str(zone)

bounding_box_df = pd.read_sql(bounding_box_sql, data_cafe_engine)

nodes_sql = 'SELECT node as node_id, x, y, on_ramp FROM urbansim.nodes ' \
            'WHERE x between ' + str(bounding_box_df.iloc[0]['Left']) + ' and ' +  str(bounding_box_df.iloc[0]['Right']) + \
            'and y between ' + str(bounding_box_df.iloc[0]['Bottom']) + ' and ' +  str(bounding_box_df.iloc[0]['Top'])


# Necessary to duplicate nodes in order to generate built environment variables for the regessions
intersection_sql = 'SELECT node as intersection_id, x, y FROM urbansim.nodes ' \
                   'WHERE x between ' + str(bounding_box_df.iloc[0]['Left']) + ' and ' +  str(bounding_box_df.iloc[0]['Right']) + \
                    'and y between ' + str(bounding_box_df.iloc[0]['Bottom']) + ' and ' +  str(bounding_box_df.iloc[0]['Top'])

edges_sql = 'SELECT from_node as from, to_node as to, distance as weight FROM urbansim.edges ' \
            'WHERE from_node IN  ' \
            '(select node from urbansim.nodes ' \
            'WHERE x between ' + str(bounding_box_df.iloc[0]['Left']) + ' and ' +  str(bounding_box_df.iloc[0]['Right']) + \
            'and y between ' + str(bounding_box_df.iloc[0]['Bottom']) + ' and ' +  str(bounding_box_df.iloc[0]['Top']) + ')' \
            'AND to_node IN ' \
            '(select node from urbansim.nodes ' \
            'WHERE x between ' + str(bounding_box_df.iloc[0]['Left']) + ' and ' +  str(bounding_box_df.iloc[0]['Right']) + \
            'and y between ' + str(bounding_box_df.iloc[0]['Bottom']) + ' and ' +  str(bounding_box_df.iloc[0]['Top']) + ')' \


parcels_sql = 'SELECT parcel_id, development_type_id, luz_id, parcel_acres as acres, ' \
              'zoning_id, ST_X(ST_Transform(centroid::geometry, 2230)) as x, ST_Y(ST_Transform(centroid::geometry, 2230))  as y, ' \
              'distance_to_coast, distance_to_freeway FROM urbansim.parcels where jurisdiction_id = ' + str(zone)


buildings_sql = 'SELECT building_id, parcel_id, COALESCE(development_type_id,0) as building_type_id, COALESCE(residential_units, 0) as residential_units, ' \
                'COALESCE(residential_sqft, 0) as residential_sqft, COALESCE(non_residential_sqft,0) as non_residential_sqft, ' \
                '0 as non_residential_rent_per_sqft, COALESCE(year_built, -1) year_built, COALESCE(stories, 1) as stories FROM urbansim.buildings ' \
                'where parcel_id IN (select parcel_id from urbansim.parcels where jurisdiction_id = ' + str(zone) + ')'


households_sql = 'SELECT household_id, building_id, persons, age_of_head, income, children FROM urbansim.households ' \
                 'where building_id IN (select building_id from urbansim.buildings where parcel_id IN ' \
                 '(select parcel_id from urbansim.parcels where jurisdiction_id = ' + str(zone) +' ))'


jobs_sql = 'SELECT job_id, building_id, sector_id FROM urbansim.jobs'
building_sqft_per_job_sql = 'SELECT luz_id, development_type_id, sqft_per_emp FROM urbansim.building_sqft_per_job'


scheduled_development_events_sql = """SELECT
                                         "siteID", parcel_id, "devTypeID" as building_type_id,
                                         EXTRACT(YEAR FROM "compDate")  as year_built, COALESCE(sfu,0) + COALESCE(mfu,0) AS residential_units,
                                         "nResSqft" as non_residential_sqft, "resSqft" as residential_sqft,NULL as non_residential_rent_per_sqft,
                                         NULL as stories
                                         FROM urbansim.scheduled_development
                                         WHERE parcel_id IN
                                         (select parcel_id from urbansim.parcels where jurisdiction_id = """ + str(zone) + ')'

schools_sql = """SELECT id, x ,y FROM urbansim.schools"""
parks_sql = """SELECT park_id,  x, y FROM urbansim.parks"""
transit_sql = 'SELECT x, y, stopnum FROM urbansim.transit'
household_controls_sql = """SELECT yr as year, income_quartile, households as hh FROM urbansim.household_controls"""
employment_controls_sql = """SELECT yr as year, number_of_jobs, sector_id FROM urbansim.employment_controls"""
zoning_allowed_uses_sql = """SELECT development_type_id, zoning_id FROM urbansim.zoning_allowed_use ORDER BY development_type_id, zoning_id"""
zoning_allowed_uses_aggregate_sql = """SELECT zoning_id, array_agg(development_type_id) as allowed_development_types FROM urbansim.zoning_allowed_use GROUP BY zoning_id"""
fee_schedule_sql = """SELECT development_type_id, development_fee_per_unit_space_initial FROM urbansim.fee_schedule"""
zoning_sql = """SELECT zoning_schedule_id, zoning_id, max_dua, max_building_height as max_height, max_far, max_res_units FROM urbansim.zoning"""


assessor_transactions_sql = """SELECT parcel_id, tx_price FROM (SELECT parcel_id, RANK() OVER (PARTITION BY parcel_id ORDER BY tx_date) as tx,
                                tx_date, tx_price FROM estimation.assessor_par_transactions) x WHERE tx = 1"""

nodes_df = pd.read_sql(nodes_sql, urbansim_engine, index_col='node_id')
intersection_df = pd.read_sql(intersection_sql, urbansim_engine, index_col='intersection_id')
edges_df = pd.read_sql(edges_sql, urbansim_engine)
parcels_df = pd.read_sql(parcels_sql, urbansim_engine,index_col='parcel_id')
buildings_df = pd.read_sql(buildings_sql, urbansim_engine, index_col='building_id')
households_df = pd.read_sql(households_sql, urbansim_engine, index_col='household_id')
jobs_df = pd.read_sql(jobs_sql, urbansim_engine, index_col='job_id')
building_sqft_per_job_df = pd.read_sql(building_sqft_per_job_sql, urbansim_engine)
scheduled_development_events_df = pd.read_sql(scheduled_development_events_sql, urbansim_engine)
schools_df = pd.read_sql(schools_sql, urbansim_engine, index_col='id')
parks_df = pd.read_sql(parks_sql, urbansim_engine, index_col='park_id')
transit_df = pd.read_sql(transit_sql, urbansim_engine)
household_controls_df = pd.read_sql(household_controls_sql, urbansim_engine, index_col='year')
employment_controls_df = pd.read_sql(employment_controls_sql, urbansim_engine, index_col='year')
zoning_allowed_uses_df = pd.read_sql(zoning_allowed_uses_sql, urbansim_engine, index_col='development_type_id')
zoning_allowed_uses_aggregate_df = pd.read_sql(zoning_allowed_uses_aggregate_sql, urbansim_engine)
fee_schedule_df = pd.read_sql(fee_schedule_sql, urbansim_engine, index_col='development_type_id')
zoning_df = pd.read_sql(zoning_sql, urbansim_engine)

#assessor_transactions_df = pd.read_sql(assessor_transactions_sql, urbansim_engine)

building_sqft_per_job_df.sort_values(['luz_id', 'development_type_id'], inplace=True)
building_sqft_per_job_df.set_index(['luz_id', 'development_type_id'], inplace=True)

edges_df.sort_values(['from', 'to'], inplace=True)
#edges_df.set_index(['from', 'to'], inplace=True)
# convert unicode 'zoning_id' to str (needed for HDFStore in python 2)
parcels_df['zoning_id'] = parcels_df['zoning_id'].astype(str)
zoning_allowed_uses_df['zoning_id'] = zoning_allowed_uses_df['zoning_id'].astype(str)
zoning_allowed_uses_aggregate_df['zoning_id'] = zoning_allowed_uses_aggregate_df['zoning_id'].astype(str)
zoning_allowed_uses_aggregate_df['allowed_development_types'] = zoning_allowed_uses_aggregate_df['allowed_development_types'].astype(str)

zoning_df['zoning_id'] = zoning_df['zoning_id'].astype(str)
zoning_df = zoning_df.set_index('zoning_id')

###########################
# parent data zoning
###########################
zoning_schedule_sql = """SELECT * FROM urbansim.zoning_schedule"""
zoning_schedule_df = pd.read_sql(zoning_schedule_sql, urbansim_engine)

# get dataframe from table with updated zoning for parcels
parcel_updates_sql = 'SELECT zoning_schedule_id, parcel_id, zoning_id FROM urbansim.parcel_zoning_schedule'
parcel_updates_df = pd.read_sql(parcel_updates_sql, urbansim_engine)

parcels_df['zoning_schedule_id'] = 1 # to track which parcels have orig zoning
parent = datasources.settings()['zoning_schedule_id'] # zoning schedule id from settings.yaml

# create list of zoning schedule ids w parent to each
# e.g. [3, 2, 1] where 3 has parent 2 that has parent 1
zoning_sched_ids = []
while not math.isnan(parent):
    zoning_sched_ids.append(parent)
    parent = zoning_schedule_df['parent_zoning_schedule_id'][zoning_schedule_df['zoning_schedule_id'] == parent].values[0]

zoning_df_zsid = zoning_df[zoning_df['zoning_schedule_id'] == zoning_sched_ids[-1]] # zoning_df parent
zoning_sched_ids = zoning_sched_ids[:-1] # remove last parent id (i.e. 1), bc assuming parcel table is parent

parcels_df['original_zoning_id'] = parcels_df['zoning_id']
# replace parcel table zoning starting with lowest zoning schedule id to highest id
# e.g. replace parcel table zoning with parcel zoning schedule id=2 and then with id=3
for zsid in reversed(zoning_sched_ids):
    parcel_update_zsid = parcel_updates_df[parcel_updates_df['zoning_schedule_id'] == zsid]
    parcel_update_zsid = parcel_update_zsid.set_index(['parcel_id'])
    parcels_df.loc[parcels_df.index.isin(parcel_update_zsid.index), ['zoning_id', 'zoning_schedule_id']] = parcel_update_zsid[['zoning_id', 'zoning_schedule_id']]
    zoning_df_zsid = zoning_df_zsid.append(zoning_df[zoning_df['zoning_schedule_id'] == zsid])

parcels_df['zoning_id'] = parcels_df['zoning_id'].astype(str) # zoning as string for writng to .h5

#########################################
# scale household controls
#########################################

hh_df = households_df.reset_index(drop=False) # use household_id as column
hh_inc = hh_df[['household_id', 'income']].copy()

# current income distribution of households
bins = [hh_inc.income.min()-1, 30000, 59999, 99999, 149999, hh_inc.income.max()+1]
group_names = range(1,6)
hh_income_quartile =pd.cut(hh_inc.income, bins, labels=group_names).astype('int64')
hh_income_quartile.value_counts()

orig_pop_q1 = household_controls_df[(household_controls_df.index ==2015) & (household_controls_df['income_quartile'] == 1)]
orig_pop_q2 = household_controls_df[(household_controls_df.index ==2015) & (household_controls_df['income_quartile'] == 2)]
orig_pop_q3 = household_controls_df[(household_controls_df.index ==2015) & (household_controls_df['income_quartile'] == 3)]
orig_pop_q4 = household_controls_df[(household_controls_df.index ==2015) & (household_controls_df['income_quartile'] == 4)]
orig_pop_q5 = household_controls_df[(household_controls_df.index ==2015) & (household_controls_df['income_quartile'] == 5)]

q1 = orig_pop_q1.iloc[0]['hh'].astype(float)
q2 = orig_pop_q2.iloc[0]['hh'].astype(float)
q3 = orig_pop_q3.iloc[0]['hh'].astype(float)
q4 = orig_pop_q4.iloc[0]['hh'].astype(float)
q5 = orig_pop_q5.iloc[0]['hh'].astype(float)

# calculate sccale factor
household_controls_df.ix[household_controls_df.income_quartile == 1,'scale_factor'] = hh_income_quartile.value_counts()[1]/q1
household_controls_df.ix[household_controls_df.income_quartile == 2,'scale_factor'] = hh_income_quartile.value_counts()[2]/q2
household_controls_df.ix[household_controls_df.income_quartile == 3,'scale_factor'] = hh_income_quartile.value_counts()[3]/q3
household_controls_df.ix[household_controls_df.income_quartile == 4,'scale_factor'] = hh_income_quartile.value_counts()[4]/q4
household_controls_df.ix[household_controls_df.income_quartile == 5,'scale_factor'] = hh_income_quartile.value_counts()[5]/q5

household_controls_df['hh_original'] = household_controls_df['hh'] # for checking keep original
#household_controls_df['hh'] = (household_controls_df.hh * household_controls_df.scale_factor).astype('int64')
household_controls_df['hh'] = (household_controls_df.hh * household_controls_df.scale_factor).round(decimals=0)
#household_controls_df[(household_controls_df.index ==2015)]
#household_controls_df.groupby([household_controls_df.index])['hh'].sum()
del household_controls_df['scale_factor']
del household_controls_df['hh_original']

if not os.path.exists('data'):
    os.makedirs('data')

with pd.HDFStore('data/urbansim.h5', mode='w') as store:
    store.put('nodes', nodes_df, format='t')
    store.put('intersections', intersection_df, format='t')
    store.put('edges', edges_df, format='t')
    store.put('parcels', parcels_df, format='t')
    store.put('buildings', buildings_df, format='t')
    store.put('households', households_df, format='t')
    store.put('jobs', jobs_df, format='t')
    store.put('building_sqft_per_job', building_sqft_per_job_df, format='t')
    store.put('scheduled_development_events', scheduled_development_events_df, format='t')
    store.put('schools', schools_df, format='t')
    store.put('parks', parks_df, format='t')
    store.put('transit', transit_df, format='t')
    store.put('household_controls', household_controls_df, format='t')
    store.put('employment_controls', employment_controls_df, format='t')
    store.put('zoning_allowed_uses', zoning_allowed_uses_df, format='t')
    store.put('fee_schedule', fee_schedule_df, format='t')
    store.put('zoning', zoning_df_zsid, format='t')
    store.put('zoning_allowed_uses_aggregate', zoning_allowed_uses_aggregate_df, format='t')
    #store.put('assessor_transactions', assessor_transactions_df, format='t')

zoning_df_zsid.to_csv('data/zoning_w_zsid.csv')