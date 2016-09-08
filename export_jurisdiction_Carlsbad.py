from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import pandas as pd
import os
import models as md
import yaml
import math

with open('configs/settings.yaml', 'r') as f:
    settings = yaml.load(f)

urbansim_engine = create_engine(get_connection_string("configs/dbconfig.yml", 'urbansim_database'))

#nodes_sql = 'SELECT node as node_id, x, y, on_ramp FROM urbansim.nodes'

nodes_sql = 'SELECT node as node_id, x, y, on_ramp FROM urbansim.nodes ' \
            'WHERE x between 6219937 and 6268194 and y between 1964616 and 2014714'

intersection_sql = 'SELECT node as intersection_id, x, y FROM urbansim.nodes'

edges_sql = 'SELECT from_node as from, to_node as to, distance as weight FROM urbansim.edges'

# Necessary to duplicate nodes in order to generate built environment variables for the regessions
intersection_sql = 'SELECT node as intersection_id, x, y FROM urbansim.nodes ' \
                   'WHERE x between 6219937 and 6268194 and y between 1964616 and 2014714'

edges_sql = 'SELECT from_node as from, to_node as to, distance as weight FROM urbansim.edges ' \
            'WHERE from_node IN  (select node from urbansim.nodes ' \
            'WHERE x between 6219937 and 6268194 and y between 1964616 and 2014714) AND to_node ' \
            'IN (select node from urbansim.nodes WHERE x between 6219937 and 6268194 and y between 1964616 and 2014714)'

parcels_sql = 'SELECT parcel_id, development_type_id, luz_id, parcel_acres as acres, zoning_id, ST_X(ST_AsText(centroid)) as x, ST_Y(ST_AsText(centroid)) as y, distance_to_coast, distance_to_freeway FROM urbansim.parcels'
parcels_sql = 'SELECT parcel_id, development_type_id, luz_id, parcel_acres as acres, zoning_id, ST_X(ST_Transform(centroid::geometry, 2230)) as x, ST_Y(ST_Transform(centroid::geometry, 2230))  as y, ' \
              'distance_to_coast, distance_to_freeway FROM urbansim.parcels where jurisdiction_id = 1'

#buildings_sql = 'SELECT building_id, parcel_id, COALESCE(development_type_id,0) as building_type_id, COALESCE(residential_units, 0) as residential_units, COALESCE(residential_sqft, 0) as residential_sqft, COALESCE(non_residential_sqft,0) as non_residential_sqft, 0 as non_residential_rent_per_sqft, COALESCE(year_built, -1) year_built, COALESCE(stories, 1) as stories FROM urbansim.buildings'

buildings_sql = 'SELECT building_id, parcel_id, COALESCE(development_type_id,0) as building_type_id, COALESCE(residential_units, 0) as residential_units, ' \
                'COALESCE(residential_sqft, 0) as residential_sqft, COALESCE(non_residential_sqft,0) as non_residential_sqft, ' \
                '0 as non_residential_rent_per_sqft, COALESCE(year_built, -1) year_built, COALESCE(stories, 1) as stories FROM urbansim.buildings ' \
                'where parcel_id IN (select parcel_id from urbansim.parcels where jurisdiction_id = 1)'


#households_sql = 'SELECT household_id, building_id, persons, age_of_head, income, children FROM urbansim.households'

households_sql = 'SELECT household_id, building_id, persons, age_of_head, income, children FROM urbansim.households ' \
                 'where building_id IN (select building_id from urbansim.buildings where parcel_id IN ' \
                 '(select parcel_id from urbansim.parcels where jurisdiction_id = 1))'


jobs_sql = 'SELECT job_id, building_id, sector_id FROM urbansim.jobs'
building_sqft_per_job_sql = 'SELECT luz_id, development_type_id, sqft_per_emp FROM urbansim.building_sqft_per_job'
scheduled_development_events_sql = """SELECT
                                         scheduled_development_event_id, parcel_id, building_type_id
                                         ,year_built, sqft_per_unit, residential_units, non_residential_sqft
                                         ,improvement_value, res_price_per_sqft, non_residential_rent_per_sqft
                                         ,COALESCE(stories,1) as stories FROM urbansim.scheduled_development_event
                                         WHERE parcel_id IN (select parcel_id from urbansim.parcels where jurisdiction_id = 1)"""
schools_sql = """SELECT id, x ,y FROM urbansim.schools"""
parks_sql = """SELECT park_id,  x, y FROM urbansim.parks"""
transit_sql = 'SELECT x, y, stopnum FROM urbansim.transit'
household_controls_sql = """SELECT yr as year, income_quartile, households as hh FROM urbansim.household_controls"""
employment_controls_sql = """SELECT yr as year, number_of_jobs, sector_id FROM urbansim.employment_controls"""
zoning_allowed_uses_sql = """SELECT development_type_id, zoning_id FROM urbansim.zoning_allowed_use ORDER BY development_type_id, zoning_id"""
fee_schedule_sql = """SELECT development_type_id, development_fee_per_unit_space_initial FROM urbansim.fee_schedule"""
zoning_sql = """SELECT zoning_schedule_id, zoning_id, max_dua, max_building_height as max_height, max_far, max_res_units FROM staging.zoning"""


assessor_transactions_sql = """SELECT parcel_id, tx_price FROM (SELECT parcel_id, RANK() OVER (PARTITION BY parcel_id ORDER BY tx_date) as tx, tx_date, tx_price FROM estimation.assessor_par_transactions) x WHERE tx = 1"""

nodes_df = pd.read_sql(nodes_sql, urbansim_engine, index_col='node_id')
intersection_df = pd.read_sql(intersection_sql, urbansim_engine, index_col='intersection_id')
edges_df = pd.read_sql(edges_sql, urbansim_engine)
parcels_df = pd.read_sql(parcels_sql, urbansim_engine,index_col='parcel_id')
buildings_df = pd.read_sql(buildings_sql, urbansim_engine, index_col='building_id')
households_df = pd.read_sql(households_sql, urbansim_engine, index_col='household_id')
jobs_df = pd.read_sql(jobs_sql, urbansim_engine, index_col='job_id')
building_sqft_per_job_df = pd.read_sql(building_sqft_per_job_sql, urbansim_engine)
scheduled_development_events_df = pd.read_sql(scheduled_development_events_sql, urbansim_engine, index_col='scheduled_development_event_id')
schools_df = pd.read_sql(schools_sql, urbansim_engine, index_col='id')
parks_df = pd.read_sql(parks_sql, urbansim_engine, index_col='park_id')
transit_df = pd.read_sql(transit_sql, urbansim_engine)
household_controls_df = pd.read_sql(household_controls_sql, urbansim_engine, index_col='year')
employment_controls_df = pd.read_sql(employment_controls_sql, urbansim_engine, index_col='year')
zoning_allowed_uses_df = pd.read_sql(zoning_allowed_uses_sql, urbansim_engine, index_col='development_type_id')
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
zoning_df['zoning_id'] = zoning_df['zoning_id'].astype(str)
zoning_df = zoning_df.set_index('zoning_id')

###########################
# parent data zoning
###########################
zoning_schedule_sql = """SELECT * FROM staging.zoning_schedule"""
zoning_schedule_df = pd.read_sql(zoning_schedule_sql, urbansim_engine)

if (settings['zoning_schedule_id'] > 1) and (settings['zoning_schedule_id'] != max(zoning_df['zoning_schedule_id'])):
    zoning_df = md.get_zoning_values(id1=settings['zoning_schedule_id'], id_name='zoning_schedule_id',
                                     parent_name='parent_zoning_schedule_id',
                                     df_data=zoning_df, df_id=zoning_schedule_df)

###########################
# parent data parcel
###########################

# parcel_zoning_schedule = """SELECT ps.zoning_schedule_id, ps.parcel_id, p.development_type_id, p.luz_id, p.parcel_acres as acres, ps.zoning_id as zoning_id, ST_X(ST_Transform(centroid::geometry, 2230)) as x, ST_Y(ST_Transform(centroid::geometry, 2230)) as y,
#                p.distance_to_coast, p.distance_to_freeway
#                FROM staging.parcel_zoning_schedule as ps
#                INNER JOIN urbansim.parcels as p
#                on ps.parcel_id = p.parcel_id"""
#
# parcel_zoning_schedule_df = pd.read_sql(parcel_zoning_schedule, urbansim_engine)
#
# parcels_df['zoning_schedule_id'] = 1
#
# parcels_df = parcels_df.append(parcel_zoning_schedule_df)
#
# parcels_df = md.get_parent_values(id1=settings['zoning_schedule_id'], id_name='zoning_schedule_id',
#                                  parent_name='parent_zoning_schedule_id',
#                                  column='parcel_id', df_data=parcels_df, df_id=zoning_schedule_df)
#
# parcel_df = parcels_df.set_index('parcel_id')
# parcels_df['zoning_id'] = parcels_df['zoning_id'].astype(str)

# get dataframe from table with updated zoning for parcels
parcel_updates_sql = 'SELECT zoning_schedule_id, parcel_id, zoning_id FROM staging.parcel_zoning_schedule'
parcel_updates_df = pd.read_sql(parcel_updates_sql, urbansim_engine)

parcels_df['zoning_schedule_id'] = 1 # to track which parcels have orig zoning
parent = settings['zoning_schedule_id'] # zoning schedule id from settings.yaml

# create list of zoning schedule ids w parent to each
# e.g. [3, 2, 1] where 3 has parent 2 that has parent 1
zoning_sched_ids = []
while not math.isnan(parent):
    zoning_sched_ids.append(parent)
    parent = zoning_schedule_df['parent_zoning_schedule_id'][zoning_schedule_df['zoning_schedule_id'] == parent].values[0]

zoning_sched_ids = zoning_sched_ids[:-1] # remove last parent id (i.e. 1), bc assuming parcel table is parent

parcels_df['original_zoning_id'] = parcels_df['zoning_id']
# replace parcel table zoning starting with lowest zoning schedule id to highest id
# e.g. replace parcel table zoning with parcel zoning schedule id=2 and then with id=3
for zsid in reversed(zoning_sched_ids):
    parcel_update_zsid = parcel_updates_df[parcel_updates_df['zoning_schedule_id'] == zsid]
    parcel_update_zsid = parcel_update_zsid.set_index(['parcel_id'])
    parcels_df.loc[parcels_df.index.isin(parcel_update_zsid.index), ['zoning_id', 'zoning_schedule_id']] = parcel_update_zsid[['zoning_id', 'zoning_schedule_id']]

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
    store.put('zoning', zoning_df, format='t')
    #store.put('assessor_transactions', assessor_transactions_df, format='t')