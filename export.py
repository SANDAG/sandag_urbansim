
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import pandas as pd
from urbansim_defaults import datasources

urbansim_engine = create_engine(get_connection_string("configs/dbconfig.yml", 'urbansim_database'))

zsid = datasources.settings()['zoning_schedule_id']

nodes_sql = 'SELECT node as node_id, x, y, on_ramp FROM urbansim.nodes'
# Necessary to duplicate nodes in order to generate built environment variables for the regessions
intersection_sql = 'SELECT node as intersection_id, x, y FROM urbansim.nodes'
edges_sql = 'SELECT from_node as from, to_node as to, distance as weight FROM urbansim.edges'

parcels_sql = '''SELECT p.parcel_id, p.development_type_id,
                        p.luz_id, p.parcel_acres as acres,
                        ST_X(ST_Transform(centroid::geometry, 2230)) as x,
                        ST_Y(ST_Transform(centroid::geometry, 2230)) as y,
                        COALESCE(p.distance_to_coast,10000) as distance_to_coast, COALESCE(p.distance_to_freeway,10000) as distance_to_freeway,
                        sp.siteid
                       ,zp.zoning_schedule_id,zp.zoning_id
                   FROM urbansim.parcels p
                   JOIN urbansim.zoning_parcels zp
                     ON p.parcel_id = zp.parcel_id
                   LEFT JOIN urbansim.scheduled_development_parcels sp
                          ON p.parcel_id = sp.parcel_id
                   WHERE zp.zoning_schedule_id = ''' + str(zsid)

buildings_sql = 'SELECT building_id, parcel_id, COALESCE(development_type_id,0) as building_type_id, COALESCE(residential_units, 0) as residential_units, COALESCE(residential_sqft, 0) as residential_sqft, COALESCE(non_residential_sqft,0) as non_residential_sqft, 0 as non_residential_rent_per_sqft, COALESCE(year_built, 0) as year_built, COALESCE(stories, 1) as stories FROM urbansim.buildings'
households_sql = 'SELECT household_id, building_id, persons, age_of_head, income, children FROM urbansim.households'
jobs_sql = 'SELECT job_id, building_id, sector_id FROM urbansim.jobs'
building_sqft_per_job_sql = 'SELECT luz_id, development_type_id, sqft_per_emp FROM urbansim.building_sqft_per_job'
scheduled_development_events_sql =  '''WITH parcels_for_sched_dev AS
                                             (SELECT siteid, count(parcel_id) as parcel_count
                                                FROM urbansim.scheduled_development_parcels
                                            GROUP BY siteid)
                                        SELECT sd."siteID", sp.parcel_id,
                                               sd."devTypeID" as building_type_id,
                                               EXTRACT(YEAR FROM "compDate")  as year_built,
                                               COALESCE(sfu,0) + COALESCE(mfu,0) AS total_units,
                                               "nResSqft" as non_residential_sqft,
                                               "resSqft" as residential_sqft,
                                               NULL as non_residential_rent_per_sqft,
                                               NULL as stories,
                                               (COALESCE(sfu,0) + COALESCE(mfu,0))/parcel_count AS residential_units
                                          FROM urbansim.scheduled_development_parcels sp
                                          JOIN urbansim.scheduled_development sd
                                            ON sp.siteid = sd."siteID"
                                          JOIN parcels_for_sched_dev parcels_sd
                                            ON parcels_sd.siteid = sp.siteid'''
schools_sql = """SELECT id, x ,y FROM urbansim.schools"""
parks_sql = """SELECT park_id,  x, y FROM urbansim.parks"""
transit_sql = 'SELECT x, y, stopnum FROM urbansim.transit'
household_controls_sql = """SELECT yr as year, income_quartile, households as hh FROM urbansim.household_controls"""
employment_controls_sql = """SELECT yr as year, number_of_jobs, sector_id FROM urbansim.employment_controls"""
zoning_allowed_uses_sql = """SELECT development_type_id, zoning_id FROM urbansim.zoning_allowed_use ORDER BY development_type_id, zoning_id"""
fee_schedule_sql = """SELECT development_type_id, development_fee_per_unit_space_initial FROM urbansim.fee_schedule"""
zoning_sql = """SELECT zoning_id, max_dua, max_building_height as max_height, max_far, max_res_units FROM urbansim.zoning"""


assessor_transactions_sql = """SELECT parcel_id, tx_price FROM (SELECT parcel_id, RANK() OVER (PARTITION BY parcel_id ORDER BY tx_date) as tx, tx_date, tx_price FROM estimation.assessor_par_transactions) x WHERE tx = 1"""

nodes_df = pd.read_sql(nodes_sql, urbansim_engine, index_col='node_id')
intersection_df = pd.read_sql(intersection_sql, urbansim_engine, index_col='intersection_id')
edges_df = pd.read_sql(edges_sql, urbansim_engine)
parcels_df = pd.read_sql(parcels_sql, urbansim_engine, index_col='parcel_id')
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