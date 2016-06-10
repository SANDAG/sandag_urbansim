
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import pandas as pd

urbansim_engine = create_engine(get_connection_string("configs/dbconfig.yml", 'urbansim_database'))

nodes_sql = 'SELECT node as node_id, x, y, on_ramp FROM urbansim.nodes'
# Necessary to duplicate nodes in order to generate built environment variables for the regessions
intersection_sql = 'SELECT node as intersection_id, x, y FROM urbansim.nodes'
edges_sql = 'SELECT from_node as [from], to_node as [to], distance as [weight] FROM urbansim.edges'
parcels_sql = 'SELECT parcel_id, development_type_id, luz_id, parcel_acres as acres, zoning_id, centroid.STX as x, centroid.STY as y, distance_to_coast, distance_to_freeway FROM urbansim.parcels'
buildings_sql = 'SELECT building_id, parcel_id, development_type_id as building_type_id, COALESCE(residential_units, 0) as residential_units, COALESCE(residential_sqft, 0) as residential_sqft, COALESCE(non_residential_sqft,0) as non_residential_sqft, 0 as non_residential_rent_per_sqft, COALESCE(year_built, -1) year_built, COALESCE(stories, 1) as stories FROM urbansim.buildings'
households_sql = 'SELECT household_id, building_id, persons, age_of_head, income, children FROM urbansim.households'
jobs_sql = 'SELECT job_id, building_id, sector_id FROM urbansim.jobs'
building_sqft_per_job_sql = 'SELECT luz_id, development_type_id, sqft_per_emp FROM urbansim.building_sqft_per_job'
scheduled_development_events_sql = """SELECT
                                         scheduled_development_event_id, parcel_id, development_type_id as building_type_id
                                         ,year_built, sqft_per_unit, residential_units, non_residential_sqft
                                         ,improvement_value, res_price_per_sqft, nonres_rent_per_sqft as non_residential_rent_per_sqft
                                         ,COALESCE(stories,1) as stories FROM urbansim.scheduled_development_event"""
schools_sql = """SELECT objectID as id, Shape.STX as x ,Shape.STY as y FROM gis.schools WHERE SOCType IN ('Junior High Schools (Public)','K-12 Schools (Public)','Preschool','Elemen Schools In 1 School Dist. (Public)','Elementary Schools (Public)','Intermediate/Middle Schools (Public)','High Schools (Public)','Private')"""
parks_sql = """SELECT subparcel as park_id, shape.STCentroid().STX x, shape.STCentroid().STY y FROM gis.landcore WHERE lu IN (7207,7210,7211,7600,7601,7604,7605)"""
transit_sql = 'SELECT x, y, stopnum FROM gis.transit_stops'
household_controls_sql = """SELECT yr as [year], hh_income_id as income_quartile, hh FROM isam.defm.households WHERE dem_version = 'S0021' and eco_version = '001' AND yr >= 2015"""
employment_controls_sql = """SELECT yr as [year], jobs as number_of_jobs, sector_id FROM isam.defm.jobs WHERE dem_version = 'S0021' and eco_version = '001' AND yr >= 2015"""
zoning_allowed_uses_sql = """SELECT development_type_id, zoning_id FROM urbansim.zoning_allowed_use ORDER BY development_type_id, zoning_id"""
fee_schedule_sql = """SELECT development_type_id, development_fee_per_unit_space_initial FROM urbansim.fee_schedule"""
zoning_sql = """SELECT zoning_id, max_dua, max_building_height as max_height, max_far FROM urbansim.zoning"""

assessor_transactions_sql = """SELECT parcel_id, tx_price FROM (SELECT parcel_id, RANK() OVER (PARTITION BY parcel_id ORDER BY tx_date) as tx, tx_date, tx_price FROM estimation.assessor_par_transactions) x WHERE tx = 1"""

nodes_df = pd.read_sql(nodes_sql, urbansim_engine, index_col='node_id')
intersection_df = pd.read_sql(intersection_sql, urbansim_engine, index_col='intersection_id')
edges_df = pd.read_sql(edges_sql, urbansim_engine)
parcels_df = pd.read_sql(parcels_sql, urbansim_engine, index_col='parcel_id')
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
zoning_df = pd.read_sql(zoning_sql, urbansim_engine, index_col='zoning_id')
assessor_transactions_df = pd.read_sql(assessor_transactions_sql, urbansim_engine)

building_sqft_per_job_df.sort_values(['luz_id', 'development_type_id'], inplace=True)
building_sqft_per_job_df.set_index(['luz_id', 'development_type_id'], inplace=True)

edges_df.sort_values(['from', 'to'], inplace=True)
#edges_df.set_index(['from', 'to'], inplace=True)

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
    store.put('assessor_transactions', assessor_transactions_df, format='t')