import numpy as np
import pandas as pd
from pysandag.database import get_connection_string
from sqlalchemy import create_engine
from urbansim.models.lcm import unit_choice

urbansim_engine = create_engine(get_connection_string("d:/dev/sandag_urbansim_rebuild/configs/dbconfig.yml", 'urbansim_database'))
#, legacy_schema_aliasing=False)

def random_allocate_agents_by_geography(agents, containers, geography_id_col, containers_units_col):
    """Allocate agents (e.g., households, jobs) to a container (e.g., buildings) based
       on the number of units available in each container. The agent and container unit
       totals are controled to a geography.

    :param agents: A dataframe with agents to be assigned.
    :param containers: A dataframe to which the agents will be assigned
    :param geography_id_col: The column id in both input dataframes for identifying the control geography zones
    :param containers_units_col: The column in the container dataframe enumerating number of slots in container for agents
    :type agents: pandas.DataFrame
    :type containers: pandas.DataFrame
    :type geography_id_col: string
    :type containers_units_col: string
    :return: Summary dataframe of allocation
    :rtype: pandas.DataFrame
    """
    audit_df = pd.DataFrame(
                    data=np.zeros((len(np.unique(agents[geography_id_col])), 3), dtype=np.int)
                    ,index=np.unique(agents[geography_id_col])
                    ,columns=['demand','supply','residual'])
    
    empty_units = containers[containers[containers_units_col] > 0][containers_units_col].sort_values(ascending=False)
    alternatives = containers[[geography_id_col]]
    alternatives = alternatives.ix[np.repeat(empty_units.index.values, empty_units.values.astype('int'))]
    
    geography_agent_counts = agents.groupby(geography_id_col).size()
    zones = len(np.unique(agents[geography_id_col]))

    for idx, geo_zone in enumerate(np.unique(agents[geography_id_col])):
        print "Processing %s %s of %s (%s = %s)" % (geography_id_col, idx, zones, geography_id_col, geo_zone)

        num_agents = geography_agent_counts[geography_agent_counts.index.values == geo_zone].values[0]
        chooser_ids = agents.index[agents[geography_id_col] == geo_zone].values
        alternative_ids = alternatives[alternatives[geography_id_col] == geo_zone].index.values
        probabilities = np.ones(len(alternative_ids))
        num_units = len(alternative_ids)
        choices = unit_choice(chooser_ids, alternative_ids, probabilities)
        agents.loc[chooser_ids, containers.index.name] = choices
        audit_df.ix[geo_zone] = [num_agents, num_units, num_units - num_agents]
    
    return audit_df


def process_households():

    bldgs_sql = """SELECT 
                    bldg.building_id, bldg.residential_units, p.mgra_id as mgra
                  FROM 
                    urbansim.buildings bldg
                    INNER JOIN spacecore.urbansim.parcels p ON bldg.parcel_id = p.parcel_id"""

    # Get 2015 Households from ABM
    hh_sql =  """SELECT
                   scenario_id, lu_hh_id as household_id, building_id, mgra, tenure, persons, workers, age_of_head, income, children
                   ,race_id, cars
                 FROM
                    input.household(127)"""
               
    buildings = pd.read_sql(bldgs_sql, urbansim_engine, index_col='building_id')
    households = pd.read_sql(hh_sql, urbansim_engine, index_col='household_id')

    results_df = random_allocate_agents_by_geography(households, buildings, 'mgra', 'residential_units')
    results_df.to_csv('process_households_results.csv')
    
    if 'mgra' in households.columns:
        del households['mgra']
    
    households.ix[households.building_id.isnull(), 'building_id'] = -1
    households['tenure'] = -1
    
    for col in households.columns:
        households[col] = households[col].astype('int')
        
    households.to_csv('households.csv')
    households.to_sql('households', urbansim_engine, schema='urbansim', if_exists='replace', chunksize=1000)


def process_jobs():
    bldgs_sql = """SELECT
                    bldg.building_id, bldg.job_spaces, p.block_id
                  FROM
                    urbansim.buildings bldg
                    INNER JOIN urbansim.parcels p ON bldg.parcel_id = p.parcel_id"""

    # Get 2015 Households from ABM
    jobs_sql =  """SELECT block_id, job_id, sector_id FROM [input].[jobs_wac_2013]"""

    buildings = pd.read_sql(bldgs_sql, urbansim_engine, index_col='building_id')
    jobs = pd.read_sql(jobs_sql, urbansim_engine, index_col='job_id')
    
    results_df = random_allocate_agents_by_geography(jobs, buildings, 'block_id', 'job_spaces')
    results_df.to_csv('process_jobs_results.csv')
    
    if 'block_id' in jobs.columns:
        del jobs['block_id']
        
    jobs.ix[jobs.building_id.isnull(), 'building_id'] = -1
    jobs['building_id'] = jobs['building_id'].astype('int')
    
    jobs.to_csv('jobs.csv')
    jobs.to_sql('jobs', urbansim_engine, schema='urbansim', if_exists='replace', chunksize=1000)
    

def process_residential_units():
    """Allocate total parcel DU to buildings. Only run unit choice allocation model for those parcels in which there
    is more than one DU and more than one building on the parcel."""

    # Select buildings from buildings table that only have one building on the parcel.
    single_bldg_units_sql = """
      SELECT building_id
        ,landcore.du as units
      FROM urbansim.buildings
          LEFT JOIN
              (SELECT parcelId, SUM(du) du
              FROM gis.landcore
              GROUP BY parcelID) landcore
          ON landcore.parcelID = buildings.parcel_id
      WHERE parcel_id IN
        (SELECT parcel_id
         FROM urbansim.buildings
         GROUP BY parcel_id
         HAVING COUNT(*) = 1)"""
    units = pd.read_sql(single_bldg_units_sql, urbansim_engine, index_col='building_id')

    # Get buildings that have > 1 DU. These will be the buildings we allocate units to.
    multi_unit_multi_bldgs_sql = """
        SELECT building_id
          ,parcel_id
          ,landcore.du as residential_units
        FROM urbansim.buildings
        LEFT JOIN
            (SELECT parcelId, SUM(du) du
             FROM gis.landcore
             GROUP BY parcelID) landcore
             ON landcore.parcelID = buildings.parcel_id
        WHERE parcel_id IN
            (SELECT parcel_id
             FROM urbansim.buildings
             GROUP BY parcel_id
             HAVING COUNT(*) > 1)
        AND du > 0"""
    buildings = pd.read_sql(multi_unit_multi_bldgs_sql, urbansim_engine, index_col='building_id')

    units_to_allocate_sql = """
        SELECT id as unit_id
            ,parcel_id
        FROM input.landcore_units
        WHERE parcel_id IN
          (SELECT parcel_id
           FROM urbansim.buildings
           GROUP BY parcel_id
           HAVING COUNT(*) > 1)
        ORDER BY parcel_id"""
    units_to_allocate = pd.read_sql(units_to_allocate_sql, urbansim_engine, index_col='unit_id')

    results_df = random_allocate_agents_by_geography(units_to_allocate, buildings, 'parcel_id', 'residential_units')
    results_df.to_csv('process_residential_units_results')

    # Sum allocated unit qty by building_id
    updated_units = units_to_allocate.groupby('building_id').agg('count')
    updated_units.columns = ['units']
    updated_units.to_csv('unit_assignment.csv')

    # Append units that were allocated to units not needing allocation, and write to SQL.
    units = units.append(updated_units)
    units.to_sql('unit_assignment', urbansim_engine, schema='urbansim', if_exists='replace', chunksize=1000)

    # Update the buildings table with the updated residential_units
    update_sql = """
        UPDATE urbansim.buildings
        SET residential_units = ISNULL(unit_assignment.units, 0)
        FROM urbansim.buildings
          LEFT JOIN urbansim.unit_assignment
          ON unit_assignment.building_id = buildings.building_id
    """
    conn = urbansim_engine.connect()
    conn.execute(update_sql)
    conn.close()

if __name__ == '__main__':
    process_residential_units()
    process_households()
    process_jobs()
