import numpy as np
import pandas as pd
from pysandag.database import get_connection_string
from sqlalchemy import create_engine
from urbansim.models.lcm import unit_choice

urbansim_engine = create_engine(get_connection_string("E:/Apps/urbansim/sandag_urbansim_rebuild/configs/dbconfig.yml", 'urbansim_database'))

def random_allocate_households(housholds, buildings, mgra_id_col, units_col):
    audit_df = pd.DataFrame(
                    data=np.zeros((len(np.unique(households[mgra_id_col])), 3), dtype=np.int)
                    ,index=np.unique(households[mgra_id_col])
                    ,columns=['demand','supply','residual'])
    
    empty_units = buildings[buildings[units_col] > 0][units_col].sort_values(ascending=False)
    alternatives = buildings[['development_type_id', 'parcel_id', mgra_id_col]]
    alternatives = alternatives.ix[np.repeat(empty_units.index.values, empty_units.values.astype('int'))]
    
    mgra_agent_counts = households.groupby(mgra_id_col).size()
    
    for mgra in np.unique(households[mgra_id_col]):
        print "Processing MGRA: %s" % (mgra)
        num_households = mgra_agent_counts[mgra_agent_counts.index.values == mgra].values[0]
        chooser_ids = households.index[households[mgra_id_col] == mgra].values
        alternative_ids = alternatives[alternatives[mgra_id_col] == mgra].index.values
        probabilities = np.ones(len(alternative_ids))
        num_units = len(alternative_ids)
        choices = unit_choice(chooser_ids, alternative_ids, probabilities)
        households.loc[chooser_ids, 'building_id'] = choices
        audit_df.ix[mgra] = [num_households, num_units, num_units - num_households]
    
    return audit_df

def random_allocate_jobs(jobs, buildings, block_id_col, jobs_col):
        audit_df = pd.DataFrame(
                    data=np.zeros((len(np.unique(jobs[block_id_col])), 3), dtype=np.int)
                    ,index=np.unique(jobs[block_id_col])
                    ,columns=['demand','supply','residual'])
        
        empty_units = buildings[buildings[jobs_col] > 0][jobs_col].sort_values(ascending=False)
        alternatives = buildings[['development_type_id', 'parcel_id', block_id_col]]
        alternatives = alternatives.ix[np.repeat(empty_units.index.values, empty_units.values.astype('int'))]
        
        block_agent_counts = jobs.groupby(block_id_col).size()
        blocks = len(np.unique(jobs['block_id']))
        
        for idx, block in enumerate(np.unique(jobs[block_id_col])):
            print "Processing Block %s of %s: %s" % (idx, blocks, block)
            num_jobs = block_agent_counts[block_agent_counts.index.values == block].values[0]
            chooser_ids = jobs.index[jobs[block_id_col] == block].values
            alternative_ids = alternatives[alternatives[block_id_col] == block].index.values
            probabilities = np.ones(len(alternative_ids))
            num_units = len(alternative_ids)
            choices = unit_choice(chooser_ids, alternative_ids, probabilities)
            jobs.loc[chooser_ids, 'building_id'] = choices
            audit_df.ix[block] = [num_jobs, num_units, num_units - num_jobs]

        return audit_df

def process_households():

    bldgs_sql = """SELECT 
                    id as building_id, bldg.development_type_id, bldg.parcel_id, improvement_value, residential_units, residential_sqft
                    ,non_residential_sqft, price_per_sqft, stories, year_built, p.mgra_id as mgra
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

    results_df = random_allocate_households(households, buildings, 'mgra', 'residential_units')
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
                    p.parcel_id, bldg.id as building_id, bldg.development_type_id, bldg.non_residential_sqft
                    ,p.luz_id, p.block_id, usage.sqft_per_emp
                  FROM
                    urbansim.buildings bldg
                    INNER JOIN urbansim.parcels p ON bldg.parcel_id = p.parcel_id 
                    INNER JOIN input.sqft_per_job_by_devtype usage
                        ON bldg.development_type_id = usage.development_type_id AND p.luz_id = usage.luz_id"""

    # Get 2015 Households from ABM
    jobs_sql =  """SELECT block_id, job_id, sector_id FROM [input].[jobs_wac_2013]"""

    buildings = pd.read_sql(bldgs_sql, urbansim_engine, index_col='building_id')
    jobs = pd.read_sql(jobs_sql, urbansim_engine, index_col='job_id')
    buildings.ix[buildings.sqft_per_emp < 40, 'sqft_per_emp'] = 40
    buildings['job_spaces'] = np.ceil(buildings.non_residential_sqft / buildings.sqft_per_emp)
    buildings.ix[buildings.job_spaces.isnull(), 'job_spaces'] = 0
    buildings['job_spaces'] = buildings['job_spaces'].astype('int')
    
    results_df = random_allocate_jobs(jobs, buildings, 'block_id', 'job_spaces')
    results_df.to_csv('process_jobs_results.csv')
    
    if 'block_id' in jobs.columns:
        del jobs['block_id']
        
    jobs.ix[jobs.building_id.isnull(), 'building_id'] = -1
    
    jobs.to_csv('jobs.csv')
    

if __name__ == '__main__':
    #process_households()
    process_jobs()