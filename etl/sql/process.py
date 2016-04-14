import numpy as np
import pandas as pd
from pysandag.database import get_connection_string
from sqlalchemy import create_engine
from urbansim.models.lcm import unit_choice

urbansim_engine = create_engine(get_connection_string("E:/Apps/urbansim/sandag/configs/dbconfig.yml", 'urbansim_database'))

def random_allocate_households(households, buildings, mgra_id_col, units_col):
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
    #households.to_sql('households', urbansim_engine, schema='urbansim', if_exists='replace')
    
if __name__ == '__main__':
    process_households()