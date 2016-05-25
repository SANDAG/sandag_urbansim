import orca
import models, datasources, variables

orca.run(['build_networks'])

orca.run([#'scheduled_development_events'
         'neighborhood_vars','rsh_simulate','nrh_simulate','nrh_simulate2'
         ,'jobs_transition',"elcm_simulate",'households_transition', "hlcm_simulate"
         ,"price_vars","feasibility","residential_developer","non_residential_developer"
], iter_vars=range(2015,2016))

orca.get_table('nodes').to_frame().to_csv('data/nodes.csv')
orca.get_table('buildings').to_frame().to_csv('data/buildings.csv')
orca.get_table('households').to_frame(['household_id', 'building_id', 'persons', 'age_of_head', 'income', 'income_quartile']).to_csv('data/households.csv')
orca.get_table('jobs').to_frame(['job_id', 'building_id', 'sector_id']).to_csv('data/jobs.csv')
orca.get_table('feasibility').to_frame().to_csv('data/feasibility.csv')
orca.get_table('parcels').to_frame().to_csv('data/parcels.csv')
