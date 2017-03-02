import orca
import models, datasources, variables
import sys
from sd_utils import to_database, get_git_hash, update_scenario
from sqlalchemy import create_engine
from pysandag.database import get_connection_string
import pandas as pd
import os
import logging

# logging.basicConfig(filename='log.txt',level=logging.INFO)

# orig_stdout = sys.stdout
# f = file('data\\stdout.txt', 'w')
# sys.stdout = f
try:
    os.remove('data/results.h5')
except OSError:
    pass

rng = range(2015, 2020)
scenario = 'Solana_Beach_zsid1'

orca.run(['build_networks'])


orca.run(['scheduled_development_events',
         'neighborhood_vars', 'rsh_simulate', 'nrh_simulate2', 'jobs_transition', "elcm_simulate",
          'households_transition', "hlcm_simulate", "price_vars", "feasibility2",
          "residential_developer", "non_residential_developer"
          ], iter_vars=rng, data_out='data\\results.h5', out_interval=1)
# residential only
"""
orca.run(['scheduled_development_events',
          'neighborhood_vars',
          'rsh_simulate',
          'households_transition',
          "hlcm_simulate",
          "price_vars",
          "feasibility2",
          "residential_developer"
          ], iter_vars=rng, data_out='data\\results.h5', out_interval=1)
"""
orca.get_table('nodes').to_frame().to_csv('data/nodes.csv')
orca.get_table('buildings').to_frame().to_csv('data/buildings.csv')
orca.get_table('households').to_frame().to_csv('data/households.csv')
orca.get_table('jobs').to_frame().to_csv('data/jobs.csv')
orca.get_table('feasibility').to_frame().to_csv('data/feasibility.csv')
orca.get_table('parcels').to_frame().to_csv('data/parcels.csv')

# sys.stdout = orig_stdout
# f.close()
# Base year is referred to by 0

update_scenario(scenario)

rng2 = [0] + rng[1:len(rng)]
to_database(scenario,  rng=rng2)
