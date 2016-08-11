import orca
import models, datasources, variables
import sys

# orig_stdout = sys.stdout
# f = file('data\\stdout.txt', 'w')
# sys.stdout = f

orca.run(['build_networks'])

# residential only
orca.run(['scheduled_development_events',
          'neighborhood_vars',
          'rsh_simulate',
          'households_transition',
          "hlcm_simulate",
          "price_vars",
          "feasibility2",
          "residential_developer"
          ], iter_vars=range(2015,2050),data_out='data\\results.h5', out_interval=1)

orca.get_table('nodes').to_frame().to_csv('data/nodes.csv')
orca.get_table('buildings').to_frame().to_csv('data/buildings.csv')
orca.get_table('households').to_frame().to_csv('data/households.csv')
# orca.get_table('jobs').to_frame().to_csv('data/jobs.csv')
orca.get_table('feasibility').to_frame().to_csv('data/feasibility.csv')
orca.get_table('parcels').to_frame().to_csv('data/parcels.csv')

# sys.stdout = orig_stdout
# f.close()



