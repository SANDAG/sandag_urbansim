import urbansim.sim.simulation as sim
from urbansim_defaults import models
import datasources
import models
import variables

sim.run(["build_networks", "neighborhood_vars"])

sim.run(["rsh_estimate"])