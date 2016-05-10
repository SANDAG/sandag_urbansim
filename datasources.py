import numpy as np
import pandas as pd

import urbansim.sim.simulation as sim
import urbansim.utils.misc as misc


@sim.table('building_sqft_per_job', cache=True)
def building_sqft_per_job(store):
    return store['building_sqft_per_job']


@sim.table('employment_controls', cache=True)
def employment_controls(store):
    return store['employment_controls']


@sim.table('fee_schedule', cache=True)
def fee_schedule(store):
    return store['fee_schedule']


@sim.table('household_controls', cache=True)
def household_controls(store):
    return store['household_controls']


@sim.table('scheduled_development_events', cache=True)
def scheduled_development_events(store):
    return store['scheduled_development_events']


@sim.table('zoning', cache=True)
def zoning(store):
    return store['zoning']


@sim.table('zoning_allowed_uses', cache=True)
def zoning_allowed_uses(store, parcels):
    zoning_allowed_uses_df = store['zoning_allowed_uses']
    parcels = parcels.to_frame(columns = ['zoning_id',])
    allowed_df = pd.DataFrame(index=parcels.index)

    for devtype in np.unique(zoning_allowed_uses_df.index.values):
        devtype_allowed = zoning_allowed_uses_df.loc[devtype].reset_index().set_index('zoning_id')
        allowed = misc.reindex(devtype_allowed.development_type_id, parcels.zoning_id)
        df = pd.DataFrame(data=False, index=allowed.index, columns=['allowed'])
        df[~allowed.isnull()] = True
        allowed_df[devtype] = df.allowed

    return allowed_df


sim.broadcast('zoning', 'parcels', cast_index=True, onto_on='zoning_id')