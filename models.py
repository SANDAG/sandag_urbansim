import pandana as pdna
import pandas as pd
from urbansim.developer import sqftproforma
import orca
from urbansim_defaults import models
from urbansim_defaults import utils
from urbansim.developer import developer
import numpy as np
from pysandag.database import get_connection_string
import math
import psycopg2
import getpass
import datetime



###  ESTIMATIONS  ##################################
@orca.step('rsh_estimate')
def rsh_estimate(assessor_transactions, aggregations):
    return utils.hedonic_estimate("rsh.yaml", assessor_transactions, aggregations)


def get_year():
    year = orca.get_injectable('year')
    if year is None:
        year = 2016
    return year


### SIMULATIONS ####################################
@orca.step('build_networks')
def build_networks(settings , store, parcels, intersections):
    edges, nodes = store['edges'], store['nodes']
    net = pdna.Network(nodes["x"], nodes["y"], edges["from"], edges["to"],
                       edges[["weight"]])

    max_distance = settings['build_networks']['max_distance']
    net.precompute(max_distance)

    #SETUP POI COMPONENTS
    on_ramp_nodes = nodes[nodes.on_ramp]
    net.init_pois(num_categories=4, max_dist=max_distance, max_pois=1)
    net.set_pois('onramps', on_ramp_nodes.x, on_ramp_nodes.y)

    parks = store.parks
    net.set_pois('parks', parks.x, parks.y)

    schools = store.schools
    net.set_pois('schools', schools.x, schools.y)

    transit = store.transit
    net.set_pois('transit', transit.x, transit.y)

    orca.add_injectable("net", net)

    p = parcels.to_frame(parcels.local_columns)
    i = intersections.to_frame(intersections.local_columns)

    p['node_id'] = net.get_node_ids(p['x'], p['y'])
    i['node_id'] = net.get_node_ids(i['x'], i['y'])

    #p.to_csv('data/parcels.csv')
    orca.add_table("parcels", p)
    orca.add_table("intersections", i)


@orca.step('rsh_simulate')
def rsh_simulate(settings, buildings, aggregations):
    yaml_cfg = settings['rsh_yaml']
    return utils.hedonic_simulate(yaml_cfg, buildings, aggregations,
                                  "residential_price")


"""
@sim.model('feasibility')
def feasibility(parcels, settings, fee_schedule,
                #parcel_sales_price_sqft_func,
                parcel_is_allowed_func):
    # Fee table preprocessing
    #fee_schedule = sim.get_table('fee_schedule').to_frame()
    #parcel_fee_schedule = sim.get_table('parcel_fee_schedule').to_frame()
    parcels = parcels.to_frame(columns = ['zoning_id','development_type_id'])
    #fee_schedule = fee_schedule.groupby(['fee_schedule_id', 'development_type_id']).development_fee_per_unit_space_initial.mean().reset_index()
    #parcel_use_allowed_callback = sim.get_injectable('parcel_is_allowed_func')

    def run_proforma_lookup(parcels, fees, pf, use, form, residential_to_yearly, parcel_filter = None):
        if parcel_filter:
            parcels = parcels.query(parcel_filter)
        # add prices for each use (rents).  Apply fees
        parcels[use] = misc.reindex(sim.get_table('nodes')[use], sim.get_table('parcels').node_id) - fees

        #Calibration shifters
        calibration_shifters = ['feasibility']['msa_id']
        #calibration_shifters = pd.read_csv('.\\data\\calibration\\msa_shifters.csv').set_index('msa_id').to_dict()

        if use == 'residential':
            shifter_name = 'res_price_shifter'
        else:
            shifter_name = 'nonres_price_shifter'
        parcels[shifter_name] = 1.0
        shifters = calibration_shifters[shifter_name]
        for msa_id in shifters.keys():
            shift = shifters[msa_id]
            parcels[shifter_name][parcels.msa_id == msa_id] = shift

        parcels[use] = parcels[use] * parcels[shifter_name]

        #LUZ shifter
        if use == 'residential':
            #target_luz = pd.read_csv('.\\data\\calibration\\target_luz.csv').values.flatten()
            #luz_shifter = pd.read_csv('.\\data\\calibration\\luz_du_shifter.csv').values[0][0]
            target_luz = np.array(settings['feasibility']['target_luz'].values(), dtype='int64')
            luz_shifter = settings['feasibility']['luz_shifter'].values()[0]
            parcels[use][parcels.luz_id.isin(target_luz)] = parcels[use][parcels.luz_id.isin(target_luz)] * luz_shifter

        # convert from cost to yearly rent
        if residential_to_yearly:
            parcels[use] *= pf.config.cap_rate

        # Price minimum if hedonic predicts outlier
        parcels[use][parcels[use] <= .5] = .5
        parcels[use][parcels[use].isnull()] = .5

        print "Describe of the yearly rent by use"
        print parcels[use].describe()
        allowed = parcel_is_allowed_func(form).loc[parcels.index]
        #allowed = parcel_use_allowed_callback(form).loc[parcels.index]
        feasibility = pf.lookup(form, parcels[allowed], only_built=True,
                                    pass_through=[])

        if use == 'residential':
            def iter_feasibility(feasibility, price_scaling_factor):
                if price_scaling_factor > 3.0:
                    return feasibility
                # Get targets
                target_units = residential_space_targets()[form]
                #Calculate number of profitable units
                d = {}
                d[form] = feasibility
                feas = pd.concat(d.values(), keys=d.keys(), axis=1)
                dev = developer.Developer(feas)
                profitable_units = run_developer(dev, form, target_units, get_year(), build = False)

                print 'Feasibility given current prices/zonining indicates %s profitable units and target of %s' % (profitable_units, target_units)

                if profitable_units < target_units:
                    price_scaling_factor += .1
                    print 'Scaling prices up by factor of %s' % price_scaling_factor
                    parcels[use] = parcels[use] * price_scaling_factor
                    feasibility = pf.lookup(form, parcels[allowed], only_built=True,
                                        pass_through=[])

                    return iter_feasibility(feasibility, price_scaling_factor)
                else:
                    price_scaling_factor += .1
                    parcels[use] = parcels[use] * price_scaling_factor
                    feasibility = pf.lookup(form, parcels[allowed], only_built=True,
                                        pass_through=[])
                    return feasibility
            feasibility = iter_feasibility(feasibility, 1.0)

        elif use != 'residential':
            def iter_feasibility(feasibility, price_scaling_factor):
                if price_scaling_factor > 3.0:
                    return feasibility
                # Get targets
                targets = non_residential_space_targets()
                target_units = targets[form]/400
                #Calculate number of profitable units
                feasibility['current_units'] = parcels.total_job_spaces
                feasibility["parcel_size"] = parcels.parcel_size
                feasibility = feasibility[feasibility.parcel_size < 200000]
                feasibility['job_spaces'] = np.round(feasibility.non_residential_sqft / 400.0)
                feasibility['net_units'] = feasibility.job_spaces - feasibility.current_units
                feasibility.net_units = feasibility.net_units.fillna(0)
                profitable_units = int(feasibility.net_units.sum())
                print 'Feasibility given current prices/zonining indicates %s profitable units and target of %s' % (profitable_units, target_units)

                if profitable_units < target_units:
                    price_scaling_factor += .1
                    print 'Scaling prices up by factor of %s' % price_scaling_factor
                    parcels[use] = parcels[use] * price_scaling_factor
                    feasibility = pf.lookup(form, parcels[allowed], only_built=True,
                                        pass_through=[])

                    return iter_feasibility(feasibility, price_scaling_factor)
                else:
                    return feasibility
            feasibility = iter_feasibility(feasibility, 1.0)

        print len(feasibility)
        return feasibility

    def residential_proforma(form, devtype_id, parking_rate):
        print form
        use = 'residential'
        parcels = sim.get_table('parcels').to_frame()

        residential_to_yearly = True
        parcel_filter = settings['feasibility']['parcel_filter']
        #parcel_filter = None
        pfc = sqftproforma.SqFtProFormaConfig()
        pfc.forms = {form: {use : 1.0}}
        pfc.uses = [use]
        pfc.residential_uses = [True]
        pfc.parking_rates = {use : parking_rate}
        pfc.costs = {use : [170.0, 190.0, 210.0, 240.0]}

        #Fees
        fees = pd.Series(data=fee_schedule.loc[devtype_id].development_fee_per_unit_space_initial, index=parcels.index)
        fees = fees.rename('development_fee_per_square_unit')
        #fee_schedule_devtype = fee_schedule[fee_schedule.development_type_id == devtype_id]
        #parcel_fee_schedule_devtype = pd.merge(parcel_fee_schedule, fee_schedule_devtype, left_on = 'fee_schedule_id', right_on = 'fee_schedule_id')
        #parcel_fee_schedule_devtype['development_fee_per_unit'] = parcel_fee_schedule_devtype.development_fee_per_unit_space_initial*parcel_fee_schedule_devtype.portion
        #parcel_fees_processed = parcel_fee_schedule_devtype.groupby('parcel_id').development_fee_per_unit.sum()
        #fees = pd.Series(data = parcel_fees_processed, index = parcels.index).fillna(0)

        pf = sqftproforma.SqFtProForma(pfc)

        return run_proforma_lookup(parcels, fees, pf, use, form, residential_to_yearly, parcel_filter = parcel_filter)

    def nonresidential_proforma(form, devtype_id, use, parking_rate):
        print form
        parcels = sim.get_table('parcels').to_frame()

        residential_to_yearly = False
        parcel_filter = settings['feasibility']['parcel_filter']
        #parcel_filter = None
        pfc = sqftproforma.SqFtProFormaConfig()
        pfc.forms = {form: {use : 1.0}}
        pfc.uses = [use]
        pfc.residential_uses = [False]
        pfc.parking_rates = {use : parking_rate}
        if use == 'retail':
            pfc.costs = {use : [160.0, 175.0, 200.0, 230.0]}
        elif use == 'industrial':
            pfc.costs = {use : [140.0, 175.0, 200.0, 230.0]}
        else: #office
            pfc.costs = {use : [160.0, 175.0, 200.0, 230.0]}

        #Fees
        fees = pd.Series(data=fee_schedule.loc[devtype_id].development_fee_per_unit_space_initial, index=parcels.index)
        fees = fees.rename('development_fee_per_square_unit')
        #fee_schedule_devtype = fee_schedule[fee_schedule.development_type_id == devtype_id]
        #parcel_fee_schedule_devtype = pd.merge(parcel_fee_schedule, fee_schedule_devtype, left_on = 'fee_schedule_id', right_on = 'fee_schedule_id')
        #parcel_fee_schedule_devtype['development_fee_per_unit'] = parcel_fee_schedule_devtype.development_fee_per_unit_space_initial*parcel_fee_schedule_devtype.portion
        #parcel_fee_schedule = pd.merge(parcels, fee_schedule_devtype, left_on='development_type_id', right_on='development_type_id')
        #parcel_fees_processed = parcel_fee_schedule_devtype.groupby('parcel_id').development_fee_per_unit.sum()
        #fees = pd.Series(data = parcel_fees_processed, index = parcels.index).fillna(0)

        pf = sqftproforma.SqFtProForma(pfc)
        fees = fees*pf.config.cap_rate

        return run_proforma_lookup(parcels, fees, pf, use, form, residential_to_yearly, parcel_filter = parcel_filter)

    d = {}

    ##SF DETACHED proforma (devtype 19)
    form = 'sf_detached'
    devtype_id = 19
    d[form] = residential_proforma(form, devtype_id, parking_rate = 1.0)

    ##SF ATTACHED proforma (devtype 20)
    form = 'sf_attached'
    devtype_id = 20
    d[form] = residential_proforma(form, devtype_id, parking_rate = 1.0)

    ##MF_RESIDENTIAL proforma (devtype 21)
    form = 'mf_residential'
    devtype_id = 21
    d[form] = residential_proforma(form, devtype_id, parking_rate = 1.0)

    ##OFFICE (devtype 4)
    form = 'office'
    devtype_id = 4
    d[form] = nonresidential_proforma(form, devtype_id, form, parking_rate = 1.0)

    ##RETAIL (devtype 5)
    form = 'retail'
    devtype_id = 5
    d[form] = nonresidential_proforma(form, devtype_id, form, parking_rate = 2.0)

    ##LIGHT INDUSTRIAL (devtype 2)
    form = 'light_industrial'
    devtype_id = 2
    d[form] = nonresidential_proforma(form, devtype_id, 'industrial', parking_rate = .6)

    ##HEAVY INDUSTRIAL (devtype 3)
    form = 'heavy_industrial'
    devtype_id = 3
    d[form] = nonresidential_proforma(form, devtype_id, 'industrial', parking_rate = .6)

    far_predictions = pd.concat(d.values(), keys=d.keys(), axis=1)
    sim.add_table("feasibility", far_predictions)
"""

@orca.step('jobs_transition')
def jobs_transition(jobs, employment_controls, year, settings):
    return utils.full_transition(jobs,
                                 employment_controls,
                                 year,
                                 settings['jobs_transition'],
                                 "building_id")


@orca.step('nrh_simulate2')
def nrh_simulate2(buildings, aggregations):
    return utils.hedonic_simulate("nrh2.yaml", buildings, aggregations,
                                  "non_residential_price")

# residential only
@orca.step('scheduled_development_events')
def scheduled_development_events(scheduled_development_events, buildings):
    year = get_year()
    sched_dev = scheduled_development_events.to_frame()
    sched_dev = sched_dev[sched_dev.year_built==year]
    sched_dev = sched_dev[sched_dev.residential_units > 0]
    # sched_dev['residential_sqft'] = sched_dev.sqft_per_unit*sched_dev.residential_units
    #TODO: The simple division here is not consistent with other job_spaces calculations
    sched_dev['job_spaces'] = sched_dev.non_residential_sqft/400
    if len(sched_dev) > 0:
        max_bid = buildings.index.values.max()
        idx = np.arange(max_bid + 1,max_bid+len(sched_dev)+1)
        sched_dev['building_id'] = idx
        sched_dev['building_type_id'] = 21
        sched_dev = sched_dev.set_index('building_id')
        from urbansim.developer.developer import Developer
        merge = Developer(pd.DataFrame({})).merge
        b = buildings.to_frame(buildings.local_columns)
        if 'residential_price' in b.columns:
            sched_dev['residential_price'] = 0
        all_buildings = merge(b,sched_dev[b.columns])

        all_buildings['new_bldg'] = all_buildings.index.isin(idx)

        orca.add_table("buildings", all_buildings)


@orca.step('feasibility2')
def feasibility2(parcels, settings,
                parcel_sales_price_sqft_func,
                parcel_is_allowed_func):
    kwargs = settings['feasibility']

    config = sqftproforma.SqFtProFormaConfig()

    attr = ['parcel_sizes', 'fars', 'profit_factor', 'building_efficiency', 'parcel_coverage',
            'cap_rate', 'height_per_story', "sqft_per_rate", "uses", "residential_uses",
            "parking_cost_d", "parking_sqft_d", "costs", "parking_rates", 'parking_configs', 'heights_for_costs'
            , 'max_retail_height', 'max_industrial_height']

    # add max retail height and industrial height when nrh is included in the model
    # change yaml file when running nrh

    for x in attr:
        setattr(config, x, settings["sqftproforma_config"][x])

    types = {}
    attr2 = ['residential', 'retail', 'industrial', 'office', 'mixedresidential', 'mixedoffice']
    for x in attr2:
            types.update({x: settings["sqftproforma_config"]['forms'][x]})

    setattr(config, 'forms', types)

    utils.run_feasibility(parcels,
                          parcel_sales_price_sqft_func,
                          parcel_is_allowed_func,
                          config=config, forms_to_test=['residential'],
                          pass_through=['parcel_size','land_cost','weighted_rent','building_purchase_price','total_sqft'],
                          **kwargs)


def run_developer(forms, agents, buildings,supply_fname, parcel_size,
                  ave_unit_size, total_units, feasibility,
                  max_dua_zoning, max_res_units, year=None,
                  target_vacancy=.1, use_max_res_units=False,
                  form_to_btype_callback=None,
                  add_more_columns_callback=None, max_parcel_size=2000000,
                  residential=True, bldg_sqft_per_job=400.0,
                  min_unit_size=400, remove_developed_buildings=True,
                  unplace_agents=['households', 'jobs'],
                  num_units_to_build=None, profit_to_prob_func=None):
    """
    Run the developer model to pick and build buildings

    Parameters
    ----------
    forms : string or list of strings
        Passed directly dev.pick
    agents : DataFrame Wrapper
        Used to compute the current demand for units/floorspace in the area
    buildings : DataFrame Wrapper
        Used to compute the current supply of units/floorspace in the area
    supply_fname : string
        Identifies the column in buildings which indicates the supply of
        units/floorspace
    parcel_size : Series
        Passed directly to dev.pick
    ave_unit_size : Series
        Passed directly to dev.pick - average residential unit size
    total_units : Series
        Passed directly to dev.pick - total current residential_units /
        job_spaces
    feasibility : DataFrame Wrapper
        The output from feasibility above (the table called 'feasibility')
    year : int
        The year of the simulation - will be assigned to 'year_built' on the
        new buildings
    target_vacancy : float
        The target vacancy rate - used to determine how much to build
    form_to_btype_callback : function
        Will be used to convert the 'forms' in the pro forma to
        'building_type_id' in the larger model
    add_more_columns_callback : function
        Takes a dataframe and returns a dataframe - is used to make custom
        modifications to the new buildings that get added
    max_parcel_size : float
        Passed directly to dev.pick - max parcel size to consider
    min_unit_size : float
        Passed directly to dev.pick - min unit size that is valid
    residential : boolean
        Passed directly to dev.pick - switches between adding/computing
        residential_units and job_spaces
    bldg_sqft_per_job : float
        Passed directly to dev.pick - specified the multiplier between
        floor spaces and job spaces for this form (does not vary by parcel
        as ave_unit_size does)
    remove_redeveloped_buildings : optional, boolean (default True)
        Remove all buildings on the parcels which are being developed on
    unplace_agents : optional , list of strings (default ['households', 'jobs'])
        For all tables in the list, will look for field building_id and set
        it to -1 for buildings which are removed - only executed if
        remove_developed_buildings is true
    num_units_to_build: optional, int
        If num_units_to_build is passed, build this many units rather than
        computing it internally by using the length of agents adn the sum of
        the relevant supply columin - this trusts the caller to know how to compute
        this.
    profit_to_prob_func: func
        Passed directly to dev.pick

    Returns
    -------
    Writes the result back to the buildings table and returns the new
    buildings with available debugging information on each new building
    """

    dev = developer.Developer(feasibility.to_frame())

    target_units = num_units_to_build or dev.\
        compute_units_to_build(len(agents),
                               buildings[supply_fname].sum(),
                               target_vacancy)

    print "{:,} feasible buildings before running developer".format(
          len(dev.feasibility))

    #df = dev.feasibility['residential']

    df = dev.feasibility
    df['residential','max_profit_orig'] = df['residential','max_profit']
    df['residential', 'max_profit'].loc[df['residential','max_profit_orig'] < 0] = .001
    orca.add_table("feasibility", df)

    df = df['residential']
    df["parcel_size"] = parcel_size
    df["ave_unit_size"] = ave_unit_size
    df['current_units'] = total_units
    df['max_dua_zoning'] = max_dua_zoning
    df['max_res_units'] = max_res_units

    df = df[df.parcel_size < max_parcel_size]

    df['units_from_max_dua_zoning'] = np.NaN

    df.loc[df['max_dua_from_zoning'] >= 0, 'units_from_max_dua_zoning'] = (df.max_dua_from_zoning * df.acres).round()
    df['units_from_max_res_zoning'] = df['max_res_units']
    df['units_from_min_unit_size'] = (df['residential_sqft']/min_unit_size).round()

    df['units_from_zoning'] = np.NaN # final units from zoning

    df.loc[(df['units_from_max_res_zoning'] >= 0) &
                    (df['units_from_max_dua_zoning'].isnull()), 'units_from_zoning'] = df[
        'units_from_max_res_zoning']

    df.loc[(df['units_from_max_res_zoning'].isnull()) &
                    (df['units_from_max_dua_zoning'] >= 0), 'units_from_zoning'] = df[
        'units_from_max_dua_zoning']

    df.loc[(df['units_from_max_res_zoning'].isnull()) &
                    (df['units_from_max_dua_zoning'].isnull()), 'units_from_zoning'] = 0

    df.loc[(df['units_from_max_res_zoning'] >= 0) &
                    (df['units_from_max_dua_zoning'] >= 0), 'units_from_zoning'] = df[
        ['units_from_max_res_zoning', 'units_from_max_dua_zoning']].min(axis=1)
###################################################################################################
# for schedule 2 ONLY
#    df['units_from_zoning'] = df['units_from_max_res_zoning']
#    df.loc[(df['units_from_max_res_zoning'].isnull()), 'units_from_zoning'] = 0
# end for schedule 2  ONLY
#######################################################################################################

    df.loc[(df['siteid'] > 0), 'units_from_zoning'] = 0

    df['final_units_constrained_by_size'] = df[['units_from_zoning', 'units_from_min_unit_size']].min(
        axis=1)

    df['unit_size_from_final'] = df['residential_sqft'] / df['units_from_zoning']

    df.loc[(df['unit_size_from_final'] < min_unit_size), 'unit_size_from_final'] = min_unit_size

    df['final_units_constrained_by_size'] = (df['residential_sqft'] / df['unit_size_from_final']).round()
    df['net_units'] = df.final_units_constrained_by_size - df.current_units

    df['roi'] = df['max_profit'] / df['total_cost']

    df = df.reset_index(drop=False)
    df = df.set_index(['parcel_id'])

    unit_size_from_final = df.unit_size_from_final


    df2 = df.loc[:, ['parcel_id', 'zoning_id', 'zoning_schedule_id', 'parent_zoning_id',
                     'current_units','max_dua_zoning', 'acres', 'units_from_max_dua_zoning',
                     'max_res_units','allowed_development_types',
                     'allowed_units','net_units','unit_size_from_final']]
    # df['net_units_corrected'] = df['net_units']

    # df['agg'] = df['array_agg'].apply(lambda x: any(pd.Series(x).str.contains('2')))
    # df.loc[((df['agg']==False)& (df['net_units'] > 1)), 'net_units_corrected'] = 1
    feasibility_available = 'data/parcels_available_units_' + str(year) + '.csv'
    df.to_csv(feasibility_available)

    new_buildings = dev.pick(forms,
                             target_units,
                             parcel_size,
                             unit_size_from_final,
                             total_units,
                             max_parcel_size=max_parcel_size,
                             min_unit_size=min_unit_size,
                             drop_after_build=False,
                             residential=residential,
                             bldg_sqft_per_job=bldg_sqft_per_job,
                             profit_to_prob_func=profit_to_prob_func)

    orca.add_table("feasibility", dev.feasibility)

    if new_buildings is None:
        return

    if len(new_buildings) == 0:
        return new_buildings

    if year is not None:
        new_buildings["year_built"] = year

    if not isinstance(forms, list):
        # form gets set only if forms is a list
        new_buildings["form"] = forms

    if form_to_btype_callback is not None:
        new_buildings["building_type_id"] = new_buildings.\
            apply(form_to_btype_callback, axis=1)

    new_buildings["stories"] = new_buildings.stories.apply(np.ceil)

    ret_buildings = new_buildings
    if add_more_columns_callback is not None:
        new_buildings = add_more_columns_callback(new_buildings)

    print "Adding {:,} buildings with {:,} {}".\
        format(len(new_buildings),
               int(new_buildings[supply_fname].sum()),
               supply_fname)

    print "{:,} feasible buildings after running developer".format(
          len(dev.feasibility))

    old_buildings = buildings.to_frame(buildings.local_columns)
    new_buildings = new_buildings[buildings.local_columns]
    new_buildings['new_bldg'] = True

    if remove_developed_buildings:
        old_buildings = \
            utils._remove_developed_buildings(old_buildings, new_buildings, unplace_agents)

    all_buildings, new_index = dev.merge(old_buildings, new_buildings,
                                         return_index=True)
    ret_buildings.index = new_index

    orca.add_table("buildings", all_buildings)

    if "residential_units" in orca.list_tables() and residential:
        # need to add units to the units table as well
        old_units = orca.get_table("residential_units")
        old_units = old_units.to_frame(old_units.local_columns)
        new_units = pd.DataFrame({
            "unit_residential_price": 0,
            "num_units": 1,
            "deed_restricted": 0,
            "unit_num": np.concatenate([np.arange(i) for i in \
                                        new_buildings.residential_units.values]),
            "building_id": np.repeat(new_buildings.index.values,
                                     new_buildings.residential_units.\
                                     astype('int32').values)
        }).sort(columns=["building_id", "unit_num"]).reset_index(drop=True)

        print "Adding {:,} units to the residential_units table".\
            format(len(new_units))
        all_units = dev.merge(old_units, new_units)
        all_units.index.name = "unit_id"

        orca.add_table("residential_units", all_units)

        return ret_buildings
        # pondered returning ret_buildings, new_units but users can get_table
        # the units if they want them - better to avoid breaking the api

    return ret_buildings


@orca.step('residential_developer')
def residential_developer(feasibility, households, buildings, parcels, year,
                          settings, summary, form_to_btype_func,
                          add_extra_columns_func):
    kwargs = settings['residential_developer']
    new_buildings = run_developer(
        "residential",
        households,
        buildings,
        "residential_units",
        parcels.parcel_size,
        parcels.ave_sqft_per_unit,
        parcels.total_residential_units,
        feasibility,
        parcels.max_dua_zoning,
        parcels.max_res_units,
        year=year,
        form_to_btype_callback=form_to_btype_func,
        add_more_columns_callback=add_extra_columns_func,
        **kwargs)

    summary.add_parcel_output(new_buildings)


def get_git_hash(model='residential'):
    x = file('.git/refs/heads/' + model)
    git_hash = x.read()
    return git_hash


def to_database(scenario=' ', rng=range(0, 0), urbansim_connection=get_connection_string("configs/dbconfig.yml", 'urbansim_database'),
                default_schema='urbansim_output'):
    """ df_name:
            Required parameter, is the name of the table that will be read from the H5 file,
            Also first half of the table name to be stored in the database
        urbansim_connection:
            sql connection, default is for urbansim_database
        year:
            year of information to be caputured, should be pass the same range as simulation period
            minus first and last year.
        defalut_schema:
            The schema name under which to save the data, default is urbansim_output
    """
    conn = psycopg2.connect(database="urbansim", user="urbansim_user", password="urbansim", host="socioeca8",
                            port="5432")
    cursor = conn.cursor()
    t = (scenario,)
    cursor.execute('SELECT scenario_id FROM urbansim_output.parent_scenario WHERE scenario_name=%s', t)
    scenario_id = cursor.fetchone()
    cursor.execute('SELECT parent_scenario_id FROM urbansim_output.parent_scenario WHERE scenario_name=%s', t)
    parent_scenario_id = cursor.fetchone()
    conn.close()

    for year in rng:
        if year == 0 and scenario_id[0] == 1:
            for x in ['parcels', 'buildings']:

                print 'exporting ' + x + str(year) + ' ' + str(scenario_id[0])

                df = pd.read_hdf('data\\results.h5', 'base/' + x)
                df['parent_scenario_id'] = parent_scenario_id[0]
                df.to_sql(x + '_base', urbansim_connection, flavor='postgresql', schema=default_schema, if_exists='append')
        elif year != 0:
            for x in ['buildings','feasibility']:
                print 'exporting ' + x + str(year) + ' ' + str(scenario_id[0])

                df = pd.read_hdf('data\\results.h5', str(year) + '/' + x)
                if x == 'feasibility':
                    df = df['residential']
                df['year'] = year
                df['scenario_id'] = scenario_id[0]
                df['parent_scenario_id'] = parent_scenario_id[0]
                # df['zoning_schedule_id'] = settings['zoning_schedule_id']
                df.to_sql(x, urbansim_connection, flavor='postgresql', schema=default_schema, if_exists='append')

    conn = psycopg2.connect(database="urbansim", user="urbansim_user", password="urbansim", host="socioeca8",
                            port="5432")
    print "Opened database successfully"
    cursor = conn.cursor()
    cursor.execute('''DELETE FROM urbansim_output.buildings WHERE building_id + residential_units in (
                      SELECT building_id + residential_units FROM urbansim_output.buildings_base)''')
    conn.commit()
    print "Deleted any old building that existed in the base table"
    cursor.execute('''DELETE FROM urbansim_output.parcels WHERE parcel_id +  total_residential_units IN(
                      SELECT parcel_id +  total_residential_units FROM urbansim_output.parcels_base
                          )''')
    conn.commit()
    print "Deleted parcels where no buildings were made"
    conn.close()


def update_scenario(scenario=' '):

    conn = psycopg2.connect(database="urbansim", user="urbansim_user", password="urbansim", host="socioeca8",
                            port="5432")
    cursor = conn.cursor()

    cursor.execute('''CREATE TABLE IF NOT EXISTS  urbansim_output.parent_scenario
            (
                        parent_scenario_id	SERIAL PRIMARY KEY ,
                        scenario_name	VARCHAR(20)	NOT NULL,
                        scenario_id int
                    )'''
                   )

    conn.commit()

    cursor.execute('''CREATE TABLE IF NOT EXISTS  urbansim_output.scenario
            (
                         parent_scenario_id	int NOT NULL REFERENCES urbansim_output.parent_scenario(parent_scenario_id),
                         scenario_id int	NOT NULL,
                         user_name VARCHAR(20),
                         run_datetime VARCHAR(100),
                         git_hash VARCHAR(100),
                         PRIMARY KEY(parent_scenario_id, scenario_id),
                         CONSTRAINT pk_scenario_id UNIQUE (parent_scenario_id, scenario_id)
                    )'''
                   )

    conn.commit()

    cursor.execute('''CREATE TABLE IF NOT EXISTS urbansim_output.buildings_base
            (
                          building_id bigint PRIMARY KEY,
                          parcel_id bigint,
                          building_type_id bigint,
                          residential_units bigint,
                          residential_sqft bigint,
                          non_residential_sqft bigint,
                          non_residential_rent_per_sqft bigint,
                          year_built bigint,
                          stories bigint,
                          distance_to_park real,
                          distance_to_onramp_mi double precision,
                          distance_to_school real,
                          lot_size_per_unit double precision,
                          vacant_residential_units bigint,
                          building_sqft bigint,
                          structure_age bigint,
                          distance_to_freeway double precision,
                          vacant_job_spaces double precision,
                          is_office integer,
                          distance_to_coast_mi double precision,
                          year_built_1960to1970 boolean,
                          distance_to_onramp real,
                          year_built_1980to1990 boolean,
                          year_built_1970to1980 boolean,
                          residential_price_adj double precision,
                          distance_to_transit real,
                          year_built_1950to1960 boolean,
                          parcel_size double precision,
                          general_type text,
                          distance_to_coast double precision,
                          node_id bigint,
                          year_built_1940to1950 boolean,
                          zone_id text,
                          luz_id bigint,
                          is_retail integer,
                          sqft_per_job double precision,
                          job_spaces integer,
                          sqft_per_unit integer,
                          distance_to_transit_mi double precision,
                          parent_scenario_id bigint,
                          FOREIGN KEY (parent_scenario_id)
                          REFERENCES urbansim_output.parent_scenario(parent_scenario_id)
                        )'''
                   )

    conn.commit()

    cursor.execute('''CREATE TABLE IF NOT EXISTS urbansim_output.households_base
          (
                      household_id bigint PRIMARY KEY,
                      building_id bigint,
                      persons bigint,
                      age_of_head bigint,
                      income bigint,
                      children bigint,
                      node_id bigint,
                      income_quartile bigint,
                      zone_id text,
                      parent_scenario_id bigint,
                      FOREIGN KEY (parent_scenario_id)
                      REFERENCES urbansim_output.parent_scenario(parent_scenario_id)
                    )'''
                   )

    conn.commit()
    cursor.execute('''CREATE TABLE IF NOT EXISTS urbansim_output.parcels_base
          (
                      parcel_id bigint PRIMARY KEY,
                      zoning_schedule_id integer,
                      development_type_id bigint,
                      luz_id bigint,
                      acres double precision,
                      zoning_id text,
                      siteid integer,
                      x double precision,
                      y double precision,
                      distance_to_coast double precision,
                      distance_to_freeway double precision,
                      node_id bigint,
                      distance_to_park real,
                      total_job_spaces double precision,
                      total_sqft double precision,
                      distance_to_school real,
                      lot_size_per_unit double precision,
                      building_purchase_price_sqft double precision,
                      max_far integer,
                      building_purchase_price double precision,
                      avg_residential_price double precision,
                      zoned_du integer,
                      ave_unit_size double precision,
                      distance_to_onramp real,
                      max_dua_zoning integer,
                      newest_building double precision,
                      distance_to_transit real,
                      max_height double precision,
                      parcel_size double precision,
                      parcel_acres double precision,
                      ave_sqft_per_unit double precision,
                      zone_id text,
                      total_residential_units double precision,
                      land_cost double precision,
                      max_res_units double precision,
                      zoned_du_underbuild double precision,
                      oldest_building double precision,
                      parent_scenario_id bigint,
                      FOREIGN KEY (parent_scenario_id)
                      REFERENCES urbansim_output.parent_scenario(parent_scenario_id)
                    )'''
                   )

    conn.commit()

    cursor.execute('''CREATE TABLE IF NOT EXISTS  urbansim_output.buildings
                (
                            building_id bigint,
                            parcel_id bigint,
                            building_type_id bigint,
                            residential_units bigint,
                            residential_sqft bigint,
                            non_residential_sqft bigint,
                            non_residential_rent_per_sqft bigint,
                            year_built bigint,
                            stories bigint,
                            residential_price double precision,
                            distance_to_park real,
                            distance_to_onramp_mi double precision,
                            distance_to_school real,
                            lot_size_per_unit double precision,
                            vacant_residential_units bigint,
                            building_sqft bigint,
                            structure_age bigint,
                            distance_to_freeway double precision,
                            vacant_job_spaces double precision,
                            is_office integer,
                            distance_to_coast_mi double precision,
                            year_built_1960to1970 boolean,
                            distance_to_onramp real,
                            year_built_1980to1990 boolean,
                            year_built_1970to1980 boolean,
                            residential_price_adj double precision,
                            distance_to_transit real,
                            year_built_1950to1960 boolean,
                            parcel_size double precision,
                            general_type text,
                            distance_to_coast double precision,
                            node_id bigint,
                            year_built_1940to1950 boolean,
                            zone_id text,
                            luz_id bigint,
                            is_retail integer,
                            sqft_per_job double precision,
                            job_spaces integer,
                            sqft_per_unit integer,
                            distance_to_transit_mi double precision,
                            year bigint,
                            scenario_id bigint,
                            parent_scenario_id bigint,
                            FOREIGN KEY (parent_scenario_id, scenario_id)
                            REFERENCES urbansim_output.scenario(parent_scenario_id, scenario_id)
                        )'''
                   )

    conn.commit()

    cursor.execute('''CREATE TABLE IF NOT EXISTS urbansim_output.households
          (
                      household_id bigint,
                      building_id bigint,
                      persons bigint,
                      age_of_head bigint,
                      income bigint,
                      children bigint,
                      income_quartile bigint,
                      node_id bigint,
                      zone_id text,
                      year bigint,
                      scenario_id bigint,
                      parent_scenario_id bigint,
                      FOREIGN KEY (parent_scenario_id, scenario_id)
                      REFERENCES urbansim_output.scenario(parent_scenario_id, scenario_id)
                    )'''
                   )

    conn.commit()
    cursor.execute('''CREATE TABLE IF NOT EXISTS urbansim_output.parcels
          (
                      parcel_id bigint,
                      zoning_schedule_id integer,
                      development_type_id bigint,
                      luz_id bigint,
                      acres double precision,
                      zoning_id text,
                      siteid integer,
                      x double precision,
                      y double precision,
                      distance_to_coast double precision,
                      distance_to_freeway double precision,
                      node_id bigint,
                      distance_to_park real,
                      total_job_spaces double precision,
                      total_sqft double precision,
                      distance_to_school real,
                      lot_size_per_unit double precision,
                      building_purchase_price_sqft double precision,
                      max_far integer,
                      building_purchase_price double precision,
                      avg_residential_price double precision,
                      zoned_du integer,
                      ave_unit_size real,
                      distance_to_onramp real,
                      max_dua_zoning integer,
                      newest_building double precision,
                      distance_to_transit real,
                      max_height double precision,
                      parcel_size double precision,
                      parcel_acres double precision,
                      ave_sqft_per_unit real,
                      zone_id text,
                      total_residential_units double precision,
                      land_cost double precision,
                      max_res_units double precision,
                      zoned_du_underbuild double precision,
                      oldest_building double precision,
                      year bigint,
                      scenario_id bigint,
                      parent_scenario_id bigint,
                      FOREIGN KEY (parent_scenario_id, scenario_id)
                      REFERENCES urbansim_output.scenario(parent_scenario_id, scenario_id)
                    )'''
                   )

    conn.commit()

    t = (scenario,)
    cursor.execute('SELECT scenario_id FROM urbansim_output.parent_scenario WHERE scenario_name=%s', t)
    scenario_id = cursor.fetchone()
    cursor.execute('SELECT parent_scenario_id FROM urbansim_output.parent_scenario WHERE scenario_name=%s', t)
    parent_scenario_id = cursor.fetchone()

    if scenario_id:
        print 'Scenario_id updated'
        query = 'UPDATE urbansim_output.parent_scenario SET scenario_id = %s where scenario_name= %s;'
        data = (scenario_id[0] + 1, t[0])
        cursor.execute(query, data)
        conn.commit()

        query = "INSERT INTO urbansim_output.scenario (parent_scenario_id, scenario_id, user_name, run_datetime" \
                ", git_hash)" \
                "VALUES (%s, %s, %s, %s, %s);"

        data = (parent_scenario_id[0], scenario_id[0] + 1, getpass.getuser(), str(datetime.datetime.now()), get_git_hash())
        cursor.execute(query, data)
        conn.commit()

    else:
        print 'A new parent scenario id added'
        query = "INSERT INTO urbansim_output.parent_scenario (scenario_name, scenario_id) VALUES (%s, %s);"
        data = (t[0], 1)
        cursor.execute(query, data)
        conn.commit()

        cursor.execute('SELECT parent_scenario_id FROM urbansim_output.parent_scenario WHERE scenario_name=%s', t)
        parent_scenario_id2 = cursor.fetchone()

        query = "INSERT INTO urbansim_output.scenario (parent_scenario_id, scenario_id, user_name, run_datetime" \
                ", git_hash)" \
                "VALUES (%s, %s, %s, %s, %s);"

        data = (parent_scenario_id2[0], 1, getpass.getuser(), str(datetime.datetime.now()), get_git_hash())
        cursor.execute(query, data)
        conn.commit()
    conn.close()




