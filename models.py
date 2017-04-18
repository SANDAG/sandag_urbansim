import pandana as pdna
import pandas as pd

# from urbansim.developer import sqftproforma
import sqftproforma
import orca
from urbansim_defaults import models
from urbansim_defaults import utils
import numpy as np
import developer
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
def scheduled_development_events(scheduled_development_events, buildings, parcels, phase_in):
    year = get_year()
    sched_dev = scheduled_development_events.to_frame()
    sched_dev = sched_dev.groupby('siteID').apply(lambda x: x.iloc[np.random.randint(0, len(x))])
    phasein = phase_in.to_frame()
    sched_dev = sched_dev.merge(phasein,left_on = 'siteID',right_on = 'siteID')
    sched_dev = sched_dev.loc[sched_dev['Units'] > 0]
    sched_dev = sched_dev[sched_dev.Year==year]
    #TODO: The simple division here is not consistent with other job_spaces calculations
    sched_dev['job_spaces'] = sched_dev.non_residential_sqft/400
    sched_dev['job_spaces_original'] = sched_dev['job_spaces']
    if len(sched_dev) > 0:
        max_bid = buildings.index.values.max()
        idx = np.arange(max_bid + 1,max_bid+len(sched_dev)+1)
        sched_dev['building_id'] = idx
        sched_dev['building_type_id'] = np.where(sched_dev['Htype'] == 'MF', 21, 19)
        sched_dev = sched_dev.set_index('building_id')
        sched_dev['new_bldg'] = True
        sched_dev['year_built'] = year
        sched_dev['new_units'] = sched_dev['Units']
        sched_dev['sch_dev'] = True
        sched_dev['residential_units'] = sched_dev['Units']
        b = buildings.to_frame(buildings.local_columns)
        if 'residential_price' in b.columns:
            sched_dev['residential_price'] = 0
        if 'non_residential_price' in b.columns:
            sched_dev['non_residential_price'] = 0
        all_buildings = developer.Developer.merge(b,sched_dev[b.columns])
        all_buildings = all_buildings.fillna(0)
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
    run_feasibility(parcels,
                          parcel_sales_price_sqft_func,
                          parcel_is_allowed_func, parcel_filter = 'scheduled_development==False',
                          config=config,
                          pass_through=['parcel_size','land_cost','weighted_rent','building_purchase_price','building_purchase_price_sqft','total_sqft','parcel_avg_price_residential', 'addl_units', "new_built_units", 'max_res_units', 'development_type_id', 'job_spaces', 'luz_id', 'sqft_per_job'],
                          **kwargs)


def target_units_def():
    df = orca.get_table('buildings').to_frame(columns="year_built")
    year = get_year()
    df = df[df.year_built != 2016]
    if year == 2016:
        x = 0
    elif year <= 2020:
        x = df[df.year_built > (year - 5)].count() / 4
        x = int(x[0])
    elif year > 2020:
        x = df[df.year_built > (year - 5)].count() / 5
        x = int(x[0])
    return x


def target_avg_unit_size():
    df = orca.get_table('buildings').to_frame(columns="year_built" "sqft_per_unit")
    year = get_year()
    df = df[df.year_built != 2016]
    df = df[df.year_built > (year - 5)]
    df[(df.sqft_per_unit > 4000)]["sqft_per_unit"] = 4000
    x = df['sqft_per_unit'].mean()
    print int(x)
    return 2000


def run_developer(forms, agents, buildings, supply_fname, parcel_size,
                  ave_unit_size, total_units, feasibility,
                  max_dua_zoning, max_res_units, addl_units,year=None,
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
    # num_units_to_build = target_units_def()
    # ave_unit_size = target_avg_unit_size()
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

    parcels = orca.get_table('parcels').to_frame()

    df = df['residential']
    settings = orca.get_injectable('settings')
    df["parcel_size"] = parcel_size
    df["ave_unit_size"] = ave_unit_size
    df['current_units'] = total_units
    df['max_dua_zoning'] = max_dua_zoning
    df['max_res_units'] = max_res_units
    df['addl_units'] = addl_units
    df['zoning_id'] = parcels.zoning_id
    df['siteid'] = parcels.siteid
    df['zoning_schedule_id'] = parcels.zoning_schedule_id
    df['acres'] = parcels.parcel_acres
    df['land_cost_per_sqft'] = settings['default_land_cost']
    df['cap_rate'] = settings['sqftproforma_config']['cap_rate']
    df['building_efficiency'] = settings['sqftproforma_config']['building_efficiency']
    df['min_size_per_unit'] = min_unit_size
    df['max_dua_from_zoning'] =  df['max_dua_zoning']
    df['development_type_id'] = parcels.development_type_id
    df = df[df.parcel_size < max_parcel_size]
    '''
    df['units_from_max_dua_zoning'] = np.NaN

    df.loc[df['max_dua_from_zoning'] >= 0, 'units_from_max_dua_zoning'] = (df.max_dua_from_zoning * df.acres).round()
    df['units_from_max_res_zoning'] = df['max_res_units']


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
        '''

###################################################################################################
# for schedule 2 ONLY
    df['units_from_min_unit_size'] = (df['residential_sqft'] / min_unit_size).round()
    # df.loc[(df['units_from_max_res_zoning'].isnull()), 'units_from_zoning'] = 0
# end for schedule 2  ONLY
#######################################################################################################

    df.loc[(df['siteid'] > 0), 'units_from_zoning'] = 0

    df['final_units_constrained_by_size'] = df[['addl_units', 'units_from_min_unit_size']].min(
        axis=1)

    df['unit_size_from_final'] = df['residential_sqft'] / df['units_from_zoning']

    df.loc[(df['unit_size_from_final'] < min_unit_size), 'unit_size_from_final'] = min_unit_size

    df['final_units_constrained_by_size'] = (df['residential_sqft'] / df['unit_size_from_final']).round()
    df['net_units'] = df.final_units_constrained_by_size

    df['roi'] = df['max_profit'] / df['total_cost']

    df = df.reset_index(drop=False)
    df = df.set_index(['parcel_id'])

    unit_size_from_final = df.unit_size_from_final

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
               int(new_buildings['net_units'].sum()),
               supply_fname)

    print "{:,} feasible buildings after running developer".format(
          len(dev.feasibility))

    old_buildings = buildings.to_frame(buildings.local_columns)
    new_buildings = new_buildings[buildings.local_columns]
    new_buildings['new_bldg'] = True
    new_buildings['sch_dev'] = False
    new_buildings['new_units'] = new_buildings['residential_units']
    if not residential:
        new_buildings['job_spaces_original'] = new_buildings['job_spaces']
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
        ["residential", "mixedresidential", "mixedoffice"],
        households,
        buildings,
        "residential_units",
        parcels.parcel_size,
        parcels.ave_sqft_per_unit,
        parcels.total_residential_units,
        feasibility,
        parcels.max_dua_zoning,
        parcels.max_res_units,
        parcels.addl_units,
        year=year,
        form_to_btype_callback=form_to_btype_func,
        add_more_columns_callback=add_extra_columns_func,
        **kwargs)

    summary.add_parcel_output(new_buildings)


@orca.step('non_residential_developer')
def non_residential_developer(feasibility, jobs, buildings, parcels, year,
                              settings, summary, form_to_btype_func,
                              add_extra_columns_func):

    kwargs = settings['non_residential_developer']
    new_buildings = run_developer(
        ["office", "retail", "industrial", "mixedresidential", "mixedoffice"],
        jobs,
        buildings,
        "job_spaces",
        parcels.parcel_size,
        parcels.ave_sqft_per_unit,
        parcels.job_spaces,
        feasibility,
        parcels.max_dua_zoning,
        parcels.max_res_units,
        parcels.addl_units,
        year=year,
        form_to_btype_callback=form_to_btype_func,
        add_more_columns_callback=add_extra_columns_func,
        residential=False,
        **kwargs)

    summary.add_parcel_output(new_buildings)


def run_feasibility(parcels, parcel_price_callback,
                    parcel_use_allowed_callback, residential_to_yearly=True,
                    parcel_filter=None, only_built=True, forms_to_test=None,
                    config=None, pass_through=[], simple_zoning=False):
    """
    Execute development feasibility on all parcels

    Parameters
    ----------
    parcels : DataFrame Wrapper
        The data frame wrapper for the parcel data
    parcel_price_callback : function
        A callback which takes each use of the pro forma and returns a series
        with index as parcel_id and value as yearly_rent
    parcel_use_allowed_callback : function
        A callback which takes each form of the pro forma and returns a series
        with index as parcel_id and value and boolean whether the form
        is allowed on the parcel
    residential_to_yearly : boolean (default true)
        Whether to use the cap rate to convert the residential price from total
        sales price per sqft to rent per sqft
    parcel_filter : string
        A filter to apply to the parcels data frame to remove parcels from
        consideration - is typically used to remove parcels with buildings
        older than a certain date for historical preservation, but is
        generally useful
    only_built : boolean
        Only return those buildings that are profitable - only those buildings
        that "will be built"
    forms_to_test : list of strings (optional)
        Pass the list of the names of forms to test for feasibility - if set to
        None will use all the forms available in ProFormaConfig
    config : SqFtProFormaConfig configuration object.  Optional.  Defaults to
        None
    pass_through : list of strings
        Will be passed to the feasibility lookup function - is used to pass
        variables from the parcel dataframe to the output dataframe, usually
        for debugging
    simple_zoning: boolean, optional
        This can be set to use only max_dua for residential and max_far for
        non-residential.  This can be handy if you want to deal with zoning
        outside of the developer model.

    Returns
    -------
    Adds a table called feasibility to the sim object (returns nothing)
    """

    pf = sqftproforma.SqFtProForma(config) if config \
        else sqftproforma.SqFtProForma()

    df = parcels.to_frame()

    if parcel_filter:
        df = df.query(parcel_filter)

    # add prices for each use
    for use in pf.config.uses:
        # assume we can get the 80th percentile price for new development
        df[use] = parcel_price_callback(use)

    # convert from cost to yearly rent
    if residential_to_yearly:
        df["residential"] *= pf.config.cap_rate

    print "Describe of the yearly rent by use"
    print df[pf.config.uses].describe()

    d = {}
    forms = forms_to_test or pf.config.forms
    for form in forms:
        print "Computing feasibility for form %s" % form
        allowed = parcel_use_allowed_callback(form).loc[df.index]

        newdf = df[allowed]
        if simple_zoning:
            if form == "residential":
                # these are new computed in the effective max_dua method
                newdf["max_far"] = pd.Series()
                newdf["max_height"] = pd.Series()
            else:
                # these are new computed in the effective max_far method
                newdf["max_dua"] = pd.Series()
                newdf["max_height"] = pd.Series()

        d[form] = pf.lookup(form, newdf, only_built=only_built,
                            pass_through=pass_through)
        if residential_to_yearly and "residential" in pass_through:
            d[form]["residential"] /= pf.config.cap_rate

    far_predictions = pd.concat(d.values(), keys=d.keys(), axis=1)

    orca.add_table("feasibility", far_predictions)