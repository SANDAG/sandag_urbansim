import pandana as pdna
import pandas as pd
from urbansim.developer import sqftproforma
import orca
from urbansim_defaults import models
from urbansim_defaults import utils
import numpy as np
from pysandag.database import get_connection_string


###  ESTIMATIONS  ##################################
@orca.step('rsh_estimate')
def rsh_estimate(assessor_transactions, aggregations):
    return utils.hedonic_estimate("rsh.yaml", assessor_transactions, aggregations)


def get_year():
    year = orca.get_injectable('year')
    if year is None:
        year = 2015
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


@orca.step('scheduled_development_events')
def scheduled_development_events(scheduled_development_events, buildings):
    year = get_year()
    sched_dev = scheduled_development_events.to_frame()
    sched_dev = sched_dev[sched_dev.year_built==year]
    sched_dev['residential_sqft'] = sched_dev.sqft_per_unit*sched_dev.residential_units
    #TODO: The simple division here is not consistent with other job_spaces calculations
    sched_dev['job_spaces'] = sched_dev.non_residential_sqft/400
    if len(sched_dev) > 0:
        max_bid = buildings.index.values.max()
        idx = np.arange(max_bid + 1,max_bid+len(sched_dev)+1)
        sched_dev['building_id'] = idx
        sched_dev = sched_dev.set_index('building_id')
        from urbansim.developer.developer import Developer
        merge = Developer(pd.DataFrame({})).merge
        b = buildings.to_frame(buildings.local_columns)
        all_buildings = merge(b,sched_dev[b.columns])
        orca.add_table("buildings", all_buildings)


@orca.step('feasibility2')
def feasibility2(parcels, settings,
                parcel_sales_price_sqft_func,
                parcel_is_allowed_func):
    kwargs = settings['feasibility']

    from urbansim.developer import sqftproforma

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
                          **kwargs)


def get_git_hash(model='residential'):
    x = file('.git/refs/heads/' + model)
    git_hash = x.read()
    return git_hash


def to_database(scenario=' ', urbansim_connection=get_connection_string("configs/dbconfig.yml", 'urbansim_database'),
                rng=range(0, 0), default_schema='urbansim_output'):
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

    for year in rng:
        if year == 0 and scenario_id[0] == 1:
            for x in ['parcels', 'buildings', 'households']:
                print 'exporting ' + x + str(year) + ' ' + str(scenario_id[0])

                df = pd.read_hdf('data\\results.h5', 'base/' + x)
                df['parent_scenario_id'] = parent_scenario_id[0]
                df.to_sql(x + '_base', urbansim_connection, flavor='postgresql', schema=default_schema, if_exists='append')
        elif year != 0:
            for x in ['parcels', 'buildings', 'households']:
                print 'exporting ' + x + str(year) + ' ' + str(scenario_id[0])

                df = pd.read_hdf('data\\results.h5', str(year) + '/' + x)
                df['year'] = year
                df['scenario_id'] = scenario_id[0]
                df['parent_scenario_id'] = parent_scenario_id[0]
                # df['zoning_schedule_id'] = settings['zoning_schedule_id']
                df.to_sql(x, urbansim_connection, flavor='postgresql', schema=default_schema, if_exists='append')

    cursor.execute('''DELETE FROM urbansim_output.buildings WHERE building_id + residential_units in (
                      SELECT building_id + residential_units FROM urbansim_output.buildings_base
                          )''')
    conn.commit()
    print "Deleted any old building that existed in the base table"
    cursor.execute('''DELETE FROM urbansim_output.parcels WHERE parcel_id +  total_residential_units IN(
                      SELECT parcel_id +  total_residential_units FROM urbansim_output.parcels_base
                          )''')
    conn.commit()
    print "Deleted parcels where no buildings were made"


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
                      development_type_id bigint,
                      luz_id bigint,
                      acres double precision,
                      zoning_id text,
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
                      development_type_id bigint,
                      luz_id bigint,
                      acres double precision,
                      zoning_id text,
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

        data = (parent_scenario_id[0], scenario_id[0] + 1, getpass.getuser(), str(datetime.now()), get_git_hash())
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

        data = (parent_scenario_id2[0], 1, getpass.getuser(), str(datetime.now()), get_git_hash())
        cursor.execute(query, data)
        conn.commit()


def get_parent_values(id1=1, id_name='id', parent_name='parent', column='year', df_data=pd.DataFrame()
                      , df_id=pd.DataFrame()):

    """
    Definition to get parent values if not exists in the current period
    id1: The zone ID.
    id_name: Column name of the ID, by default 'id'
    parent_name: Column name of the parent ID, by defalut 'parent'
    column: The column which has to be checked if exists in current period.
    df_data: A pandas dataframe consisting of data
    df_id: A pandas dataframe consisting of ids and parents id.

    return: the definition returns a pandas df with index of the child id as
            and senior most parent indexed at length(df). This allows to pull zone id specification from df.
    """

    parent = df_id[parent_name][df_id[id_name] == id1].values

    if not parent:
        df_data = df_data[df_data[id_name] == id1]
        return df_data
    else:
        df_parent = get_parent_values(id1=parent[0], df_data=df_data, df_id=df_id)

        df_data = df_data[df_data[id_name] == id1]
        list1 = df_data[column]

        df_parent = df_parent[~df_parent[column].isin(list1)]
        df_data = df_data.append(df_parent)
        # df_data['final_id'] = id1
        return df_data.reset_index(drop=True)


def get_id(id_name='id', df_data=pd.DataFrame()):
    """
    return the currently specified zone id, as the dataframe is indexed according to parent_ids
    """
    return df_data[id_name].values[0]


def get_zoning_values(id1=1, id_name='zoning_schedule_id', parent_name='parent_zoning_schedule_id',
                      df_data=pd.DataFrame()
                      , df_id=pd.DataFrame()):
    """
    Definition to get parent values if not exists in the current period
    id1: The zone ID.
    id_name: Column name of the ID, by default 'id'
    parent_name: Column name of the parent ID, by defalut 'parent'
    column: The column which has to be checked if exists in current period.
    df_data: A pandas dataframe consisting of data
    df_id: A pandas dataframe consisting of ids and parents id.

    return: the definition returns a pandas df with index of the child id as
            and senior most parent indexed at length(df). This allows to pull zone id specification from df.
    """

    parent = df_id[parent_name][df_id[id_name] == id1].values

    if math.isnan(parent):
        df_data = df_data[df_data[id_name] == id1]
        return df_data
    else:
        df_parent = get_zoning_values(id1=parent[0], df_data=df_data, df_id=df_id)
        df_data = df_data[df_data[id_name] == id1]
        df_data = df_data.append(df_parent)
        return df_data

