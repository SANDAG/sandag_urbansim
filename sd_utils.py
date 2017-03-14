import pandas as pd
from pysandag.database import get_connection_string
import sqlalchemy
import psycopg2
import getpass
import yaml
import datetime
import numpy as np
from urbansim_defaults import datasources

with open('configs/settings.yaml', 'r') as f:
    settings = yaml.load(f)


def get_git_hash(model='residential'):
    x = file('.git/refs/heads/' + model)
    git_hash = x.read()
    return git_hash


def get_jurisdiction_name(code=1):
    engine = sqlalchemy.create_engine('mssql://sql2014a8/data_cafe?trusted_connection=yes')
    sql_query = 'SELECT name FROM ref.geography_zone WHERE geography_type_id = 136 and zone =' + str(code)
    df = pd.read_sql(sql_query, engine)
    return df.name[0]


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
                df.to_sql(x + '_base', urbansim_connection, schema=default_schema, if_exists='append')
        elif year == rng[len(rng)-1]:
            for x in ['buildings','feasibility']:
                print 'exporting ' + x + str(year) + ' ' + str(scenario_id[0])

                df = pd.read_hdf('data\\results.h5', str(year) + '/' + x)
                if x == 'feasibility':
                    df = df['residential']
                    df.rename(columns={'total_sqft': 'total_sqft_existing_bldgs'}, inplace=True)
                df['year'] = year
                df['scenario_id'] = scenario_id[0]
                df['parent_scenario_id'] = parent_scenario_id[0]
                if x == 'buildings':
                    df = df[df.new_bldg == 1]
                    df.sch_dev = df.sch_dev.astype(int)
                    df.new_bldg = df.new_bldg.astype(int)
                elif x == 'feasibility':
                        df = df[df.addl_units > 0]
                        df['existing_units'] = np.where(df['new_built_units'] == 0, df['total_residential_units'], \
                                                        df['total_residential_units'] - df['addl_units'])
                df.to_sql(x, urbansim_connection, schema=default_schema, if_exists='append')

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


