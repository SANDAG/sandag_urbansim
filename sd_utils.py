import pandas as pd
from pysandag.database import get_connection_string
import sqlalchemy
import psycopg2
import getpass
import yaml
import datetime
import numpy as np
from urbansim_defaults import datasources
from sqlalchemy import create_engine


def get_git_hash(model='residential'):
    x = file('.git/refs/heads/' + model)
    git_hash = x.read()
    return git_hash


def to_database(scenario=' ', rng=range(0, 0), default_schema='urbansim_output'):
    """ scenario:
            string scenario description set in simulation.py
        rng:
            range of simulation, writing last year in range to db
        default_schema:
            The schema name under which to save the data, default is urbansim_output
    """

    # connect to database
    db = get_connection_string("configs/dbconfig.yml",'urbansim_database')
    urbansim_engine = create_engine(db)

    # get scenario run num
    scenario_sql = "SELECT scenario_id, parent_scenario_id FROM urbansim_output.parent_scenario WHERE scenario_name='%s'" %scenario
    scenario_num= pd.read_sql(scenario_sql, urbansim_engine)

    for x in ['buildings','feasibility']:
        print 'exporting to db: ' + x + ' for year ' + str(rng[-1]) + ' (scenario ' + str(scenario_num.iloc[0]['scenario_id']) + ')'
        # read data from h5 output file (data for each year is saved)
        df = pd.read_hdf('data\\results.h5', str(rng[-1]) + '/' + x)
        if x == 'feasibility':
            df = df['residential'] # only residential feasibility
            df.rename(columns={'total_sqft': 'total_sqft_existing_bldgs'}, inplace=True)
            df = df[df.addl_units > 0] # only buildings with capacity
            df['existing_units'] = np.where(df['new_built_units'] == 0, df['total_residential_units'], \
                                            df['total_residential_units'] - df['addl_units'])
        if x == 'buildings':
            df.sch_dev = df.sch_dev.astype(int) # change boolean t/f to 1/0
            df.new_bldg = df.new_bldg.astype(int) # change boolean t/f to 1/0
            df = df[(df.new_bldg == 1)]  # new buildings only to database

        df['year'] = rng[-1]
        df['scenario_id'] = scenario_num.iloc[0]['scenario_id']
        df['parent_scenario_id'] = scenario_num.iloc[0]['parent_scenario_id']

        df.to_sql(x, urbansim_engine, schema=default_schema, if_exists='append')


def update_scenario(scenario=' '):

    conn = psycopg2.connect(database="urbansim", user="urbansim_user", password="urbansim", host="socioeca8",
                            port="5432")
    cursor = conn.cursor()

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


