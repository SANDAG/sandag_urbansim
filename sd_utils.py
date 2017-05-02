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


def get_git_hash(model='nonresidential'):
    x = file('.git/refs/heads/' + model)
    git_hash = x.read()
    return git_hash


def get_jurisdiction_name(code=1):
    engine = sqlalchemy.create_engine('mssql://sql2014a8/data_cafe?trusted_connection=yes')
    sql_query = 'SELECT name FROM ref.geography_zone WHERE geography_type_id = 136 and zone =' + str(code)
    df = pd.read_sql(sql_query, engine)
    return df.name[0]


def get_max_job_id():
    conn = psycopg2.connect(database="urbansim", user="urbansim_user", password="urbansim", host="socioeca8",
                            port="5432")
    cursor = conn.cursor()
    cursor.execute('SELECT max(job_id) as job_id FROM urbansim.jobs')
    job_id = cursor.fetchone()
    return job_id[0]


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
            for x in ['buildings', 'feasibility', 'jobs']:
                print 'exporting ' + x + str(year) + ' ' + str(scenario_id[0])

                df = pd.read_hdf('data\\results.h5', str(year) + '/' + x)
                if x == 'feasibility':
                    list1 = ['mixedoffice', 'industrial', 'office', 'retail', 'mixedresidential']
                    df1 = df['residential']
                    z = 'residential'
                    df1['use_type'] = z
                    for y in list1:
                        df2 = df[str(y)]
                        df2 = df2.dropna(subset=['max_profit'])
                        df2['use_type'] = str(y)
                        df1 = df1.append(df2)
                    df = df1
                    df.rename(columns={'total_sqft': 'total_sqft_existing_bldgs'}, inplace=True)
                    df = df[(df.addl_units > 0) | (df.non_residential_sqft > 0)]
                    df['existing_units'] = np.where(df['new_built_units'] == 0, df['total_residential_units'], \
                                                    df['total_residential_units'] - df['addl_units'])

                if x == 'buildings':
                    df = df[df.new_bldg == 1]
                    df.sch_dev = df.sch_dev.astype(int)
                    df.new_bldg = df.new_bldg.astype(int)

                elif x == 'jobs':
                    df.index.names = ['job_id']
                    df = df[df.index > get_max_job_id()]
                df['year'] = year
                df['scenario_id'] = scenario_id[0]
                df['parent_scenario_id'] = parent_scenario_id[0]

                df.to_sql(x, urbansim_connection, schema=default_schema, if_exists='append')


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


