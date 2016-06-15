import pandas as pd
from pysandag.database import get_connection_string
from pysandag.gis import  transform_wkt
from sqlalchemy import create_engine
import yaml

##OPEN yaml DATASET DICTIONARY
with open('E:\\apps\\sandag_urbansim\\etl\\postgresql\\urbansim_datasets_test.yml') as y:
    datasets = yaml.load(y)

##SELECT DATASETS TO LOAD FROM yaml
selected = [
    #'building_sqft_per_job',
    'buildings',
    'development_type',
    #'edges',
    #'households',
    #'jobs',
    #'nodes',
    'parcels',
    'zoning_allowed_use',
    'zoning'
]

##PROCESS SELECTED DATASETS
for key in selected:
    dataset = datasets[key]

    #GET THE CONNECTION STRINGS
    in_connection_string = get_connection_string("dbconfig.yml", 'in_db')
    out_connection_string = get_connection_string("dbconfig.yml", 'out_db')

    ##INPUT QUERY
    in_query_non_spatial = dataset['in_query_non_spatial']

    ##MSSQL SQLAlchemy
    sql_in_engine = create_engine(in_connection_string)
    ##Pandas Data Frame for non-spatial data
    df_non_spatial = pd.read_sql(in_query_non_spatial, sql_in_engine, index_col= dataset['index_col'])
    print 'Loaded Non-Spatial Query'

    ##CHECK FOR SPATIAL DATA PROCESSING >>
    if dataset.has_key('in_query_spatial'):
        ##PANDAS DATAFRAME FOR SPATIAL DATA
        in_query_spatial = dataset['in_query_spatial']
        df_spatial = pd.read_sql(in_query_spatial, sql_in_engine, index_col=dataset['index_col'])
        print 'Loaded Spatial Query'

        #Transform Shape from SPCS to WGS --> See method above for details
        s = df_spatial['shape'].apply(lambda x: transform_wkt(x))
        df_spatial['shape'] = s
        print 'Transformed Shapes'

        #Join spatial and non-spatial frames
        df = pd.concat([df_non_spatial, df_spatial], axis = 1)
    else:
        df = df_non_spatial
        print 'Non-spatial Dataset'
    ##SPATIAL DATA PROCESSING <<

    ##Output
    out_table = dataset['out_table']

    #Map columns --> Notice special geometry column
    column_data_types = dataset['column_data_types']

    print 'Start Data Load'
    ##PostgreSQL SQLAlchemy
    sql_out_engine = create_engine(out_connection_string)

    #Write PostgreSQL
    df.to_sql(out_table, sql_out_engine, schema='urbansim_test', if_exists='replace', index=True, dtype = column_data_types)

    print "Table Loaded to {0}".format(out_table)
    print '*' * 30