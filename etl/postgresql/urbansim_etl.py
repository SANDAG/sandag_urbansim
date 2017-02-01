import pandas as pd
from pysandag.database import get_connection_string
from pysandag.gis import transform_wkt
from sqlalchemy import create_engine, MetaData, Table, Index, Column
import yaml

##OPEN yaml DATASET DICTIONARY
with open('urbansim_datasets.yml') as y:
    datasets = yaml.load(y)

##SELECT DATASETS TO LOAD FROM yaml
selected = [
    #'assessor_transactions',
    #'building_sqft_per_job',
    #'buildings',
    #'development_type',
    #'edges',
    #'employment_controls',
    #'fee_schedule',
    #'household_controls'
    #'households',
    #'jobs',
    #'ludu2012',
    #'ludu2015',
    #'nodes',
    #'parcels',
    #'parks',
    #'scheduled_development',
    #'scheduled_development_parcels',
    #'schools',
    'sr13capacity_ludu2015',
    #'transit',
    #'zoning_allowed_use',
    #'zoning',
]

sql_in_engine = create_engine(get_connection_string("dbconfig.yml", 'in_db'))
sql_out_engine = create_engine(get_connection_string("dbconfig.yml", 'out_db'))
schema = datasets['schema']
print 'Schema', schema

metadata = MetaData(bind=sql_out_engine, schema=schema)

##PROCESS SELECTED DATASETS
for key in selected:
    dataset = datasets[key]
    print ">>> {0}".format(key)

    ##INPUT QUERY
    in_query_non_spatial = dataset['in_query_non_spatial']

    ##Pandas Data Frame for non-spatial data
    df_non_spatial = pd.read_sql(in_query_non_spatial, sql_in_engine, index_col= dataset['index_col'])
    print 'Loaded Non-Spatial Query'

    ##CHECK FOR SPATIAL DATA PROCESSING >>
    if dataset.has_key('in_query_spatial'):
        ##PANDAS DATAFRAME FOR SPATIAL DATA
        in_query_spatial = dataset['in_query_spatial']
        df_spatial = pd.read_sql(in_query_spatial, sql_in_engine, index_col=dataset['index_col'])
        print 'Loaded Spatial Query'

        for col in df_spatial.columns:
            #Transform Shape from SPCS to WGS --> See method above for details
            s = df_spatial[col].apply(lambda x: transform_wkt(x))
            df_spatial[col] = s
        print 'Transformed Shapes'

        #Join spatial and non-spatial frames
        df = pd.concat([df_non_spatial, df_spatial], axis = 1)
        df.index.name = df_non_spatial.index.name
    else:
        df = df_non_spatial
        print 'Non-spatial Dataset'
    ##SPATIAL DATA PROCESSING <<

    ##Output
    out_table = dataset['out_table']

    #Map columns --> Notice special geometry column
    column_data_types = dataset['column_data_types']

    print 'Start Data Load'

    tbl = Table(out_table, metadata)
    for column_name, column_type in column_data_types.iteritems():
        tbl.append_column(Column(column_name, column_type, primary_key=column_name==df.index.name))

    if tbl.exists():
        tbl.drop()
    tbl.create()

    #Write PostgreSQL
    df.to_sql(out_table, sql_out_engine, schema=schema, if_exists='append', index=True, dtype=column_data_types)
    #df.to_csv(out_table+'.csv')

    print "Table Loaded to {0}".format(out_table)

    indexes = dataset['indexes']
    tbl = Table(out_table, metadata, autoload=True)

    for name, col in indexes.iteritems():
        print "Building Index: " + name
        idx = Index(name, tbl.c[col])
        idx.create()

    print "Index Creation Complete"



    print '*' * 30