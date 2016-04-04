import pandas as pd
import sqlalchemy
from util import get_connection_string
from sqlalchemy import create_engine

#GET THE CONNECTION STRINGS
in_connection_string = get_connection_string("dbconfig.yml", 'in_db')
out_connection_string = get_connection_string("dbconfig.yml", 'out_db')

##Input Query
in_query_non_spatial = """
SELECT
    development_type_id
	,name
	,building_form
	,site_use
	,priority
FROM spacecore.ref.development_type
"""
##MSSQL SQLAlchemy
sql_in_engine = create_engine(in_connection_string)
##Pandas Data Frame
df = pd.read_sql(in_query_non_spatial, sql_in_engine, index_col = 'development_type_id')
print 'Loaded Non-Spatial Query'

##Output
out_table = 'development_type'

#Map columns --> Notice special geometry column
column_data_types = {
    'development_type_id' : sqlalchemy.Integer,
    'name' : sqlalchemy.String,
    'building_form' : sqlalchemy.String,
    'site_use' : sqlalchemy.String,
    'priority' : sqlalchemy.Integer,

}

print 'Start Data Load'
##PostgreSQL SQLAlchemy
sql_out_engine = create_engine(out_connection_string)

#Write PostgreSQL
df.to_sql(out_table, sql_out_engine, schema='urbansim', if_exists='replace', index=True, dtype = column_data_types)

print "Table Loaded to {}".format(out_table)