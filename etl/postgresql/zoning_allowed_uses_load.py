
import geoalchemy2
import pandas as pd
import sqlalchemy
from util import get_connection_string, TransformWKT
from sqlalchemy import create_engine

#GET THE CONNECTION STRINGS
in_connection_string = get_connection_string("dbconfig.yml", 'in_db')
out_connection_string = get_connection_string("dbconfig.yml", 'out_db')

##Input Query
in_query = """
SELECT zoningID AS zoning_id
  ,lu AS allowed_use_id
  ,effectDate AS effect_date
FROM spacecore_dev.zoninguses
"""
##MSSQL SQLAlchemy
sql_in_engine = create_engine(in_connection_string)
##Pandas Data Frame
df = pd.read_sql(in_query, sql_in_engine, index_col = 'zoning_id')
print 'Loaded Query'

##Output
out_table = 'zoning_allowed_uses'

#Map columns
column_data_types = {
    'zoning_id' : sqlalchemy.Integer,
    'allowed_use_id' : sqlalchemy.Integer,
    'effect_date' : sqlalchemy.Date,
}

print 'Start Data Load'

##PostgreSQL SQLAlchemy
sql_out_engine = create_engine(out_connection_string)

#Write PostgreSQL
df.to_sql(out_table, sql_out_engine, schema='public', if_exists='replace',
              index=True, dtype = column_data_types)

print "Table Loaded to {}".format(out_table)
