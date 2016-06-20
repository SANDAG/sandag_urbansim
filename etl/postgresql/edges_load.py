import pandas as pd
import sqlalchemy
from pysandag.database import get_connection_string
from sqlalchemy import create_engine

#GET THE CONNECTION STRINGS
in_connection_string = get_connection_string("dbconfig.yml", 'in_db')
out_connection_string = get_connection_string("dbconfig.yml", 'out_db')

##Input Query
in_query = """
SELECT from_node
      ,to_node
      ,distance
FROM spacecore.urbansim.edges
"""
##MSSQL SQLAlchemy
sql_in_engine = create_engine(in_connection_string)
##Pandas Data Frame
df = pd.read_sql(in_query, sql_in_engine, index_col = ['from_node', 'to_node'])
print 'Loaded Query'

##Output
out_table = 'edges'

#Map columns
column_data_types = {
    'from_node' : sqlalchemy.Integer,
    'to_node' : sqlalchemy.Integer,
    'distance' : sqlalchemy.Float
}

print 'Start Data Load'

##PostgreSQL SQLAlchemy
sql_out_engine = create_engine(out_connection_string)

#Write PostgreSQL
df.to_sql(out_table, sql_out_engine, schema='urbansim', if_exists='replace',
              index=True, dtype = column_data_types)

print "Table Loaded to {}".format(out_table)
