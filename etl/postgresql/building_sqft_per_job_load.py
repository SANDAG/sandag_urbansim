import pandas as pd
import sqlalchemy
from pysandag.database import get_connection_string
from sqlalchemy import create_engine

#GET THE CONNECTION STRINGS
in_connection_string = get_connection_string("dbconfig.yml", 'in_db')
out_connection_string = get_connection_string("dbconfig.yml", 'out_db')

##Input Query
in_query = """
SELECT id
    ,luz_id
    ,development_type_id
    ,sqft_per_emp
FROM spacecore.urbansim.building_sqft_per_job
"""
##MSSQL SQLAlchemy
sql_in_engine = create_engine(in_connection_string)
##Pandas Data Frame
df = pd.read_sql(in_query, sql_in_engine, index_col = 'id')
print 'Loaded Query'

##Output
out_table = 'building_sqft_per_job'

#Map columns
column_data_types = {

    'id' : sqlalchemy.Integer,
    'luz_id' : sqlalchemy.Integer,
    'development_type_id' : sqlalchemy.Integer,
    'sqft_per_emp' : sqlalchemy.Float
}

print 'Start Data Load'

##PostgreSQL SQLAlchemy
sql_out_engine = create_engine(out_connection_string)

#Write PostgreSQL
df.to_sql(out_table, sql_out_engine, schema='urbansim', if_exists='replace',
              index=True, dtype = column_data_types)

print "Table Loaded to {}".format(out_table)
