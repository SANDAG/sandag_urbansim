import pandas as pd
import sqlalchemy
from pysandag.database import get_connection_string
from sqlalchemy import create_engine

#GET THE CONNECTION STRINGS
in_connection_string = get_connection_string("dbconfig.yml", 'in_db')
out_connection_string = get_connection_string("dbconfig.yml", 'out_db')

##Input Query
in_query = """
SELECT household_id
    ,scenario_id
    ,building_id
    ,tenure
    ,persons
    ,workers
    ,age_of_head
    ,income
    ,children
    ,race_id
    ,cars
FROM spacecore.urbansim.households
"""
##MSSQL SQLAlchemy
sql_in_engine = create_engine(in_connection_string)
##Pandas Data Frame
df = pd.read_sql(in_query, sql_in_engine, index_col = 'household_id')
print 'Loaded Query'

##Output
out_table = 'households'

#Map columns
column_data_types = {
    'household_id' : sqlalchemy.Integer,
    'scenario_id' : sqlalchemy.Integer,
    'building_id' : sqlalchemy.Integer,
    'tenure' : sqlalchemy.Integer,
    'persons' : sqlalchemy.Integer,
    'workers' : sqlalchemy.Integer,
    'age_of_head' : sqlalchemy.Integer,
    'income' : sqlalchemy.Integer,
    'children' : sqlalchemy.Integer,
    'race_id' : sqlalchemy.Integer,
    'cars' : sqlalchemy.Integer
}

print 'Start Data Load'

##PostgreSQL SQLAlchemy
sql_out_engine = create_engine(out_connection_string)

#Write PostgreSQL
df.to_sql(out_table, sql_out_engine, schema='urbansim', if_exists='replace',
              index=True, dtype = column_data_types)

print "Table Loaded to {}".format(out_table)
