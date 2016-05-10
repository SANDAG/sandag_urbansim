import geoalchemy2
import ogr
import pandas as pd
from pysandag.database import get_connection_string
from pysandag.gis import  transform_wkt
import sqlalchemy
from sqlalchemy import create_engine

#GET THE CONNECTION STRINGS
in_connection_string = get_connection_string("dbconfig.yml", 'in_db')
out_connection_string = get_connection_string("dbconfig.yml", 'out_db')

##Input Query
in_query_non_spatial = """
SELECT building_id
	,development_type_id
	,parcel_id
	,improvement_value
	,residential_units
	,residential_sqft
	,non_residential_sqft
    ,job_spaces
    ,non_residential_rent_per_sqft
	,price_per_sqft
	,stories
	,year_built
FROM spacecore.urbansim.buildings
WHERE building_id < 100
"""
##MSSQL SQLAlchemy
sql_in_engine = create_engine(in_connection_string)
##Pandas Data Frame for non-spatial data
df_non_spatial = pd.read_sql(in_query_non_spatial, sql_in_engine, index_col = 'building_id')
print 'Loaded Non-Spatial Query'

##Pandas Data Frame for spatial data
in_query_spatial = """
  SELECT building_id, shape.STAsText() AS shape FROM spacecore.urbansim.buildings WHERE building_id < 100
"""
df_spatial = pd.read_sql(in_query_spatial, sql_in_engine, index_col='building_id')
print 'Loaded Spatial Query'

#Transform Shape from SPCS to WGS --> See method above for details
s = df_spatial['shape'].apply(lambda x: transform_wkt(x))
df_spatial['shape'] = s
print 'Transformed Shapes'

#Join spatial and non-spatial frames
df = pd.concat([df_non_spatial, df_spatial], axis = 1)

##Output
out_table = 'buildings'

#Map columns --> Notice special geometry column
column_data_types = {
    'building_id' : sqlalchemy.Integer,
    'development_type_id' : sqlalchemy.Integer,
    'parcel_id' : sqlalchemy.Integer,
    'improvement_value' : sqlalchemy.Float,
    'residential_units' : sqlalchemy.Integer,
    'residential_sqft' : sqlalchemy.Integer,
    'non_residential_sqft' : sqlalchemy.Integer,
    'job_spaces': sqlalchemy.Integer,
    'non_residential_rent_per_sqft': sqlalchemy.Float,
    'price_per_sqft' : sqlalchemy.Float,
    'stories' : sqlalchemy.Integer,
    'year_built' : sqlalchemy.Integer,
    'shape' : geoalchemy2.Geography('Geometry', srid=4326)
}

print 'Start Data Load'
##PostgreSQL SQLAlchemy
sql_out_engine = create_engine(out_connection_string)

#Write PostgreSQL
df.to_sql(out_table, sql_out_engine, schema='urbansim', if_exists='replace', index=True, dtype = column_data_types)

print "Table Loaded to {0}".format(out_table)
