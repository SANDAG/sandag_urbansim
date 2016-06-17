import geoalchemy2
import pandas as pd
import sqlalchemy
from pysandag.database import get_connection_string
from pysandag.gis import transform_wkt
from sqlalchemy import create_engine

#GET THE CONNECTION STRINGS
in_connection_string = get_connection_string("dbconfig.yml", 'in_db')
out_connection_string = get_connection_string("dbconfig.yml", 'out_db')

##Input Query
in_query_non_spatial = """
SELECT
    zoning_id
    ,jurisdiction_id
    ,zone_code
    ,region_id
    ,min_far
    ,max_far
    ,min_front_setback
    ,max_front_setback
    ,rear_setback
    ,side_setback
    ,min_dua
    ,max_dua
    ,max_building_height
--    ,allowed_uses
FROM spacecore.urbansim.zoning

"""
##MSSQL SQLAlchemy
sql_in_engine = create_engine(in_connection_string)
##Pandas Data Frame
df_non_spatial = pd.read_sql(in_query_non_spatial, sql_in_engine, index_col = 'zoning_id')
print 'Loaded Non-Spatial Query'

##Pandas Data Frame for spatial data
in_query_spatial = """
SELECT
    zoning_id
    ,shape.STAsText() as shape
FROM spacecore.urbansim.zoning
WHERE shape IS NOT NULL
"""
df_spatial = pd.read_sql(in_query_spatial, sql_in_engine, index_col='zoning_id')
print 'Loaded Spatial Query'

#Transform Shape from SPCS to WGS --> See method above for details
s = df_spatial['shape'].apply(lambda x: transform_wkt(x))
df_spatial['shape'] = s

print 'Transformed Shapes'

#Join spatial and non-spatial frames
df = pd.concat([df_non_spatial, df_spatial], axis = 1)
df.index.name = df_non_spatial.index.name

##Output
out_table = 'zoning'

#Map columns --> Notice special geometry column
column_data_types = {
    'zoning_id' : sqlalchemy.String,
    'jurisdiction_id' : sqlalchemy.Integer,
    'zone_code' : sqlalchemy.String,
    'region_id' : sqlalchemy.Integer,
    'min_far' : sqlalchemy.Numeric,
    'max_far' : sqlalchemy.Numeric,
    'min_front_setback' : sqlalchemy.Numeric,
    'max_front_setback' : sqlalchemy.Numeric,
    'rear_setback' : sqlalchemy.Numeric,
    'side_setback' : sqlalchemy.Numeric,
    'min_dua' : sqlalchemy.Numeric,
    'max_dua' : sqlalchemy.Numeric,
    'max_building_height' : sqlalchemy.Integer,
    'allowed_uses' : sqlalchemy.String,
    'shape' : geoalchemy2.Geography('Geometry', srid=4326)
}

print 'Start Data Load'
##PostgreSQL SQLAlchemy
sql_out_engine = create_engine(out_connection_string)

#Write PostgreSQL
df.to_sql(out_table, sql_out_engine, schema='urbansim', if_exists='replace', index=True, dtype = column_data_types)

print "Table Loaded to {}".format(out_table)