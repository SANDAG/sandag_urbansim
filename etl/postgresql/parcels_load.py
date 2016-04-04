import geoalchemy2
import pandas as pd
import sqlalchemy
from util import get_connection_string, transform_wkt
from sqlalchemy import create_engine

#GET THE CONNECTION STRINGS
in_connection_string = get_connection_string("dbconfig.yml", 'in_db')
out_connection_string = get_connection_string("dbconfig.yml", 'out_db')

##Input Query
in_query_non_spatial = """
SELECT
    parcel_id
    ,development_type_id
    ,zoning_id
    ,land_value
    ,parcel_acres
    ,region_id
    ,mgra_id
    ,luz_id
    ,msa_id
    ,proportion_undevelopable
    ,tax_exempt_status
FROM
  spacecore.urbansim.parcels
"""
##MSSQL SQLAlchemy
sql_in_engine = create_engine(in_connection_string)
##Pandas Data Frame for non-spatial data
df_non_spatial = pd.read_sql(in_query_non_spatial, sql_in_engine, index_col='parcel_id')
print 'Loaded Non-Spatial Query'

##Pandas Data Frame for spatial data
in_query_spatial = """
  SELECT parcel_id, shape.STAsText() as shape, centroid.STAsText() as centroid FROM spacecore.urbansim.parcels
"""
df_spatial = pd.read_sql(in_query_spatial, sql_in_engine, index_col='parcel_id')
print 'Loaded Spatial Query'

#Transform Shape from SPCS to WGS --> See method above for details
s = df_spatial['shape'].apply(lambda x: transform_wkt(x))
df_spatial['shape'] = s

s = df_spatial['centroid'].apply(lambda x: transform_wkt(x))
df_spatial['centroid'] = s

print 'Transformed Shapes'

#Join spatial and non-spatial frames
df = pd.concat([df_non_spatial, df_spatial], axis = 1)

##Output
out_table = 'parcels'

#Map columns --> Notice special geometry column
column_data_types = {
    'parcel_id' : sqlalchemy.Integer,
    'development_type_id' : sqlalchemy.Integer,
    'zoning_id': sqlalchemy.VARCHAR,
    'land_value' : sqlalchemy.Integer,
    'parcel_acres' : sqlalchemy.Float,
    'region_id' : sqlalchemy.Integer,
    'mgra_id' : sqlalchemy.Integer,
    'luz_id' : sqlalchemy.Integer,
    'msa_id' : sqlalchemy.Integer,
    'proportion_undevelopable' : sqlalchemy.Numeric,
    'tax_exempt_status' : sqlalchemy.String,
    'shape_wkt': sqlalchemy.String,
    'shape' : geoalchemy2.Geography('Geometry', srid=4326),
    'centroid': geoalchemy2.Geography('Point', srid=4326)
}

print 'Start Data Load'
##PostgreSQL SQLAlchemy
sql_out_engine = create_engine(out_connection_string)

#Write PostgreSQL
df.to_sql(out_table, sql_out_engine, schema='urbansim', if_exists='replace', index=True, dtype = column_data_types)

print "Table Loaded to {0}".format(out_table)
