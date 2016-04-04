__author__ = 'esa'

import geoalchemy2
import pandas as pd
import pyodbc
import sqlalchemy
import yaml

from osgeo import ogr
from osgeo import osr
from sqlalchemy import create_engine

"""
===============================================
Function to Build Connection Strings from YAML
===============================================
"""
def get_connection_string(cfg_file, cfg_section):
    with open(cfg_file, 'r') as dbconfig:
        db_cfg = yaml.load(dbconfig)
    
    alchemy_driver = db_cfg[cfg_section]['sql_alchemy_driver']
    driver = db_cfg[cfg_section]['driver']
    host = db_cfg[cfg_section]['host']
    database = db_cfg[cfg_section]['database']
    port = db_cfg[cfg_section]['port']
    user = db_cfg[cfg_section]['user']
    password = db_cfg[cfg_section]['password']

    if user is not None and password is not None:
        credentials = "{0}:{1}@".format(user, password)
    else:
        credentials = ""

    if port is not None:
        port_fmt = ":{0}".format(port)
    else:
        port_fmt = ""

    if database is not None:
        database_fmt = "/{0}".format(database)
    else:
        database_fmt = ""
    
    if driver is not None:
        driver_fmt = "?driver={0}".format(driver)
    else:
        driver_fmt = ""
    
    return "{0}://{1}{2}{3}{4}{5}".format(
        alchemy_driver, credentials, host,
        port_fmt, database_fmt, driver_fmt) 

#GET THE CONNECTION STRINGS
in_connection_string = get_connection_string("E:/Apps/urbansim/sandag_urbansim/scripts/dbconfig.yml", 'in_db')
out_connection_string = get_connection_string("E:/Apps/urbansim/sandag_urbansim/scripts/dbconfig.yml", 'out_db')

#SET UP State Plane to WGS Transformation
source = osr.SpatialReference()
source.ImportFromEPSG(2230)

target = osr.SpatialReference()
target.ImportFromEPSG(4326)
transform = osr.CoordinateTransformation(source, target)

"""
===============================
TRANSFORMATION FUNCTION
===============================
"""
def TransformWKT(geomWkt):
    geomBinary = ogr.CreateGeometryFromWkt(geomWkt)
    geomBinary.Transform(transform)
    return "SRID=4326;" + geomBinary.ExportToWkt()

##Input Query
in_query_non_spatial = """
  SELECT
    bldg.bldgID as building_id
    ,bldg.parcelID as parcel_id
    ,bldg.devTypeID as development_type_id
    ,bldg.bldgFloor as stories
    ,bldg.effectDate as year_built
    ,sum(CAST(COALESCE(val.asrImprov, 0) / bldgs_parcel.buildings as bigint)) as improvement_value
    ,sum(COALESCE(res.resSqft, 0)) as residential_sqft
    ,sum(COALESCE(res.resDU, 0)) as residential_units
    ,sum(COALESCE(nres.nResSqft, 0)) as non_residential_sqft
    ,SUM(COALESCE(rent.rent, 0)) as price_per_sqft
  FROM
    spacecore_dev.building bldg
    JOIN (SELECT parcelID, count(*) buildings FROM spacecore_dev.BUILDING GROUP BY parcelID) bldgs_parcel ON bldg.parcelID = bldgs_parcel.parcelID
    LEFT JOIN spacecore_dev.landcore lc ON bldg.parcelID = lc.parcelID
    LEFT JOIN spacecore_dev.apnvaluation val ON lc.apn = val.apn
    LEFT JOIN (SELECT bldgID, count(*) as resDU, sum(reSqft) resSqFt FROM spacecore_dev.RESSPACE GROUP BY bldgID) res ON bldg.bldgID = res.bldgID
    LEFT JOIN (SELECT bldgID,sum(nResSqft) nResSqFt FROM spacecore_dev.NONRESSPACE GROUP BY bldgID) nres ON bldg.bldgID = nres.bldgID
    LEFT JOIN spacecore_dev.rentinfo rent ON bldg.bldgID = rent.bldgID
  GROUP BY
    bldg.bldgID
    ,bldg.parcelID
    ,bldg.devTypeID
    ,bldg.bldgFloor
    ,bldg.effectDate 
"""
##MSSQL SQLAlchemy
sql_in_engine = create_engine(in_connection_string)
##Pandas Data Frame for non-spatial data
bldg_df_non_spatial = pd.read_sql(in_query_non_spatial, sql_in_engine, index_col = 'building_id')
print bldg_df_non_spatial

##Pandas Data Frame for spatial data
in_query_spatial = """
  SELECT bldgID as building_id, shape.STAsText() as shape FROM spacecore_dev.BUILDING
"""
bldg_df_spatial = pd.read_sql(in_query_spatial, sql_in_engine, index_col='building_id')

#Transform Shape from SPCS to WGS --> See method above for details
s = bldg_df_spatial['shape'].apply(lambda x: TransformWKT(x))
bldg_df_spatial['shape'] = s

#Join spatial and non-spatial frames
bldg_df = pd.concat([bldg_df_non_spatial, bldg_df_spatial], axis = 1)


##Output
out_table = 'buildings'

#Map columns --> Notice special geometry column
column_data_types = {
    'building_id' : sqlalchemy.Integer,
    'parcel_id' : sqlalchemy.Integer,
    'development_type_id' : sqlalchemy.Integer,
    'stories' : sqlalchemy.Integer,
    'year_built' : sqlalchemy.DateTime,
    'improvement_value' : sqlalchemy.Integer,
    'residential_units' : sqlalchemy.Integer,
    'residential_sqft' : sqlalchemy.Integer,
    'non_residential_sqft' : sqlalchemy.Integer,
    'price_per_sqft' : sqlalchemy.Integer,
    'shape' : geoalchemy2.Geometry('Polygon', srid=4326)
}

##PostgreSQL SQLAlchemy
sql_out_engine = create_engine(out_connection_string)

#Write PostgreSQL
bldg_df.to_sql(out_table, sql_out_engine, schema='public', if_exists='replace',
              index=True, dtype = column_data_types)

print ">>>Table Loaded to {}".format(out_table)