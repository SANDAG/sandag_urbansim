E:\OSGeo4W64\bin\ogr2ogr.exe -f MSSQLSpatial "MSSQL:server=sql2014a8;database=spacecore;trusted_connection=yes" E:\data\urbansim_data_development\buildings\Sampled_SANDAG_footprints_merged.shp -nln buildings_updated -lco SCHEMA="GIS" -s_srs EPSG:2230 -t_srs EPSG:2230 -lco OVERWRITE=YES -OVERWRITE