#!/bin/bash
  
ogr2ogr \
    -clipsrc -180.1 -85.0511 180.1 85.0511 \
    -f Postgresql \
    -lco DIM=2 \
    -lco GEOMETRY_NAME=geometry \
    -nlt GEOMETRY \
    -overwrite \
    -progress \
    -s_srs EPSG:4326 \
    -t_srs EPSG:3857 \
    PG:"dbname=$PGDATABASE user=$PGUSER host=$PGHOST password=$PGPASSWORD port=$PGPORT" \
    "/wof/natural-earth-vector/10m_cultural/ne_10m_populated_places.shp"

