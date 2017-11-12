#!/bin/bash

echo """
    -- create natural-earth schema :

    CREATE SCHEMA IF NOT EXISTS ne;

    --
""" | psql

ogr2ogr \
    -f Postgresql \
    -lco DIM=2 \
    -lco GEOMETRY_NAME=geometry \
    -nlt GEOMETRY \
    -overwrite \
    -progress \
    -s_srs EPSG:4326 \
    -t_srs EPSG:4326 \
    -lco SCHEMA=ne \
    PG:"dbname=$PGDATABASE user=$PGUSER host=$PGHOST password=$PGPASSWORD port=$PGPORT" \
    "/wof/natural-earth-vector/10m_cultural/ne_10m_populated_places.shp"


echo """
    -- test .. ;

    \d+ ne.ne_10m_populated_places;

    select ogc_fid,scalerank,iso_a2,name  from ne.ne_10m_populated_places limit 10;

    --
""" | psql
