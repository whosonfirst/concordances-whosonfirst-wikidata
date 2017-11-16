#!/bin/bash

echo """
    -- create natural-earth schema :

    CREATE SCHEMA IF NOT EXISTS ne;
    DROP TABLE IF EXISTS ne.ne_10m_populated_places CASCADE;

    --
""" | psql -e

ogr2ogr \
    -f Postgresql \
    -lco DIM=2 \
    -lco GEOMETRY_NAME=geometry \
    -lco SCHEMA=ne \
    -nlt GEOMETRY \
    -overwrite \
    -progress \
    -s_srs EPSG:4326 \
    -t_srs EPSG:4326 \
    PG:"dbname=$PGDATABASE user=$PGUSER host=$PGHOST password=$PGPASSWORD port=$PGPORT" \
    "/wof/natural-earth-vector/10m_cultural/ne_10m_populated_places.shp"


echo """

    ANALYSE ne.ne_10m_populated_places ;

    -- test .. ;

    \d+ ne.ne_10m_populated_places;

    select ogc_fid,scalerank,iso_a2,name  from ne.ne_10m_populated_places limit 10;

    --
""" | psql -e
