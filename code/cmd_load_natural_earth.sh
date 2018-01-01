#!/bin/bash


tablepath=$1
tablename=$2


echo """
    -- create natural-earth schema :

    CREATE SCHEMA IF NOT EXISTS ne;
    DROP TABLE IF EXISTS ne.${tablename} CASCADE;

    --
""" | psql -e

ogr2ogr \
    -f Postgresql \
    -lco DIM=2 \
    -lco GEOMETRY_NAME=geometry \
    -lco SCHEMA=ne \
    -lco precision=NO \
    -nlt GEOMETRY \
    -overwrite \
    -progress \
    -s_srs EPSG:4326 \
    -t_srs EPSG:4326 \
    PG:"dbname=$PGDATABASE user=$PGUSER host=$PGHOST password=$PGPASSWORD port=$PGPORT" \
    "/wof/natural-earth-vector/${tablepath}/${tablename}.shp"


echo """

    ANALYSE ne.${tablename} ;

    -- test .. ;

    \d+ ne.${tablename};

    -- \x
    -- select *  from ne.${tablename} limit 2;
    
    --
    select featurecla, count(*) as N from ne.${tablename} group by featurecla order by N desc;
    --
""" | psql -e
