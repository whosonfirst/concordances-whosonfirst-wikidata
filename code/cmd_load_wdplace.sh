#!/bin/bash

set -e
set -u

part=$1


echo "START:======== load wdpalce ${part}  ==========="

echo """
    --
    CREATE SCHEMA IF NOT EXISTS wdplace;
    DROP TABLE IF EXISTS wdplace.wd${part} CASCADE;
    --
""" | psql -e

time pgfutter --schema wdplace --table wd${part} --jsonb  json /wof/wikidata_dump/wdplace0${part}.json

echo """
    --
    -- ANALYZE wdplace.wd${part};
    --
    SELECT count(*) FROM wdplace.wd${part} ;
    --
    \d+ wdplace.wd${part} 
    --
""" | psql -e



echo """
    drop table if exists  wdplace.wd${part}_x CASCADE;
    create table  wdplace.wd${part}_x as

    select
         data->>'id'::text                      as wd_id
        ,get_wdlabeltext(data->>'id'::text)     as wd_name_en
        ,(regexp_split_to_array( get_wdlabeltext(data->>'id'::text), '[,()]'))[1]   as wd_name_en_clean
        ,is_cebuano(data)                       as wd_is_cebuano
        ,get_wdc_value(data, 'P1566')           as p1566_geonames    
        ,ST_SetSRID(ST_MakePoint( 
                     cast(get_wdc_globecoordinate(data,'P625')->0->>'longitude' as double precision)
                    ,cast(get_wdc_globecoordinate(data,'P625')->0->>'latitude'  as double precision)
                    )
            , 4326) as wd_point
        ,get_wdc_item_label(data,'P31')    as p31_instance_of
        ,get_wdc_item_label(data,'P17')    as p17_country_id 
        ,get_wd_name_array(data)           as wd_name_array 
        ,get_wd_altname_array(data)        as wd_altname_array
    FROM wdplace.wd${part}
    ;

    --

    \d+ wdplace.wd${part}_x 
    
    --

""" | psql -e

echo "END:======== load wdpalce ${part} ==========="
