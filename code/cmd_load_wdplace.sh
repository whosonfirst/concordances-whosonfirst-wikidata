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
    -- CREATE INDEX wdplace_wd${part}_gin  ON wdplace.wd${part} USING GIN (data);
    --
    ANALYZE wdplace.wd${part};
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
        ,get_wdc_value(data, 'P1566')           as p1566_geonames    
        ,ST_SetSRID(ST_MakePoint( 
                     cast(get_wdc_globecoordinate(data,'P625')->0->>'longitude' as double precision)
                    ,cast(get_wdc_globecoordinate(data,'P625')->0->>'latitude'  as double precision)
                    )
            , 4326) as wd_point
        ,get_wdc_item_label(data,'P31')    as p31_instance_of
        ,get_wdc_item_label(data,'P17')    as p17_country_id     
        ,get_wdc_item_label(data,'P36')    as p36_capital
        ,get_wdc_item_label(data,'P1376')  as p1376_capital_of
        ,get_wdc_item_label(data,'P190')   as p190_sister_city
        ,get_wdc_item_label(data,'P460')   as p460_same_as
        ,get_wdc_population(data,'P1082')  as p1082_population 
        ,get_wdc_value(data, 'P300')       as p300_iso3166_2
        ,get_wdc_value(data, 'P901')       as p901_fips10_4
    FROM wdplace.wd${part}
    ORDER BY data->>'id'::text
    ;

    CREATE INDEX wdplace_wd${part}_x_point      ON wdplace.wd${part}_x  USING GIST(wd_point);
    CREATE INDEX wdplace_wd${part}_x_wdid       ON wdplace.wd${part}_x ( wd_id );
    CREATE INDEX wdplace_wd${part}_x_nameclean  ON wdplace.wd${part}_x ( wd_name_en_clean );

    ANALYSE  wdplace.wd${part}_x ;
    --
    
    \d+ wdplace.wd${part}_x 
    
    --

""" | psql -e

echo "END:======== load wdpalce ${part} ==========="
