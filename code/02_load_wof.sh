#!/bin/bash

set -e
set -u
echo "======== 02_load_wof START ==========="


#repo=/wof/whosonfirst-data/
wof_repo=/wof/whosonfirst-data/

function load_wof(){

    table=$1
    csv=$2

    echo """
        DROP TABLE IF EXISTS ${table} CASCADE ;
        CREATE UNLOGGED TABLE ${table} (
            id             BIGINT PRIMARY KEY
            ,parent_id      BIGINT
            ,placetype_id   BIGINT
            ,is_superseded  SMALLINT
            ,is_deprecated  SMALLINT
            ,meta           JSONB
            ,properties     JSONB
            ,geom_hash      CHAR(32)
            ,lastmod        CHAR(25)
            ,geom           GEOGRAPHY(MULTIPOLYGON, 4326)
            ,centroid       GEOGRAPHY(POINT, 4326)
        )
        ;  
    """ | psql


    echo "--------------- load ${table} - with wof-pgis-index -----------------"
    time /wof/go-whosonfirst-pgis/bin/wof-pgis-index \
        -pgis-password $PGPASSWORD \
        -pgis-host     $PGHOST \
        -pgis-table ${table} \
        -mode meta ${wof_repo}meta/${csv}
        
    echo "======== index & test: ${table} ==========="

    echo """

        -- index --
        -- CREATE INDEX ${table}_by_geom        ON ${table} USING GIST(geom);
        -- CREATE INDEX ${table}_by_centroid    ON ${table} USING GIST(centroid);
        -- CREATE INDEX ${table}_by_placetype   ON ${table} (placetype_id);
        -- CREATE INDEX ${table}_by_properties  ON ${table} USING GIN (properties);
        -- CREATE INDEX ${table}_by_propertiesp ON ${table} USING GIN (properties jsonb_path_ops);

        -- analyze --
        analyze ${table};

        -- test --
        SELECT 
            id
        ,placetype_id
        ,properties->>'wof:name'                     AS wof_name
        ,properties->'wof:concordances'->>'wd:id'    AS wd_id
        FROM ${table}
        LIMIT 10;

        ;  
    """ | psql

    echo "======== 02_load_wof END ==========="
}

# psql -f ./wof_sql_step1.sql 


load_wof    wof_borough         wof-borough-latest.csv
load_wof    wof_country         wof-country-latest.csv
load_wof    wof_empire          wof-empire-latest.csv
load_wof    wof_macrohood       wof-macrohood-latest.csv

load_wof    wof_neighbourhood   wof-neighbourhood-latest.csv
load_wof    wof_timezone        wof-timezone-latest.csv
load_wof    wof_campus          wof-campus-latest.csv
load_wof    wof_county          wof-county-latest.csv
load_wof    wof_localadmin      wof-localadmin-latest.csv
load_wof    wof_macroregion     wof-macroregion-latest.csv
load_wof    wof_ocean           wof-ocean-latest.csv

load_wof    wof_dependency      wof-dependency-latest.csv
load_wof    wof_locality        wof-locality-latest.csv 
load_wof    wof_marinearea      wof-marinearea-latest.csv
load_wof    wof_planet          wof-planet-latest.csv
load_wof    wof_continent       wof-continent-latest.csv
load_wof    wof_disputed        wof-disputed-latest.csv
load_wof    wof_macrocounty     wof-macrocounty-latest.csv
load_wof    wof_microhood       wof-microhood-latest.csv
load_wof    wof_region          wof-region-latest.csv

## load_wof    wof_concordances    wof-concordances-latest.csv

    echo """
        create or replace view wof AS
              select 'wof_borough'          as metatable, * from wof_borough         
        union select 'wof_campus'           as metatable, * from wof_campus          
        union select 'wof_continent'        as metatable, * from wof_continent       
        union select 'wof_country'          as metatable, * from wof_country         
        union select 'wof_county'           as metatable, * from wof_county          
        union select 'wof_dependency'       as metatable, * from wof_dependency      
        union select 'wof_disputed'         as metatable, * from wof_disputed        
        union select 'wof_empire'           as metatable, * from wof_empire          
        union select 'wof_localadmin'       as metatable, * from wof_localadmin      
        union select 'wof_locality'         as metatable, * from wof_locality        
        union select 'wof_macrocounty'      as metatable, * from wof_macrocounty     
        union select 'wof_macrohood'        as metatable, * from wof_macrohood       
        union select 'wof_macroregion'      as metatable, * from wof_macroregion     
        union select 'wof_marinearea'       as metatable, * from wof_marinearea      
        union select 'wof_microhood'        as metatable, * from wof_microhood       
        union select 'wof_neighbourhood'    as metatable, * from wof_neighbourhood   
        union select 'wof_ocean'            as metatable, * from wof_ocean           
        union select 'wof_planet'           as metatable, * from wof_planet          
        union select 'wof_region'           as metatable, * from wof_region          
        union select 'wof_timezone'         as metatable, * from wof_timezone        
        ;  
    """ | psql

exit 