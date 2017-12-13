#!/bin/bash

set -e
set -u


source_table=$1
target_table=${1}_validated

echo """

    DROP TABLE IF EXISTS ${target_table} CASCADE ;
    CREATE TABLE ${target_table} AS
        SELECT   wof.id
                ,wof.metatable
                ,wof.wof_name
                ,wof.wof_country
                ,wd.* 
        FROM  wfwd.wof_validated_suggested_list as wof 
             ,${source_table}    as wd 
        WHERE wof.wd_id=wd.wd_id
        ORDER BY wof_country, wof_name 
    ;

    CREATE INDEX ON ${target_table} (wof_country);  

    -- analyze --

    ANALYZE ${target_table};

    -- test --

    SELECT count(*) as _count 
    FROM ${target_table};

    -- sample --   
    SELECT * 
    FROM ${target_table}
    LIMIT 10;

    -- export --
    \cd :reportdir 
    \copy (SELECT * FROM ${target_table} ORDER BY wof_country, wof_name  ) TO '${target_table}.csv' CSV  HEADER;

""" | psql -e -vreportdir="${outputdir}"
