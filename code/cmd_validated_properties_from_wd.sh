#!/bin/bash

set -e
set -u


source_table=$1
target_table=${1}_validated
target_sqlite3_table=$(echo "${target_table}" | cut -d'.' -f2)

echo """

    DROP TABLE IF EXISTS ${target_table} CASCADE ;
    CREATE UNLOGGED TABLE ${target_table} AS
        SELECT   wof.id
                ,wof.metatable
                ,wof.wof_name
                ,wof.wof_country
                ,wof._matching_category
                ,wd.* 
        FROM  wfwd.wof_validated_suggested_list as wof 
             ,${source_table}                   as wd 
        WHERE wof.wd_id=wd.wd_id
        ORDER BY wof_country, wof_name 
    ;

    CREATE INDEX ON ${target_table} (wof_country) ;  

    -- analyze --

    --------------  ANALYZE ${target_table};

    -- test --

    SELECT count(*) as _count 
    FROM ${target_table};

    -- export --
    \cd :reportdir 
    \copy (SELECT * FROM ${target_table} ORDER BY wof_country, wof_name  ) TO '${target_table}.csv' DELIMITER ',' CSV HEADER ESCAPE '\"';

""" | psql -e -vreportdir="${outputdir}"


echo "${outputdir}/z${target_table}.db"


sqlite3 -batch ${outputdir}/${target_table}.db   <<EOF

.mode csv
.import ${outputdir}/${target_table}.csv  ${target_sqlite3_table}

EOF

ls -la  ${outputdir}/${target_table}.db

sqlite3  ${outputdir}/${target_table}.db  "PRAGMA table_info( ${target_sqlite3_table})" > ${outputdir}/${target_table}.dbstruct.txt