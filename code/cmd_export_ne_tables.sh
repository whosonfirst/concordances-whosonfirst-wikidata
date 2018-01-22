#!/bin/bash

set -e
set -u

source_table=$1
target_table=${1}
target_sqlite3_table=$(echo "${target_table}" | cut -d'.' -f2)


mkdir -p ${outputdir}/ne


##  where substr(_matching_category,1,2) in ('OK','WA') 
echo """
    -- export --
    \cd :reportdir 
    \copy (SELECT * FROM ${source_table}  ORDER BY ogc_fid  ) TO '${target_table}.csv' DELIMITER ',' CSV HEADER ESCAPE '\"';

""" | psql -e -vreportdir="${outputdir}/ne"


echo "${outputdir}/ne/${target_table}.db"

sqlite3 -batch ${outputdir}/ne/${target_table}.db   <<EOF

.mode csv
.import ${outputdir}/ne/${target_table}.csv  ${target_sqlite3_table}

EOF

ls -la  ${outputdir}/ne/${target_table}.db

sqlite3  ${outputdir}/ne/${target_table}.db  "PRAGMA table_info( ${target_sqlite3_table})" > ${outputdir}/ne/${target_table}.dbstruct.txt


#gzip ${outputdir}/ne/${target_table}.db
#gzip ${outputdir}/ne/${target_table}.csv

ls -la  ${outputdir}/ne/${target_table}.*
