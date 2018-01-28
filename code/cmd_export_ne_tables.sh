#!/bin/bash

set -e
set -u

source_table=$1
target_table=${1}
target_sqlite3_table=$(echo "${target_table}" | cut -d'.' -f2)

function neexport(){  
nedir=$1 
sqlfilter=$2

mkdir -p ${nedir}
##  where substr(_matching_category,1,2) in ('OK','WA') 
echo """
    -- export --
    \cd :reportdir 
    \copy (SELECT * FROM ${source_table}  ${sqlfilter}  ORDER BY ogc_fid  ) TO '${target_table}.csv' DELIMITER ',' CSV HEADER ESCAPE '\"';
""" | psql -e -vreportdir="${nedir}"

echo "${nedir}/${target_table}.db"
sqlite3 -batch ${nedir}/${target_table}.db   <<EOF
.mode csv
.import ${nedir}/${target_table}.csv  ${target_sqlite3_table}
EOF

ls -la  ${nedir}/${target_table}.db
sqlite3  ${nedir}/${target_table}.db  "PRAGMA table_info( ${target_sqlite3_table})" > ${nedir}/${target_table}.dbstruct.txt
ls -la  ${nedir}/${target_table}.*
}


neexport ${outputdir}/ne     "where 1=1"

neexport ${outputdir}/ne_ok   "where substr(_matching_category,1,2) in ('OK','MA')"

