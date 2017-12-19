#!/bin/bash

set -e
set -u

source_table=$1
target_table=${1}
target_sqlite3_table=$(echo "${target_table}" | cut -d'.' -f2)

echo """
    -- export --
    \cd :reportdir 
    \copy (SELECT * FROM ${source_table} ORDER BY wof_country, wof_name  ) TO '${target_table}.csv' DELIMITER ',' CSV HEADER ESCAPE '\"';

""" | psql -e -vreportdir="${outputdir}"


echo "${outputdir}/z${target_table}.db"

sqlite3 -batch ${outputdir}/${target_table}.db   <<EOF

.mode csv
.import ${outputdir}/${target_table}.csv  ${target_sqlite3_table}

EOF

ls -la  ${outputdir}/${target_table}.db

sqlite3  ${outputdir}/${target_table}.db  "PRAGMA table_info( ${target_sqlite3_table})" > ${outputdir}/${target_table}.dbstruct.txt