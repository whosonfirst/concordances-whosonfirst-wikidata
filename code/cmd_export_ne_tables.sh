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
rm -f ${nedir}/${target_table}.csv

echo """
    -- export --
    CREATE TEMP VIEW ne_export as
    SELECT     
         ogc_fid
        ,min_zoom
        ,featurecla
        ,ne_name
        ,ne_wd_id
        ,new_wd_label       
        ,_suggested_wd_id
        ,_matching_category
        ,name_ar
        ,name_bn
        ,name_de
        ,name_en
        ,name_es
        ,name_fr
        ,name_el
        ,name_hi
        ,name_hu
        ,name_id
        ,name_it
        ,name_ja
        ,name_ko
        ,name_nl
        ,name_pl
        ,name_pt
        ,name_ru
        ,name_sv
        ,name_tr
        ,name_vi
        ,name_zh
        ,wd_long
        ,wd_lat
        ,ne_long
        ,ne_lat
        ,a_wd_id
        ,a_wd_id_score
        ,a_wd_id_distance
        ,a_wd_id_jarowinkler
        ,a_wd_name_match_type
        ,a_wd_name_en
        ,a_step
        ,wd_number_of_matches
        ,_firstmatch_distance_category
        ,a_wof_type
        ,old_p31_instance_of
        ,new_p31_instance_of
        ,old_p17_country_id
        ,new_p17_country_id
        ,old_wd_label
        ,old_is_cebauno    
     FROM ${source_table}  ${sqlfilter}  ORDER BY ogc_fid
     ; 
    \cd :reportdir 
    \copy ( select * from ne_export ) TO '${target_table}.csv' DELIMITER ',' CSV HEADER ESCAPE '\"';
""" | psql -e -vreportdir="${nedir}"

echo "${nedir}/${target_table}.db  import"
rm -f  ${nedir}/${target_table}.db
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

