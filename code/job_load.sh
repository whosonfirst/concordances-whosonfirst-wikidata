#!/bin/bash
set -eo pipefail

cd /wof
STARTTIME=$(date +%s)
STARTDATE=$(date +"%Y%m%dT%H%M")

outputdir=/wof/output/${STARTDATE}
export  outputdir=${outputdir}


mkdir -p ${outputdir}
log_file=${outputdir}/job_${STARTDATE}.log
rm -f $log_file

echo "$log_file" > /wof/output/lastlog.env

# wait for postgres
/wof/code/pg_isready.sh

#  backup log from here ...
exec &> >(tee -a "$log_file")

echo " "
echo "-------------------------------------------------------------------------------------"
echo "--                         WOF  -  Wikidata - DW                                   --"
echo "-------------------------------------------------------------------------------------"
echo ""


date -u

lscpu | grep -E '^Thread|^Core|^Socket|^CPU\('

# -----------------------------------------------------------------------------------------------------

rm -rf ${outputdir}/joblog01
rm -rf ${outputdir}/joblog02
rm -rf ${outputdir}/joblog03
rm -rf ${outputdir}/joblog03b
rm -rf ${outputdir}/joblog03c
rm -rf ${outputdir}/joblog04
rm -rf ${outputdir}/joblog05


#chown postgres:postgres /tablespace/data_wf
#chown postgres:postgres /tablespace/data_wd
#chown postgres:postgres /tablespace/data_work

echo """
 --
 --   CREATE TABLESPACE dbspace_wf   LOCATION '/tablespace/data_wf';
 --   CREATE TABLESPACE dbspace_wd   LOCATION '/tablespace/data_wd';  
 --   CREATE TABLESPACE dbspace_work LOCATION '/tablespace/data_work';  

    CREATE EXTENSION if not exists hstore;
    CREATE EXTENSION if not exists unaccent;  
    CREATE EXTENSION if not exists plpython3u;
    CREATE EXTENSION if not exists cartodb;
    CREATE EXTENSION if not exists pg_similarity;

    DROP SCHEMA IF  EXISTS tiger CASCADE;

    CREATE SCHEMA IF NOT EXISTS wd;
    CREATE SCHEMA IF NOT EXISTS wf;
    CREATE SCHEMA IF NOT EXISTS ne;
    CREATE SCHEMA IF NOT EXISTS gn;
    CREATE SCHEMA IF NOT EXISTS newd;
    CREATE SCHEMA IF NOT EXISTS wfwd;
    CREATE SCHEMA IF NOT EXISTS wfne;
    CREATE SCHEMA IF NOT EXISTS wfgn;

    CREATE SCHEMA IF NOT EXISTS qa;

    CREATE SCHEMA IF NOT EXISTS stat;
    CREATE SCHEMA IF NOT EXISTS codes;
    --
""" | psql -e

time /wof/code/init-taginfo.sh

time parallel --jobs 1 --results ${outputdir}/joblog01 -k  < /wof/code/parallel_joblist_01_load_tables.sh
time /wof/code/pgimp/wof_import_pg_all.sh
time /wof/code/22_import_natural_earth.sh
time /wof/code/01_load_iso_language_codes.sh

time psql -e -v ON_ERROR_STOP=1 -f  /wof/code/wd_sql_functions.sql

echo "========== END OF job  ============== "
