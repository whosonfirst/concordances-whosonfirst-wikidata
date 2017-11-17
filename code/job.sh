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



#  backup log from here ...
exec &> >(tee -a "$log_file")

echo " "
echo "-------------------------------------------------------------------------------------"
echo "--                         WOF  -  Wikidata - DW                                   --"
echo "-------------------------------------------------------------------------------------"
echo ""

date -u

# install postgis functions;
wget https://raw.githubusercontent.com/CartoDB/cartodb-postgresql/master/scripts-available/CDB_TransformToWebmercator.sql
psql -f CDB_TransformToWebmercator.sql


rm -rf ${outputdir}/joblog01
rm -rf ${outputdir}/joblog02
rm -rf ${outputdir}/joblog03
rm -rf ${outputdir}/joblog04

time parallel  --results ${outputdir}/joblog01 -k  < /wof/code/parallel_joblist_01_load_tables.sh
time parallel  --results ${outputdir}/joblog02 -k  < /wof/code/parallel_joblist_02_sql_processing.sh
time parallel  --results ${outputdir}/joblog03 -k  < /wof/code/parallel_joblist_03_reporting.sh
time psql -e -vreportdir="${outputdir}" -f /wof/code/91_summary.sql
time parallel  --results ${outputdir}/joblog04 -k  < /wof/code/parallel_joblist_04_country_reporting.sh

ls ${outputdir}/* -la

echo "----------------------------------------------------------"
echo "### Directory sizes: "
du -sh *

echo "-----------------------------------------------------------"
echo "### Finished:"
date -u

date -u > ${outputdir}/_____________finished__________________.txt
echo "========== END OF job.sh log ============== "
