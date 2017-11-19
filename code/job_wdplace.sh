#!/bin/bash
set -eo pipefail

cd /wof
STARTTIME=$(date +%s)
STARTDATE=$(date +"%Y%m%dT%H%M")

outputdir=/wof/output/${STARTDATE}
export  outputdir=${outputdir}


mkdir -p ${outputdir}
log_file=${outputdir}/job_wdplace${STARTDATE}.log
rm -f $log_file

#  backup log 
exec &> >(tee -a "$log_file")

echo " "
echo "------------------------------job_wdplace.sh-----------------------------------------"
echo "--                         WOF  -  Wikidata - DW                                   --"
echo "-------------------------------------------------------------------------------------"
echo ""

date -u

# install postgis functions;
wget https://raw.githubusercontent.com/CartoDB/cartodb-postgresql/master/scripts-available/CDB_TransformToWebmercator.sql
psql -f CDB_TransformToWebmercator.sql


rm -rf ${outputdir}/joblog_place00
rm -rf ${outputdir}/joblog_place01

time parallel  --results ${outputdir}/joblog_place01 -k  < /wof/code/parallel_joblist_place1_load.sh

ls ${outputdir}/* -la

echo "----------------------------------------------------------"
echo "### Directory sizes: "
du -sh *

echo "-----------------------------------------------------------"
echo "### Finished:"
date -u

date -u > ${outputdir}/_____________wd_place_finished__________________.txt

echo "========== END OF job.sh log ============== "
