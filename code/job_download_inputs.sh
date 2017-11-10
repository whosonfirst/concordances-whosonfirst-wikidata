#!/bin/bash
set -e
set -u

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
echo "--                         Download inputs: WOF  -  Wikidata - DW                  --"
echo "-------------------------------------------------------------------------------------"
echo ""

date -u


# install postgis functions;
wget https://raw.githubusercontent.com/CartoDB/cartodb-postgresql/master/scripts-available/CDB_TransformToWebmercator.sql
psql -f CDB_TransformToWebmercator.sql


rm -rf ${outputdir}/joblog00
# Parallel downloading
time parallel --results ${outputdir}/joblog00 -k  < /wof/code/parallel_joblist_00_download.sh

echo "---------------"
echo "### Directory sizes: "
du -sh *

echo "---------------"
echo "### Finished:"
date -u

echo "========== END OF job_download_inputs.sh log ============== "
