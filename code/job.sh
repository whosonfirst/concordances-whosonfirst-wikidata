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
echo "--                         WOF  -  Wikidata - DW                                   --"
echo "-------------------------------------------------------------------------------------"
echo ""

date -u

# install postgis functions;
wget https://raw.githubusercontent.com/CartoDB/cartodb-postgresql/master/scripts-available/CDB_TransformToWebmercator.sql
psql -f CDB_TransformToWebmercator.sql


#mkdir -p /wof/l


rm -rf ${outputdir}/joblog01
rm -rf ${outputdir}/joblog02
rm -rf ${outputdir}/joblog03

time parallel --eta --results ${outputdir}/joblog01 -k  < /wof/code/parallel_joblist_01_load_tables.sh
time parallel --eta --results ${outputdir}/joblog02 -k  < /wof/code/parallel_joblist_02_sql_processing.sh
time parallel --eta --results ${outputdir}/joblog03 -k  < /wof/code/parallel_joblist_03_reporting.sh

time psql -f /wof/code/91_summary.sql

rm -f ${outputdir}/wof_wikidata_status.xlsx

pgclimb -o ${outputdir}/wof_wikidata_status.xlsx \
    -c "SELECT * FROM wof_extended_meta_status_country_summary;" \
    xlsx --sheet "meta_status_country_summary"

pgclimb -o ${outputdir}/wof_wikidata_status.xlsx \
    -c "SELECT * FROM wof_extended_status_summary;" \
    xlsx --sheet "_status_summary"

pgclimb -o ${outputdir}/wof_wikidata_status.xlsx \
    -c "SELECT * FROM wof_disambiguation_report;" \
    xlsx --sheet "disambiquation"

pgclimb -o ${outputdir}/wof_wikidata_status.xlsx \
    -c "SELECT * FROM wof_disambiguation_sum_report;" \
    xlsx --sheet "disambiquation_sum"

pgclimb -o ${outputdir}/wof_wikidata_status.xlsx \
    -c "SELECT * FROM wof_extreme_distance_report;" \
    xlsx --sheet "extreme_distance"

pgclimb -o ${outputdir}/wof_wikidata_status.xlsx \
    -c "SELECT * FROM wof_extreme_distance_sum_report;" \
    xlsx --sheet "extreme_distance_sum"

ls ${outputdir}/* -la

echo "----------------------------------------------------------"
echo "### Directory sizes: "
du -sh *

echo "-----------------------------------------------------------"
echo "### Finished:"
date -u

echo "========== END OF job.sh log ============== "
