#!/bin/bash
set -e
set -u

cd /wof
STARTTIME=$(date +%s)
STARTDATE=$(date +"%Y%m%dT%H%M")

log_file=/wof/log/job_${STARTDATE}.log
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


mkdir -p /wof/log

rm -rf /wof/log/joblog01
rm -rf /wof/log/joblog02
rm -rf /wof/log/joblog03

time parallel --results /wof/log/joblog01 -k  < /wof/code/parallel_joblist_01_load_tables.sh
time parallel --results /wof/log/joblog02 -k  < /wof/code/parallel_joblist_02_sql_processing.sh
time parallel --results /wof/log/joblog03 -k  < /wof/code/parallel_joblist_03_reporting.sh

time psql -f /wof/code/91_summary.sql

rm -f /wof/reports/wof_wikidata_status.xlsx

pgclimb -o /wof/reports/wof_wikidata_status.xlsx \
    -c "SELECT * FROM wof_extended_meta_status_country_summary;" \
    xlsx --sheet "meta_status_country_summary"

pgclimb -o /wof/reports/wof_wikidata_status.xlsx \
    -c "SELECT * FROM wof_extended_status_summary;" \
    xlsx --sheet "_status_summary"

pgclimb -o /wof/reports/wof_wikidata_status.xlsx \
    -c "SELECT * FROM wof_disambiguation_report;" \
    xlsx --sheet "disambiquation"

pgclimb -o /wof/reports/wof_wikidata_status.xlsx \
    -c "SELECT * FROM wof_disambiguation_sum_report;" \
    xlsx --sheet "disambiquation_sum"

pgclimb -o /wof/reports/wof_wikidata_status.xlsx \
    -c "SELECT * FROM wof_extreme_distance_report;" \
    xlsx --sheet "extreme_distance"

pgclimb -o /wof/reports/wof_wikidata_status.xlsx \
    -c "SELECT * FROM wof_extreme_distance_sum_report;" \
    xlsx --sheet "extreme_distance_sum"

ls /wof/reports/* -la

echo "----------------------------------------------------------"
echo "### Directory sizes: "
du -sh *

echo "-----------------------------------------------------------"
echo "### Finished:"
date -u

echo "========== END OF job.sh log ============== "
