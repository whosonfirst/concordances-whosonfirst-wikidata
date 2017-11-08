#!/bin/bash
set -e
set -u

cd /wof

mkdir -p /wof/log
rm -rf /wof/log/joblog01
rm -rf /wof/log/joblog02
rm -rf /wof/log/joblog03


time parallel --results /wof/log/joblog01 -k -j 4 < /wof/code/parallel_joblist_01_load_tables.sh
time parallel --results /wof/log/joblog02 -k -j 4 < /wof/code/parallel_joblist_02_sql_processing.sh
time parallel --results /wof/log/joblog03 -k -j 4 < /wof/code/parallel_joblist_03_reporting.sh


rm -f /wof/reports/wof_wikidata_status.xlsx
pgclimb -o /wof/reports/wof_wikidata_status.xlsx \
    -c "SELECT * FROM wof_disambiguation_report;" \
    xlsx --sheet "disambiquation"

pgclimb -o /wof/reports/wof_wikidata_status.xlsx \
    -c "SELECT * FROM wof_extreme_distance_report;" \
    xlsx --sheet "extreme_distance"

ls /wof/reports/* -la
echo "-ok-"