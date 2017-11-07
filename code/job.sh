#!/bin/bash
set -e
set -u

cd /wof

time /wof/code/01_load_iso_language_codes.sh
time /wof/code/02_load_wof.sh
time /wof/code/10_wof_parse_wdid.sh
time /wof/code/20_import_wikidata.sh



time psql -f /wof/code/50_wof_sql_step1.sql
time psql -f /wof/code/60_wd_sql_views.sql
time psql -vreportdir="/wof/reports" -f /wof/code/71_disambiquation.sql
time psql -vreportdir="/wof/reports" -f /wof/code/72_extreme_distance.sql



rm -f /wof/reports/wof_wikidata_status.xlsx
pgclimb -o /wof/reports/wof_wikidata_status.xlsx \
    -c "SELECT * FROM wof_disambiguation_report;" \
    xlsx --sheet "disambiquation"

pgclimb -o /wof/reports/wof_wikidata_status.xlsx \
    -c "SELECT * FROM wof_extreme_distance_report;" \
    xlsx --sheet "extreme_distance"



ls /wof/reports/* -la

echo "-ok-"
