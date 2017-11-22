#!/bin/bash
set -eo pipefail

cd /wof
STARTTIME=$(date +%s)
STARTDATE=$(date +"%Y%m%dT%H%M")

outputdir=/wof/output/${STARTDATE}
export  outputdir=${outputdir}

mkdir -p ${outputdir}
log_file=${outputdir}/job_wdotherplace${STARTDATE}.log
rm -f $log_file

#  backup log 
exec &> >(tee -a "$log_file")

echo " "
echo "------------------------------job_wdotherplace.sh-----------------------------------------"
echo "--                         WOF  -  Wikidata - DW                                   --"
echo "-------------------------------------------------------------------------------------"
echo ""

date -u

# install postgis functions;
wget https://raw.githubusercontent.com/CartoDB/cartodb-postgresql/master/scripts-available/CDB_TransformToWebmercator.sql
psql -f CDB_TransformToWebmercator.sql
psql -e  -f /wof/code/sql_functions.sql


#rm -rf ${outputdir}/joblog_place00
#rm -rf ${outputdir}/joblog_place01


go run ./code/wdplaceparse.go /wof/code/wikidata_country.csv    > /wof/wikidata_dump/wdplace_country.json       &
go run ./code/wdplaceparse.go /wof/code/wikidata_dependency.csv > /wof/wikidata_dump/wdplace_dependency.json    &
go run ./code/wdplaceparse.go /wof/code/wikidata_region.csv     > /wof/wikidata_dump/wdplace_region.json        &
go run ./code/wdplaceparse.go /wof/code/wikidata_county.csv     > /wof/wikidata_dump/wdplace_county.json        &
wait 
echo "::4x wdplaceparse.go  Done"
ls -la /wof/wikidata_dump/wdplace_country.json
ls -la /wof/wikidata_dump/wdplace_dependency.json
ls -la /wof/wikidata_dump/wdplace_region.json 
ls -la /wof/wikidata_dump/wdplace_county.json 

wc -l /wof/wikidata_dump/wdplace_country.json
wc -l /wof/wikidata_dump/wdplace_dependency.json
wc -l /wof/wikidata_dump/wdplace_region.json 
wc -l /wof/wikidata_dump/wdplace_county.json 

echo """
    --
    CREATE SCHEMA IF NOT EXISTS wdplace;
    DROP TABLE IF EXISTS wdplace.wd_country CASCADE;
    DROP TABLE IF EXISTS wdplace.wd_dependency CASCADE;
    DROP TABLE IF EXISTS wdplace.wd_region CASCADE;
    DROP TABLE IF EXISTS wdplace.wd_county CASCADE;    
    --
""" | psql -e

pgfutter --schema wdplace --table wd_country    --jsonb  json /wof/wikidata_dump/wdplace_country.json     &
pgfutter --schema wdplace --table wd_dependency --jsonb  json /wof/wikidata_dump/wdplace_dependency.json  &
pgfutter --schema wdplace --table wd_region     --jsonb  json /wof/wikidata_dump/wdplace_region.json      &
pgfutter --schema wdplace --table wd_county     --jsonb  json /wof/wikidata_dump/wdplace_county.json      &
wait
echo "::4x importing done"


echo """
    --
    ANALYZE  wdplace.wd_country ;
    ANALYZE  wdplace.wd_county ;  
    ANALYZE  wdplace.wd_dependency ;
    ANALYZE  wdplace.wd_region ;
    --
""" | psql -e
echo "::4x PG analyze done"

#time parallel  --results ${outputdir}/joblog_place01 -k  < /wof/code/parallel_joblist_place1_load.sh


time psql -e -vreportdir="${outputdir}" -f    /wof/code/wdplace_02_match_country.sql

time psql -e -vreportdir="${outputdir}" -f    /wof/code/wdplace_03_match_county.sql
time psql -e -vreportdir="${outputdir}" -f    /wof/code/wdplace_04_match_region.sql
time psql -e -vreportdir="${outputdir}" -f    /wof/code/wdplace_05_match_dependency.sql


xlsxname=${outputdir}/wd_wof_country_matches.xlsx
rm -f ${xlsxname}
pgclimb -o ${xlsxname} \
    -c "SELECT * FROM wd_mc_wof_match_agg_summary;" \
    xlsx --sheet "_summary_"

pgclimb -o ${xlsxname} \
    -c "SELECT * FROM wd_mc_wof_match_agg;" \
    xlsx --sheet "country_list"

/wof/code/cmd_export_matching_sheet.sh  wd_mcounty_wof_match_agg_summary  wd_mcounty_wof_match_agg  wd_wof_county_matches.xlsx 
/wof/code/cmd_export_matching_sheet.sh  wd_mregion_wof_match_agg_summary  wd_mregion_wof_match_agg  wd_wof_region_matches.xlsx 
/wof/code/cmd_export_matching_sheet.sh  wd_mdependency_wof_match_agg_summary  wd_mdependency_wof_match_agg  wd_wof_dependency_matches.xlsx 

ls ${outputdir}/* -la

echo "----------------------------------------------------------"
echo "### Directory sizes: "
du -sh *

echo "-----------------------------------------------------------"
echo "### Finished:"
date -u

date -u > ${outputdir}/_____________wd_otherplace_finished__________________.txt

echo "========== END OF job_wdotherpace.sh log ============== "
