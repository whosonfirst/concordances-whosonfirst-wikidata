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
    ANALYZE  wdplace.wd_dependency ;
    ANALYZE  wdplace.wd_region ;
    ANALYZE  wdplace.wd_county ;  
    --
""" | psql -e
echo "::4x PG analyze done"

#time parallel  --results ${outputdir}/joblog_place01 -k  < /wof/code/parallel_joblist_place1_load.sh
#time psql -e -vreportdir="${outputdir}" -f    /wof/code/wdplace_01_match_locality.sql


ls ${outputdir}/* -la

echo "----------------------------------------------------------"
echo "### Directory sizes: "
du -sh *

echo "-----------------------------------------------------------"
echo "### Finished:"
date -u

date -u > ${outputdir}/_____________wd_otherplace_finished__________________.txt

echo "========== END OF job_wdotherpace.sh log ============== "
