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


# -----------------------------------------------------------------------------------------------------

rm -rf ${outputdir}/joblog01
rm -rf ${outputdir}/joblog02
rm -rf ${outputdir}/joblog03
rm -rf ${outputdir}/joblog04
rm -rf ${outputdir}/joblog_place01

time parallel  --results ${outputdir}/joblog01 -k  < /wof/code/parallel_joblist_01_load_tables.sh
psql -e -f  /wof/code/wd_sql_functions.sql
time parallel  --results ${outputdir}/joblog02 -k  < /wof/code/parallel_joblist_02_sql_processing.sh
time parallel  --results ${outputdir}/joblog03 -k  < /wof/code/parallel_joblist_03_reporting.sh
time psql -e -vreportdir="${outputdir}" -f /wof/code/91_summary.sql
time parallel  --results ${outputdir}/joblog04 -k  < /wof/code/parallel_joblist_04_country_reporting.sh



# ----------------------------------------------------------------------------------

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

time parallel  --results ${outputdir}/joblog_place01 -k  < /wof/code/parallel_joblist_place1_load.sh

echo """
    --
    ANALYZE  wdplace.wd_country ;
    ANALYZE  wdplace.wd_county ;  
    ANALYZE  wdplace.wd_dependency ;
    ANALYZE  wdplace.wd_region ;
    --
""" | psql -e

time psql -e -vreportdir="${outputdir}" -f    /wof/code/wdplace_01_match_locality.sql
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



xlsxname=${outputdir}/wd_wof_locality_matches.xlsx
rm -f ${xlsxname}
pgclimb -o ${xlsxname} \
    -c "SELECT * FROM wd_wof_match_agg_summary;" \
    xlsx --sheet "_summary_"

pgclimb -o ${xlsxname} \
    -c "SELECT * FROM  wd_wof_match_agg where wd_number_of_matches>1;" \
    xlsx --sheet "multiple_matches_#not_import"

pgclimb -o ${xlsxname} \
    -c "SELECT * FROM  wd_wof_match_agg where wd_number_of_matches=1 and wof_wd_id  = a_wd_id[1];" \
    xlsx --sheet "validated_#not_import"

pgclimb -o ${xlsxname} \
    -c "SELECT * FROM  wd_wof_match_agg where wd_number_of_matches=1 and  wof_wd_id != a_wd_id[1] and wof_wd_id !='';" \
    xlsx --sheet "suggested for replace"

pgclimb -o ${xlsxname} \
    -c "SELECT * FROM  wd_wof_match_agg where wd_number_of_matches=1 and  wof_wd_id != a_wd_id[1] and wof_wd_id ='';" \
    xlsx --sheet "suggested for add"

ls ${outputdir}/* -la

echo "----------------------------------------------------------"
echo "### Directory sizes: "
du -sh *

echo "-----------------------------------------------------------"
echo "### Finished:"
date -u

date -u > ${outputdir}/_____________finished__________________.txt
echo "========== END OF job.sh log ============== "
