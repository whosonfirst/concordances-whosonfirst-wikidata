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
