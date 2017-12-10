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

echo "$log_file" > /wof/output/lastlog.env

# wait for postgres
/wof/code/pg_isready.sh

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
rm -rf ${outputdir}/joblog05
rm -rf ${outputdir}/joblog_place01


echo """
    --
    -- CREATE EXTENSION if not exists pg_stat_statements;
    --
    CREATE SCHEMA IF NOT EXISTS wdplace;
    CREATE SCHEMA IF NOT EXISTS wd;
    CREATE SCHEMA IF NOT EXISTS wf;
    CREATE SCHEMA IF NOT EXISTS ne;
    CREATE SCHEMA IF NOT EXISTS gn;
    
    CREATE SCHEMA IF NOT EXISTS wfwd;

    DROP TABLE IF EXISTS wdplace.wd_country CASCADE;
    DROP TABLE IF EXISTS wdplace.wd_dependency CASCADE;
    DROP TABLE IF EXISTS wdplace.wd_region CASCADE;
    DROP TABLE IF EXISTS wdplace.wd_county CASCADE;    
    --
""" | psql -e

time parallel  --results ${outputdir}/joblog01 -k  < /wof/code/parallel_joblist_01_load_tables.sh
psql -e -f  /wof/code/wd_sql_functions.sql
psql -e -f  /wof/code/50_wof.sql
time parallel  --results ${outputdir}/joblog02 -k  < /wof/code/parallel_joblist_02_sql_processing.sh
time parallel  --results ${outputdir}/joblog03 -k  < /wof/code/parallel_joblist_03_reporting.sh
time psql -e -vreportdir="${outputdir}" -f /wof/code/91_summary.sql


# ----------------------------------------------------------------------------------

wc -l /wof/wikidata_dump/wdplace_country.json
wc -l /wof/wikidata_dump/wdplace_dependency.json
wc -l /wof/wikidata_dump/wdplace_region.json 
wc -l /wof/wikidata_dump/wdplace_county.json 

echo """
    --
    DROP TABLE IF EXISTS wdplace.wd_country CASCADE;
    DROP TABLE IF EXISTS wdplace.wd_dependency CASCADE;
    DROP TABLE IF EXISTS wdplace.wd_region CASCADE;
    DROP TABLE IF EXISTS wdplace.wd_county CASCADE;    
    --
""" | psql -e

time parallel  --results ${outputdir}/joblog_place01 -k  < /wof/code/parallel_joblist_place1_load.sh


# Start parallel processing
time psql -e -vreportdir="${outputdir}" -f    /wof/code/wdplace_01_match_locality.sql      &
time psql -e -vreportdir="${outputdir}" -f    /wof/code/wdplace_02_match_country.sql       &
time psql -e -vreportdir="${outputdir}" -f    /wof/code/wdplace_03_match_county.sql        &
time psql -e -vreportdir="${outputdir}" -f    /wof/code/wdplace_04_match_region.sql        &
time psql -e -vreportdir="${outputdir}" -f    /wof/code/wdplace_05_match_dependency.sql    &
wait

echo """
    --
    drop table if exists wof_validated_suggested_list CASCADE;
    create table         wof_validated_suggested_list  as
        select * 
        from 
            (         select id, 'wof_locality'   as metatable, wof_name,wof_country, coalesce(_suggested_wd_id,wof_wd_id) as wd_id, _matching_category from wd_mlocality_wof_match_agg
            union all select id, 'wof_country'    as metatable, wof_name,wof_country, coalesce(_suggested_wd_id,wof_wd_id) as wd_id, _matching_category from wd_mcountry_wof_match_agg
            union all select id, 'wof_county'     as metatable, wof_name,wof_country, coalesce(_suggested_wd_id,wof_wd_id) as wd_id, _matching_category from wd_mcounty_wof_match_agg            
            union all select id, 'wof_region'     as metatable, wof_name,wof_country, coalesce(_suggested_wd_id,wof_wd_id) as wd_id, _matching_category from wd_mregion_wof_match_agg
            union all select id, 'wof_dependency' as metatable, wof_name,wof_country, coalesce(_suggested_wd_id,wof_wd_id) as wd_id, _matching_category from wd_mdependency_wof_match_agg
            -- todo country 
            ) t  
        where substr(_matching_category,1,2) ='OK'  --   // 'validated' + 'suggested'  
        order by id
    ;

    CREATE INDEX  ON   wof_validated_suggested_list (wd_id);
    CREATE INDEX  ON   wof_validated_suggested_list (id);

    ANALYSE  wof_validated_suggested_list ;
    --
""" | psql -e


# parallel sheet generating
/wof/code/cmd_export_matching_sheet.sh  wd_mcountry_wof_match_agg_summary     wd_mcountry_wof_match_agg     wd_mcountry_wof_match_notfound         wd_wof_country_matches.xlsx     &
/wof/code/cmd_export_matching_sheet.sh  wd_mcounty_wof_match_agg_summary      wd_mcounty_wof_match_agg      wd_mcounty_wof_match_notfound          wd_wof_county_matches.xlsx      &
/wof/code/cmd_export_matching_sheet.sh  wd_mregion_wof_match_agg_summary      wd_mregion_wof_match_agg      wd_mregion_wof_match_notfound          wd_wof_region_matches.xlsx      &
/wof/code/cmd_export_matching_sheet.sh  wd_mdependency_wof_match_agg_summary  wd_mdependency_wof_match_agg  wd_mdependency_wof_match_notfound      wd_wof_dependency_matches.xlsx  &
/wof/code/cmd_export_matching_sheet.sh  wd_mlocality_wof_match_agg_summary    wd_mlocality_wof_match_agg    wd_mlocality_wof_match_notfound  wd_wof_locality_matches.xlsx    &
wait


time parallel  --results ${outputdir}/joblog04 -k  < /wof/code/parallel_joblist_04_create_validated_wd_properties.sh
time parallel  --results ${outputdir}/joblog05 -k  < /wof/code/parallel_joblist_05_country_reporting.sh



ls ${outputdir}/* -la

echo "----------------------------------------------------------"
echo "### Directory sizes: "
du -sh *


echo "-----------------------------------------------------------"
echo "### Finished:"
date -u


echo """
    --
    \echo Locality
    select * from wd_mlocality_wof_match_agg_summary_pct;

    \echo Country
    select * from wd_mcountry_wof_match_agg_summary_pct;

    \echo County
    select * from wd_mcounty_wof_match_agg_summary_pct;

    \echo Region
    select * from wd_mregion_wof_match_agg_summary_pct;

    \echo Dependency
    select * from wd_mdependency_wof_match_agg_summary_pct;
    --
""" | psql -e > ${outputdir}/_____________summary__________________.txt


date -u > ${outputdir}/_____________finished__________________.txt
echo "========== END OF job.sh log ============== "
