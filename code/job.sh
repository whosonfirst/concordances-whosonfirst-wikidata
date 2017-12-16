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
rm -rf ${outputdir}/joblog03b
rm -rf ${outputdir}/joblog03c
rm -rf ${outputdir}/joblog04
rm -rf ${outputdir}/joblog05



echo """
    --
    CREATE EXTENSION if not exists unaccent;
    CREATE EXTENSION if not exists plpythonu;
    CREATE EXTENSION if not exists cartodb;
    CREATE EXTENSION if not exists pg_similarity;


    CREATE SCHEMA IF NOT EXISTS wd;
    CREATE SCHEMA IF NOT EXISTS wf;
    CREATE SCHEMA IF NOT EXISTS ne;
    CREATE SCHEMA IF NOT EXISTS gn;
    CREATE SCHEMA IF NOT EXISTS wfwd;
    CREATE SCHEMA IF NOT EXISTS wfne;
    CREATE SCHEMA IF NOT EXISTS wfgn;
    --
""" | psql -e

time parallel  --results ${outputdir}/joblog01 -k  < /wof/code/parallel_joblist_01_load_tables.sh
psql -e -f  /wof/code/wd_sql_functions.sql


psql -e -f  /wof/code/50_wof.sql
time parallel  --results ${outputdir}/joblog02 -k  < /wof/code/parallel_joblist_02_sql_processing.sh
time parallel  --results ${outputdir}/joblog03 -k  < /wof/code/parallel_joblist_03_reporting.sh
time psql -e -vreportdir="${outputdir}" -f /wof/code/91_summary.sql


# ----------------------------------------------------------------------------------


# Start parallel processing

time parallel  --results ${outputdir}/joblog03b -k  < /wof/code/parallel_joblist_03b_match.sh



echo """
    --
    drop table if exists wfwd.wof_validated_suggested_list CASCADE;
    CREATE UNLOGGED TABLE         wfwd.wof_validated_suggested_list  as
    select * 
    from 
        (         select id, 'wof_locality'   as metatable, wof_name,wof_country, coalesce(_suggested_wd_id,wof_wd_id) as wd_id, _matching_category from wfwd.wd_mlocality_wof_match_agg
        union all select id, 'wof_country'    as metatable, wof_name,wof_country, coalesce(_suggested_wd_id,wof_wd_id) as wd_id, _matching_category from wfwd.wd_mcountry_wof_match_agg
        union all select id, 'wof_county'     as metatable, wof_name,wof_country, coalesce(_suggested_wd_id,wof_wd_id) as wd_id, _matching_category from wfwd.wd_mcounty_wof_match_agg            
        union all select id, 'wof_region'     as metatable, wof_name,wof_country, coalesce(_suggested_wd_id,wof_wd_id) as wd_id, _matching_category from wfwd.wd_mregion_wof_match_agg
        union all select id, 'wof_dependency' as metatable, wof_name,wof_country, coalesce(_suggested_wd_id,wof_wd_id) as wd_id, _matching_category from wfwd.wd_mdependency_wof_match_agg

        union all select id, 'wof_disputed'      as metatable, wof_name,wof_country, coalesce(_suggested_wd_id,wof_wd_id) as wd_id, _matching_category from wfwd.wd_mdisputed_wof_match_agg
        union all select id, 'wof_macroregion'   as metatable, wof_name,wof_country, coalesce(_suggested_wd_id,wof_wd_id) as wd_id, _matching_category from wfwd.wd_mmacroregion_wof_match_agg
        union all select id, 'wof_macrocounty'   as metatable, wof_name,wof_country, coalesce(_suggested_wd_id,wof_wd_id) as wd_id, _matching_category from wfwd.wd_mmacrocounty_wof_match_agg
        union all select id, 'wof_localadmin'    as metatable, wof_name,wof_country, coalesce(_suggested_wd_id,wof_wd_id) as wd_id, _matching_category from wfwd.wd_mlocaladmin_wof_match_agg
        union all select id, 'wof_campus'        as metatable, wof_name,wof_country, coalesce(_suggested_wd_id,wof_wd_id) as wd_id, _matching_category from wfwd.wd_mcampus_wof_match_agg                        

        union all select id, 'wof_borough'       as metatable, wof_name,wof_country, coalesce(_suggested_wd_id,wof_wd_id) as wd_id, _matching_category from wfwd.wd_mborough_wof_match_agg
        union all select id, 'wof_macrohood'     as metatable, wof_name,wof_country, coalesce(_suggested_wd_id,wof_wd_id) as wd_id, _matching_category from wfwd.wd_mmacrohood_wof_match_agg
        union all select id, 'wof_neighbourhood' as metatable, wof_name,wof_country, coalesce(_suggested_wd_id,wof_wd_id) as wd_id, _matching_category from wfwd.wd_mneighbourhood_wof_match_agg
        union all select id, 'wof_microhood'     as metatable, wof_name,wof_country, coalesce(_suggested_wd_id,wof_wd_id) as wd_id, _matching_category from wfwd.wd_mmicrohood_wof_match_agg
        union all select id, 'wof_planet'        as metatable, wof_name,wof_country, coalesce(_suggested_wd_id,wof_wd_id) as wd_id, _matching_category from wfwd.wd_mplanet_wof_match_agg                        

        union all select id, 'wof_continent'     as metatable, wof_name,wof_country, coalesce(_suggested_wd_id,wof_wd_id) as wd_id, _matching_category from wfwd.wd_mcontinent_wof_match_agg
        union all select id, 'wof_ocean'         as metatable, wof_name,wof_country, coalesce(_suggested_wd_id,wof_wd_id) as wd_id, _matching_category from wfwd.wd_mocean_wof_match_agg
        union all select id, 'wof_timezone'      as metatable, wof_name,wof_country, coalesce(_suggested_wd_id,wof_wd_id) as wd_id, _matching_category from wfwd.wd_mtimezone_wof_match_agg
        union all select id, 'wof_marinearea'    as metatable, wof_name,wof_country, coalesce(_suggested_wd_id,wof_wd_id) as wd_id, _matching_category from wfwd.wd_mmarinearea_wof_match_agg          
        ) t  
    where substr(_matching_category,1,2) ='OK'  --   // 'validated' + 'suggested'  
    order by id
    ;

    CREATE INDEX  ON   wfwd.wof_validated_suggested_list (wd_id)    WITH (fillfactor = 100);
    CREATE INDEX  ON   wfwd.wof_validated_suggested_list (id)       WITH (fillfactor = 100);

    ANALYSE  wfwd.wof_validated_suggested_list ;
    --
""" | psql -e


# parallel processing  ..
time parallel  --results ${outputdir}/joblog03c -k < /wof/code/parallel_joblist_03c_matchreport.sh
time parallel  --results ${outputdir}/joblog04  -k < /wof/code/parallel_joblist_04_create_validated_wd_properties.sh
time parallel  --results ${outputdir}/joblog05  -k < /wof/code/parallel_joblist_05_country_reporting.sh


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
    select * from wfwd.wd_mlocality_wof_match_agg_summary_pct;
    \echo Country
    select * from wfwd.wd_mcountry_wof_match_agg_summary_pct;
    \echo County
    select * from wfwd.wd_mcounty_wof_match_agg_summary_pct;
    \echo Region
    select * from wfwd.wd_mregion_wof_match_agg_summary_pct;
    \echo Dependency
    select * from wfwd.wd_mdependency_wof_match_agg_summary_pct;

    \echo Disputed
    select * from wfwd.wd_mdisputed_wof_match_agg_summary_pct;
    \echo Macroregion
    select * from wfwd.wd_mmacroregion_wof_match_agg_summary_pct;
    \echo Macrocounty
    select * from wfwd.wd_mmacrocounty_wof_match_agg_summary_pct;
    \echo Localadmin
    select * from wfwd.wd_mlocaladmin_wof_match_agg_summary_pct;
    \echo Campus
    select * from wfwd.wd_mcampus_wof_match_agg_summary_pct;


    \echo Borough
    select * from wfwd.wd_mborough_wof_match_agg_summary_pct;
    \echo Macrohood
    select * from wfwd.wd_mmacrohood_wof_match_agg_summary_pct;
    \echo Neighbourhood
    select * from wfwd.wd_mneighbourhood_wof_match_agg_summary_pct;
    \echo Microhood
    select * from wfwd.wd_mmicrohood_wof_match_agg_summary_pct;
    \echo Planet
    select * from wfwd.wd_mplanet_wof_match_agg_summary_pct;


    \echo Continent
    select * from wfwd.wd_mcontinent_wof_match_agg_summary_pct;
    \echo Ocean
    select * from wfwd.wd_mocean_wof_match_agg_summary_pct;
    \echo Timezone
    select * from wfwd.wd_mtimezone_wof_match_agg_summary_pct;
    \echo Marinearea
    select * from wfwd.wd_mmarinearea_wof_match_agg_summary_pct;

    --
""" | psql -e > ${outputdir}/_____________summary__________________.txt


date -u > ${outputdir}/_____________finished__________________.txt
echo "========== END OF job.sh log ============== "
