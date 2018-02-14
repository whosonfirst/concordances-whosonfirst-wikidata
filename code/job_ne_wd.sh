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

lscpu | egrep '^Thread|^Core|^Socket|^CPU\('

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
    CREATE EXTENSION if not exists lostgis;


    CREATE SCHEMA IF NOT EXISTS wd;
    CREATE SCHEMA IF NOT EXISTS wf;
    CREATE SCHEMA IF NOT EXISTS ne;
    CREATE SCHEMA IF NOT EXISTS gn;
    CREATE SCHEMA IF NOT EXISTS newd;
    CREATE SCHEMA IF NOT EXISTS wfwd;
    CREATE SCHEMA IF NOT EXISTS wfne;
    CREATE SCHEMA IF NOT EXISTS wfgn;
    --
""" | psql -e

time /wof/code/20_import_wikidata.sh
time /wof/code/01_load_iso_language_codes.sh
time /wof/code/22_import_natural_earth.sh

# ----------------------------------------------------------------------------------
/wof/code/cmd_load_wof.sh      wf.wof_country         wof-country-latest.csv
psql -e -f  /wof/code/wd_sql_functions.sql

psql -e -f /wof/code/ne_01_match_lake.sql
psql -e -f /wof/code/ne_02_match_river.sql
psql -e -f /wof/code/ne_03_match_geography_marine_polys.sql
psql -e -f /wof/code/ne_04_match_geography_regions_polys.sql
psql -e -f /wof/code/ne_05_match_geography_regions_points.sql
psql -e -f /wof/code/ne_06_match_geography_regions_elevation_points.sql
psql -e -f /wof/code/ne_07_match_geographic_lines.sql
psql -e -f /wof/code/ne_08_match_admin_1_states_provinces.sql
psql -e -f /wof/code/ne_09_match_admin_0_map_subunits.sql
psql -e -f /wof/code/ne_10_match_admin_0_disputed_areas.sql
psql -e -f /wof/code/ne_11_match_playas.sql
psql -e -f /wof/code/ne_12_match_admin_0_countries.sql
psql -e -f /wof/code/ne_13_match_populated_places.sql
psql -e -f /wof/code/ne_14_match_airports.sql


/wof/code/cmd_export_ne_tables.sh              newd.ne_wd_match_lake_match_agg
/wof/code/cmd_export_ne_tables.sh              newd.ne_wd_match_lake_europe_match_agg
/wof/code/cmd_export_ne_tables.sh              newd.ne_wd_match_lake_north_america_match_agg
/wof/code/cmd_export_ne_tables.sh              newd.ne_wd_match_lake_historic_match_agg
/wof/code/cmd_export_ne_tables.sh              newd.ne_wd_match_river_europe_match_agg
/wof/code/cmd_export_ne_tables.sh              newd.ne_wd_match_river_north_america_match_agg
/wof/code/cmd_export_ne_tables.sh              newd.ne_wd_match_river_lake_centerlines_match_agg
/wof/code/cmd_export_ne_tables.sh              newd.ne_wd_match_geography_marine_polys_match_agg
/wof/code/cmd_export_ne_tables.sh              newd.ne_wd_match_geography_regions_polys_match_agg
/wof/code/cmd_export_ne_tables.sh              newd.ne_wd_match_geography_regions_points_match_agg
/wof/code/cmd_export_ne_tables.sh              newd.ne_wd_match_geography_regions_elepoints_match_agg
/wof/code/cmd_export_ne_tables.sh              newd.ne_wd_match_geographic_lines_match_agg
/wof/code/cmd_export_ne_tables.sh              newd.ne_wd_match_admin_1_states_provinces_match_agg
/wof/code/cmd_export_ne_tables.sh              newd.ne_wd_match_admin_0_map_subunits_match_agg
/wof/code/cmd_export_ne_tables.sh              newd.ne_wd_match_admin_0_disputed_areas_match_agg
/wof/code/cmd_export_ne_tables.sh              newd.ne_wd_match_admin_0_countries_match_agg
/wof/code/cmd_export_ne_tables.sh              newd.ne_wd_match_playas_match_agg
/wof/code/cmd_export_ne_tables.sh              newd.ne_wd_match_populated_places_match_agg
/wof/code/cmd_export_ne_tables.sh              newd.ne_wd_match_airports_match_agg


echo """
    --
select * from        newd.ne_wd_match_lake_match_agg_sum_pct  ;
select * from        newd.ne_wd_match_lake_europe_match_agg_sum_pct  ;
select * from        newd.ne_wd_match_lake_north_america_match_agg_sum_pct  ;
select * from        newd.ne_wd_match_lake_historic_match_agg_sum_pct  ;
select * from        newd.ne_wd_match_river_europe_match_agg_sum_pct  ;
select * from        newd.ne_wd_match_river_north_america_match_agg_sum_pct  ;
select * from        newd.ne_wd_match_river_lake_centerlines_match_agg_sum_pct  ;
select * from        newd.ne_wd_match_geography_marine_polys_match_agg_sum_pct  ;
select * from        newd.ne_wd_match_geography_regions_polys_match_agg_sum_pct  ;
select * from        newd.ne_wd_match_geography_regions_points_match_agg_sum_pct  ;
select * from        newd.ne_wd_match_geography_regions_elepoints_match_agg_sum_pct  ;
select * from        newd.ne_wd_match_geographic_lines_match_agg_sum_pct  ;
select * from        newd.ne_wd_match_admin_1_states_provinces_match_agg_sum_pct  ;
select * from        newd.ne_wd_match_admin_0_map_subunits_match_agg_sum_pct  ;
select * from        newd.ne_wd_match_admin_0_disputed_areas_match_agg_sum_pct  ;
select * from        newd.ne_wd_match_admin_0_countries_match_agg_sum_pct  ;
select * from        newd.ne_wd_match_playas_match_agg_sum_pct  ;
select * from        newd.ne_wd_match_populated_places_match_agg_sum_pct  ;
select * from        newd.ne_wd_match_airports_match_agg_sum_pct ;
    --
""" | psql -e > ${outputdir}/_________ne_summary__________________.txt

echo "========== END OF job.sh log ============== "


