psql -e -vreportdir="${outputdir}" -f /wof/code/71_disambiquation.sql
psql -e -vreportdir="${outputdir}" -f /wof/code/72_extreme_distance.sql
psql -e -vreportdir="${outputdir}" -f /wof/code/73_redirected.sql
psql -e -vreportdir="${outputdir}" -f /wof/code/82_export_wikidata_views.sql