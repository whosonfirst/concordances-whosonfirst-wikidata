psql -vreportdir="${outputdir}" -f /wof/code/71_disambiquation.sql
psql -vreportdir="${outputdir}" -f /wof/code/72_extreme_distance.sql
psql -vreportdir="${outputdir}" -f /wof/code/73_redirected.sql
psql -vreportdir="${outputdir}" -f /wof/code/82_export_wikidata_views.sql