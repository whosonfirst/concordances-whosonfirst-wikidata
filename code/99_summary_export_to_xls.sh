

rm -f ${outputdir}/wof_wikidata_status.xlsx
pgclimb -o ${outputdir}/wof_wikidata_status.xlsx \
    -c "SELECT * FROM wof_extended_status_summary;" \
    xlsx --sheet "_status_summary_"

pgclimb -o ${outputdir}/wof_wikidata_status.xlsx \
    -c "SELECT * FROM wof_extended_meta_status_country_summary;" \
    xlsx --sheet "meta_status_country_summary"

pgclimb -o ${outputdir}/wof_wikidata_status.xlsx \
    -c "SELECT * FROM wof_disambiguation_report;" \
    xlsx --sheet "disambiquation"

pgclimb -o ${outputdir}/wof_wikidata_status.xlsx \
    -c "SELECT * FROM wof_disambiguation_sum_report;" \
    xlsx --sheet "disambiquation_sum"

pgclimb -o ${outputdir}/wof_wikidata_status.xlsx \
    -c "SELECT * FROM wof_extreme_distance_report;" \
    xlsx --sheet "extreme_distance"

pgclimb -o ${outputdir}/wof_wikidata_status.xlsx \
    -c "SELECT * FROM wof_extreme_distance_sum_report;" \
    xlsx --sheet "extreme_distance_sum"

pgclimb -o ${outputdir}/wof_wikidata_status.xlsx \
    -c "SELECT * FROM wof_wd_redirects_report;" \
    xlsx --sheet "wd_redirects"

pgclimb -o ${outputdir}/wof_wikidata_status.xlsx \
    -c "SELECT * FROM wof_wd_redirects_sum_report;" \
    xlsx --sheet "wd_redirects_sum"




rm -f ${outputdir}/wd_claims.xlsx
pgclimb -o ${outputdir}/wd_claims.xlsx \
    -c "SELECT * FROM wikidata.wd_claims LIMIT 10000" \
    xlsx --sheet "_top10000_wd_claims"
