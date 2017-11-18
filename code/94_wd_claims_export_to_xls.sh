

rm -f ${outputdir}/wd_claims.xlsx
pgclimb -o ${outputdir}/wd_claims.xlsx \
    -c "SELECT * FROM wikidata.wd_claims LIMIT 10000" \
    xlsx --sheet "_top10000_wd_claims"
