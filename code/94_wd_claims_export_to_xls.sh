

rm -f ${outputdir}/wd_claims.xlsx
pgclimb -o ${outputdir}/wd_claims.xlsx \
    -c "SELECT * FROM wikidata.wd_claims LIMIT 40000" \
    xlsx --sheet "_top40000_wd_claims"
