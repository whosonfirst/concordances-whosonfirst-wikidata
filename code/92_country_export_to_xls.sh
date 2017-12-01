#!/bin/bash
set -eo pipefail
outputdir=$1
wof_country=$2

xlsxname=${outputdir}/wikidata_${wof_country}_properties.xlsx
rm -f ${xlsxname}


function wdexport { 
    wdtable=$1
    pgclimb -o ${xlsxname} \
        -c "SELECT * FROM wikidata.${wdtable} where wof_country='${wof_country}'  order by wof_name;" \
        xlsx --sheet "${wdtable}"
}

wdexport wd_claims_validated
wdexport wd_names_preferred_validated
wdexport wd_sitelinks_validated
wdexport wd_descriptions_validated
wdexport wd_aliases_validated
wdexport wd_labels_validated

ls -la ${xlsxname}
echo "========== END OF country export ============== "
