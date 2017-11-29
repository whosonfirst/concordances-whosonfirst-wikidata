#!/bin/bash
set -eo pipefail
outputdir=$1
wof_country=$2

xlsxname=${outputdir}/wikidata_${wof_country}_properties.xlsx
rm -f ${xlsxname}


function wdexport { 
    wdtable=$1
    pgclimb -o ${xlsxname} \
        -c "SELECT * FROM wof_extended_wd_ok as wof left join wikidata.${wdtable} as wd on wof.wd_id=wd.wd_id  where wof.wof_country='${wof_country}';" \
        xlsx --sheet "${wdtable}"
}
wdexport wd_names_preferred
wdexport wd_sitelinks
wdexport wd_descriptions
wdexport wd_aliases
wdexport wd_labels

ls -la ${xlsxname}
echo "========== END OF country export ============== "
