#!/bin/bash
set -eo pipefail

type=$1


sumtable=wfwd.wd_m${type}_wof_match_agg_summary     
table=wfwd.wd_m${type}_wof_match_agg     
notfoundtable=wfwd.wd_m${type}_wof_match_notfound         
outputfilename=wd_wof_${type}_matches.xlsx

xlsxname=${outputdir}/${outputfilename}
rm -f ${xlsxname}

pgclimb -o ${xlsxname} \
    -c "SELECT * FROM ${sumtable}_pct;" \
    xlsx --sheet "_summary_"

pgclimb -o ${xlsxname} \
    -c "SELECT * FROM ${sumtable}_country;" \
    xlsx --sheet "_country_"

pgclimb -o ${xlsxname} \
    -c "SELECT * FROM ${sumtable};" \
    xlsx --sheet "_summary_distance"

pgclimb -o ${xlsxname} \
    -c "SELECT * FROM  ${table} where wd_number_of_matches>1;" \
    xlsx --sheet "multiple_matches_#not_import"

pgclimb -o ${xlsxname} \
    -c "SELECT * FROM  ${table} where wd_number_of_matches=1 and wof_wd_id  = a_wd_id[1];" \
    xlsx --sheet "validated_#not_import"

pgclimb -o ${xlsxname} \
    -c "SELECT * FROM  ${table} where wd_number_of_matches=1 and  wof_wd_id != a_wd_id[1] and wof_wd_id !='';" \
    xlsx --sheet "suggested for replace"

pgclimb -o ${xlsxname} \
    -c "SELECT * FROM  ${table} where wd_number_of_matches=1 and  wof_wd_id != a_wd_id[1] and wof_wd_id ='';" \
    xlsx --sheet "suggested for add"

pgclimb -o ${xlsxname} \
    -c "SELECT * FROM  ${notfoundtable};" \
    xlsx --sheet "notfound"