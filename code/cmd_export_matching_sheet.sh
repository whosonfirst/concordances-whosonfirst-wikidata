#!/bin/bash
set -eo pipefail

sumtable=$1
table=$2
outputfilename=$3

xlsxname=${outputdir}/${outputfilename}
rm -f ${xlsxname}

pgclimb -o ${xlsxname} \
    -c "SELECT * FROM ${sumtable};" \
    xlsx --sheet "_summary_"

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
