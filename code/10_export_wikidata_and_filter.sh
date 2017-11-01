#!/bin/bash

set -e
set -u

echo "======== 10_export_wikidata_and_filter START ==========="

echo """
    SELECT distinct properties->'wof:concordances'->>'wd:id' AS wd_id 
    FROM whosonfirst 
    ORDER by wd_id;
""" | psql \
    | sed 's/ //g' \
    | grep ^Q  \
    | sed -e 's/^/{"type":"item","id":"/' \
    | sed -e 's/$/"/'   > wd_idp.txt

head wd_idp.txt

rm -f wd.json
echo "... fgrep ... expected runtime  30-60 min "
date

time zcat /wof/wikidata_dump/latest-all.json.gz | LANG=C  fgrep -f  wd_idp.txt - |  sed 's/,$//' > wd.json

echo "wd.json: line numbers"
wc -l wd.json

echo "======== 10_export_wikidata_and_filter END ==========="

