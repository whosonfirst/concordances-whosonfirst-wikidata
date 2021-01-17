#!/bin/bash
set -e
set -u

cd /wof
mkdir -p taginfo
cd taginfo

rm -f taginfo-db.*
rm -f taginfo-db.db.bz2

time wget https://taginfo.openstreetmap.org/download/taginfo-db.db.bz2
time bzip2 -d taginfo-db.db.bz2

echo "select value from tags where key='wikidata' and substr(value,1,1)='Q'  order by 1 ;" > ./query.sql
time sqlite3 -csv /wof/taginfo/taginfo-db.db < ./query.sql > taginfo_wikidata_raw.csv


# remove spaces
# split multipl wikidata values: "Q11682940;Q16301554"
# filter by regex -E "^Q[0-9]+$"
cat /wof/taginfo/taginfo_wikidata_raw.csv \
  | sed 's/ //g' \
  | sed 's/;Q/\nQ/g' \
  | grep -E "^Q[0-9]+$" > /wof/taginfo/taginfo_wikidata.csv

wc -l /wof/taginfo/taginfo_wikidata*.csv

exit

#
#  Error in the csv:  Q4_770_716  not valid in:  /wof/taginfo/taginfo_wikidata.csv
#  Error in the csv:  Q65936653;Q65936655;Q65936658  not valid in:  /wof/taginfo/taginfo_wikidata.csv
#  Error in the csv:  Qwik3312613  not valid in:  /wof/taginfo/taginfo_wikidata.csv
