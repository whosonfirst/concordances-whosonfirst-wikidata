#!/bin/bash

set -e
set -u
echo "======== parse wof for wikidataid ==========="

rm -f /wof/whosonfirst-data/wd.txt
find /wof/whosonfirst-data/data  -name *.geojson -exec  cat {} + | grep "wd:id" | cut -d'"' -f4 > /wof/whosonfirst-data/wd.txt
sort -u -o /wof/whosonfirst-data/wd.txt  /wof/whosonfirst-data/wd.txt
head /wof/whosonfirst-data/wd.txt
wc -l /wof/whosonfirst-data/wd.txt

echo "======== parse wikidata_dump/latest-all.json.gz ==========="
time zcat /wof/wikidata_dump/latest-all.json.gz | go run ./code/wdwofparse.go > /wof/wikidata_dump/wikidata.json


