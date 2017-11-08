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
ls -la /wof/wikidata_dump/latest-all.json.bz2
rm -f /wof/wikidata_dump/wikidata.json
time bzcat /wof/wikidata_dump/latest-all.json.bz2 | go run /wof/code/wdwofparse.go > /wof/wikidata_dump/wikidata.json

echo """
    --
    CREATE SCHEMA IF NOT EXISTS wikidata;
    DROP TABLE IF EXISTS wikidata.wd CASCADE;
    --
""" | psql


time pgfutter   --schema wikidata \
                --table wd \
                --jsonb \
                json /wof/wikidata_dump/wikidata.json

echo """
    --
    CREATE INDEX wikidata_wd_jsonb  ON wikidata.wd USING GIN (data);
    CREATE INDEX wikidata_wd_jsonbp ON wikidata.wd USING GIN (data jsonb_path_ops);
    --
    ANALYZE wikidata.wd;
    --
    SELECT count(*) FROM wikidata.wd ;
    --
    \d+ wikidata.wd 
""" | psql

