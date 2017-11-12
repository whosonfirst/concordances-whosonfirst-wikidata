#!/bin/bash
set -e
set -u

cd /wof
mkdir -p wikidata_dump
cd wikidata_dump
rm -f latest-all.ttl.gz
wget https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.ttl.gz
ls -la latest-all.*

zcat latest-all.ttl.gz  | grep " owl:sameAs " | cut -d" " -f 1,3 | sed s/wd://g  | sed "s/ /,/g" > /wof/wikidata_dump/wikidata_redirects.csv



cat /wof/wikidata_dump/wikidata_redirects.csv | go run ./code/wdredirect_wofparse.go             > /wof/wikidata_dump/wikidata_redirects_filtered.csv
echo """
    -- import 
    CREATE SCHEMA IF NOT EXISTS wikidata;
    DROP TABLE IF EXISTS wikidata.wd_redirects CASCADE;
    CREATE TABLE wikidata.wd_redirects (wd_from text , wd_to text );
    \copy wikidata.wd_redirects (wd_from,wd_to)  FROM '/wof/wikidata_dump/wikidata_redirects_filtered.csv' DELIMITER ',' CSV
    --
""" | psql