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

