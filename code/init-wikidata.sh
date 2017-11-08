#!/bin/bash
set -e
set -u

cd /wof
mkdir -p wikidata_dump
cd wikidata_dump
rm -f latest-all.json.bz2
wget https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json.bz2
ls -la latest-all.json.bz2

