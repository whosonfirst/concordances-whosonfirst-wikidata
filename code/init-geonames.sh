#!/bin/bash
set -e
set -u

cd /wof
mkdir -p geonames
cd geonames


rm -f latest-all.json.bz2

wget http://download.geonames.org/export/dump/alternateNames.zip
wget http://download.geonames.org/export/dump/allCountries.zip
wget http://download.geonames.org/export/dump/readme.txt

ls -la *.*
