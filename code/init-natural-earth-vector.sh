#!/bin/bash
set -e
set -u


cd /wof
git clone --depth 1 --branch nvkelso/224-wikidata-concordances-names https://github.com/nvkelso/natural-earth-vector.git
