#!/bin/bash
set -e
set -u

cd /wof


/wof/code/01_load_iso_language_codes.sh
/wof/code/02_load_wof.sh
/wof/code/10_export_wikidata_and_filter.sh
/wof/code/20_import_wikidata.sh

echo "-ok-"
