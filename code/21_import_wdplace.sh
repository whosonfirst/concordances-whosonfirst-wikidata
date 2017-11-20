#!/bin/bash

set -e
set -u


echo "START:======== parse wikidata_dump/latest-all.json.gz   by wdplaceparse.go ==========="
ls -la /wof/wikidata_dump/latest-all.json.*
rm -f /wof/wikidata_dump/wdplace*

go run ./code/wdplaceparse.go /wof/code/wikidata_localities.csv | split -d --additional-suffix=.json  -n r/8 - /wof/wikidata_dump/wdplace


echo "END:======== parse wikidata_dump/latest-all.json.gz   by wdplaceparse.go ==========="

