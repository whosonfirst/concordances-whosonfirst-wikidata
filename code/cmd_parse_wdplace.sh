#!/bin/bash

set -e
set -u

type=$1

echo "======== cmd_parse_wdplace start ${type} ==========="

rm -f /wof/wikidata_dump/wdplace_${type}.json
time go run /wof/code/wdplaceparse.go /wof/code/wikidata_${type}.csv    > /wof/wikidata_dump/wdplace_${type}.json

wc -l  /wof/wikidata_dump/wdplace_${type}.json
ls -la /wof/wikidata_dump/wdplace_${type}.json

echo "======== cmd_parse_wdplace end  ${type} ==========="
