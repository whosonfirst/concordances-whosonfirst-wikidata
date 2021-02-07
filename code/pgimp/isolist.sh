#!/usr/bin/env bash
set -o errexit
set -o pipefail
set -o nounset

cd /wof/whosonfirst-data
du -hs whosonfirst-data-admin-?? | sort -hr | cut -f2  | cut --delimiter='-' -f4 > isolist.csv

head /wof/whosonfirst-data/isolist.csv
